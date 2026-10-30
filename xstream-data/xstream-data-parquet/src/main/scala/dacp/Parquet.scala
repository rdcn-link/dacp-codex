package dacp

import _root_.xstream.data._
import java.nio.file.Path
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.memory.BufferAllocator
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.hadoop.conf.Configuration
object Parquet {
  private def conf(): Configuration = { val c = new Configuration(); c.set("fs.file.impl","org.apache.hadoop.fs.RawLocalFileSystem"); c.setBoolean("fs.file.impl.disable.cache",true); c }
  private def input(p:ByteSource):org.apache.parquet.io.InputFile=new org.apache.parquet.io.InputFile {
    def getLength:Long=p.size.getOrElse(throw new IllegalArgumentException("Parquet requires object size"))
    def newStream():org.apache.parquet.io.SeekableInputStream={
      require(p.supportsRanges,"Parquet requires range-readable ByteSource")
      new org.apache.parquet.io.DelegatingSeekableInputStream(new java.io.ByteArrayInputStream(Array.emptyByteArray)) {
        private var position=0L
        private var current:java.io.InputStream=null
        def getPos:Long=position
        def seek(offset:Long):Unit={require(offset>=0 && offset<=getLength,"invalid seek");if(current!=null)current.close();position=offset;current=null}
        private def stream:java.io.InputStream={if(current==null)current=p.readRange(position,getLength-position,()=>());current}
        override def read():Int={val n=stream.read();if(n>=0)position+=1;n}
        override def read(b:Array[Byte],o:Int,n:Int):Int={val k=stream.read(b,o,n);if(k>0)position+=k;k}
        override def readFully(b:Array[Byte]):Unit=readFully(b,0,b.length)
        override def readFully(b:Array[Byte],o:Int,n:Int):Unit={var done=0;while(done<n){val k=read(b,o+done,n-done);if(k<0)throw new java.io.EOFException();done+=k}}
        override def read(buffer:java.nio.ByteBuffer):Int={val b=new Array[Byte](math.min(buffer.remaining,65536));val n=read(b,0,b.length);if(n>0)buffer.put(b,0,n);n}
        override def readFully(buffer:java.nio.ByteBuffer):Unit=while(buffer.hasRemaining){if(read(buffer)<0)throw new java.io.EOFException()}
        override def close():Unit=if(current!=null)current.close()
      }
    }
  }
  def avro(s: Schema): AvroSchema = {
    val record = AvroSchema.createRecord("row",null,"dacp",false)
    record.setFields(s.getFields.asScala.map { f =>
      val t = f.getType match {
        case _: org.apache.arrow.vector.types.pojo.ArrowType.Int => AvroSchema.Type.LONG
        case _: org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint => AvroSchema.Type.DOUBLE
        case _: org.apache.arrow.vector.types.pojo.ArrowType.Bool => AvroSchema.Type.BOOLEAN
        case _: org.apache.arrow.vector.types.pojo.ArrowType.Utf8 => AvroSchema.Type.STRING
        case _ => throw new IllegalArgumentException("unsupported parquet type")
      }
      new AvroSchema.Field(f.getName,AvroSchema.createUnion(List(AvroSchema.create(AvroSchema.Type.NULL),AvroSchema.create(t)).asJava),null,org.apache.avro.JsonProperties.NULL_VALUE)
    }.asJava)
    record
  }
  def recordCount(p: ByteSource): Long = {
    val reader = ParquetFileReader.open(input(p))
    try reader.getFooter.getBlocks.asScala.foldLeft(0L)((sum, block) => Math.addExact(sum, block.getRowCount))
    finally reader.close()
  }
  def schema(p: ByteSource): Schema = {
    val reader = ParquetFileReader.open(input(p))
    try {
      val json = reader.getFooter.getFileMetaData.getKeyValueMetaData.get("dacp.arrow.schema")
      require(json != null, "Parquet file missing dacp Arrow schema metadata")
      Schema.fromJSON(json)
    } finally reader.close()
  }
  def stream(p: ByteSource, a: BufferAllocator, size: Int, scan: Option[Scan] = None)(start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit = {
    val source=ByteSource.withCheck(p,demand)
    val full = schema(source)
    scan.foreach(_.bind(full))
    def references(p: Predicate): Seq[String] = (if(p.column.nonEmpty) Seq(p.column) else Seq.empty) ++ p.children.flatMap(references)
    val required = scan.flatMap(_.columns).map(c => (c ++ scan.toSeq.flatMap(_.predicate.toSeq.flatMap(references))).distinct)
    val s = required.map(c => new Schema(c.map(n => full.findField(n)).asJava,full.getCustomMetadata)).getOrElse(full)
    val configuration = conf()
    if(required.isDefined) {
      org.apache.parquet.avro.AvroReadSupport.setRequestedProjection(configuration,avro(s))
      org.apache.parquet.avro.AvroReadSupport.setAvroReadSchema(configuration,avro(s))
    }
    val builder = AvroParquetReader.builder[GenericRecord](input(source)).withConf(configuration)
    scan.flatMap(ParquetScanPlan.filter(_,full)).foreach(f => builder.withFilter(org.apache.parquet.filter2.compat.FilterCompat.get(f)))
    val root = VectorSchemaRoot.create(s,a)
    val reader = try builder.build() catch { case t: Throwable => root.close(); throw t }
    try { start(root); var done = false
      while (!done) { demand(); val b = Vector.newBuilder[Seq[Any]]; var count = 0
        while (count < size && !done) { val r = reader.read(); if (r == null) done = true else {
          b += s.getFields.asScala.map(f => Option(r.get(f.getName)).map { case u: org.apache.avro.util.Utf8 => u.toString; case n: java.lang.Long => n.longValue; case n: java.lang.Double => n.doubleValue; case n: java.lang.Boolean => n.booleanValue; case x => x }.orNull).toSeq; count += 1
        }}
        val batch = b.result(); if (batch.nonEmpty) { Records.fill(root,batch); emit(root) }
      }
    } finally { try reader.close() finally root.close() }
  }
}
