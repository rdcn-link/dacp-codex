package dacp
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
  private def hp(p: Path) = new org.apache.hadoop.fs.Path(p.toUri)
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
  def recordCount(p: Path): Long = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(hp(p),conf()))
    try reader.getFooter.getBlocks.asScala.foldLeft(0L)((sum, block) => Math.addExact(sum, block.getRowCount))
    finally reader.close()
  }
  def schema(p: Path): Schema = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(hp(p),conf()))
    try {
      val json = reader.getFooter.getFileMetaData.getKeyValueMetaData.get("dacp.arrow.schema")
      require(json != null, "Parquet file missing dacp Arrow schema metadata")
      Schema.fromJSON(json)
    } finally reader.close()
  }
  def stream(p: Path, a: BufferAllocator, size: Int, scan: Option[Scan] = None)(start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit = {
    val full = schema(p)
    scan.foreach(_.bind(full))
    def references(p: Predicate): Seq[String] = (if(p.column.nonEmpty) Seq(p.column) else Seq.empty) ++ p.children.flatMap(references)
    val required = scan.flatMap(_.columns).map(c => (c ++ scan.toSeq.flatMap(_.predicate.toSeq.flatMap(references))).distinct)
    val s = required.map(c => new Schema(c.map(n => full.findField(n)).asJava,full.getCustomMetadata)).getOrElse(full)
    val configuration = conf()
    if(required.isDefined) {
      org.apache.parquet.avro.AvroReadSupport.setRequestedProjection(configuration,avro(s))
      org.apache.parquet.avro.AvroReadSupport.setAvroReadSchema(configuration,avro(s))
    }
    val builder = AvroParquetReader.builder[GenericRecord](hp(p)).withConf(configuration)
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
