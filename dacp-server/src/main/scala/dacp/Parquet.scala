package dacp
import java.nio.file.Path
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.memory.BufferAllocator
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.{AvroParquetWriter, AvroParquetReader}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
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
  def schema(p: Path): Schema = {
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(hp(p),conf()))
    try {
      val json = reader.getFooter.getFileMetaData.getKeyValueMetaData.get("dacp.arrow.schema")
      require(json != null, "Parquet file missing dacp Arrow schema metadata")
      Schema.fromJSON(json)
    } finally reader.close()
  }
  def writer(p: Path, s: Schema, compression: String, rowGroup: Long) = {
    val codec = compression match { case "zstd" => CompressionCodecName.ZSTD; case "snappy" => CompressionCodecName.SNAPPY; case "none" => CompressionCodecName.UNCOMPRESSED; case _ => throw new IllegalArgumentException("invalid compression") }
    AvroParquetWriter.builder[GenericRecord](hp(p)).withConf(conf()).withSchema(avro(s)).withCompressionCodec(codec)
      .withRowGroupSize(rowGroup).withExtraMetaData(Map("dacp.arrow.schema" -> s.toJson).asJava).build()
  }
  def stream(p: Path, a: BufferAllocator, size: Int)(start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit = {
    val s = schema(p); val root = VectorSchemaRoot.create(s,a)
    val reader = AvroParquetReader.builder[GenericRecord](hp(p)).withConf(conf()).build()
    try { start(root); var done = false
      while (!done) { demand(); val b = Vector.newBuilder[Seq[Any]]; var count = 0
        while (count < size && !done) { val r = reader.read(); if (r == null) done = true else {
          b += s.getFields.asScala.map(f => Option(r.get(f.getName)).map { case u: org.apache.avro.util.Utf8 => u.toString; case n: java.lang.Long => n.longValue; case n: java.lang.Double => n.doubleValue; case n: java.lang.Boolean => n.booleanValue; case x => x }.orNull).toSeq; count += 1
        }}
        val batch = b.result(); if (batch.nonEmpty) { Data.fill(root,batch); emit(root) }
      }
    } finally { reader.close(); root.close() }
  }
}
