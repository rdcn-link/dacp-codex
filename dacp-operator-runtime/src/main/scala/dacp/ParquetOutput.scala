package dacp

import java.nio.file.Path
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.io.LocalOutputFile

object ParquetOutput {
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
  def writer(p: Path, s: Schema, compression: String, rowGroup: Long) = {
    val codec = compression match { case "zstd" => CompressionCodecName.ZSTD; case "snappy" => CompressionCodecName.SNAPPY; case "none" => CompressionCodecName.UNCOMPRESSED; case _ => throw new IllegalArgumentException("invalid compression") }
    AvroParquetWriter.builder[GenericRecord](new LocalOutputFile(p)).withConf(new PlainParquetConfiguration()).withSchema(avro(s)).withCompressionCodec(codec)
      .withRowGroupSize(rowGroup).withExtraMetaData(Map("dacp.arrow.schema" -> s.toJson).asJava).build()
  }
}
