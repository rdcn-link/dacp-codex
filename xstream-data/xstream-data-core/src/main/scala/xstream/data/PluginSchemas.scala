package xstream.data

import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

object PluginSchemas {
  val bytes = new Schema(List(new Field("data", FieldType.nullable(new ArrowType.Binary()), null)).asJava)
}
