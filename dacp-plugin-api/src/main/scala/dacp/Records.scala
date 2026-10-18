package dacp

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

object Records {
  trait Rows extends AutoCloseable { def names: Seq[String]; def iterator: Iterator[Seq[Any]] }
  private def kind(v: Any): String = v match {
    case null => "null"
    case _: Boolean => "bool"
    case _: Long => "int64"
    case _: Double => "float64"
    case s: String => if (s == "true" || s == "false") "bool" else if (s.matches("[-+]?\\d+")) { try { s.toLong; "int64" } catch { case _: NumberFormatException => "utf8" } } else if (s.matches("[-+]?(?:\\d+\\.\\d*|\\d*\\.\\d+)(?:[eE][-+]?\\d+)?|[-+]?\\d+[eE][-+]?\\d+")) "float64" else "utf8"
  }
  def field(name: String, kind: String): Field = new Field(name, FieldType.nullable(kind match {
    case "int64" => new ArrowType.Int(64, true)
    case "float64" => new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
    case "bool" => new ArrowType.Bool()
    case "utf8" | "string" => new ArrowType.Utf8()
    case x => throw new IllegalArgumentException(s"unsupported schema type $x")
  }), null)
  def inferSchema(r: Records.Rows, batchSize: Int, sample: Int, explicit: Option[Schema] = None): Schema = {
    require(batchSize > 0 && sample > 0)
    try {
      explicit match {
        case Some(s) => require(s.getFields.asScala.map(_.getName).toSeq == r.names, "SCHEMA_CONFLICT: explicit columns do not match"); s
        case None =>
          val types = Array.fill(r.names.size)("null")
          r.iterator.take(Math.multiplyExact(batchSize, sample)).foreach(row => row.zipWithIndex.foreach { case (v, i) =>
            val t = kind(v)
            if (types(i) == "null") types(i) = t
            else require(t == "null" || t == types(i), s"SCHEMA_CONFLICT: ${r.names(i)}; specify explicit schema")
          })
          new Schema(r.names.zip(types).map { case (n,t) => field(n, if (t == "null") "utf8" else t) }.asJava)
      }
    } finally r.close()
  }
  // Fixed schema after sampling: fail on conflicts, never silently widen or restart a stream.
  def set(v: FieldVector, i: Int, value: Any): Unit = {
    if (value == null) v.setNull(i)
    else try v match {
      case x: BigIntVector => require(kind(value) == "int64"); x.setSafe(i, value.toString.toLong)
      case x: Float8Vector => require(Set("int64", "float64").contains(kind(value))); x.setSafe(i, value.toString.toDouble)
      case x: BitVector => require(kind(value) == "bool"); x.setSafe(i, if (value.toString == "true") 1 else 0)
      case x: VarCharVector => x.setSafe(i, value.toString.getBytes(UTF_8))
      case _ => throw new IllegalArgumentException("unsupported Arrow type")
    } catch { case e: Exception => throw new IllegalArgumentException(s"SCHEMA_CONFLICT: ${v.getName}; specify explicit schema", e) }
  }
  def fill(root: VectorSchemaRoot, rows: Seq[Seq[Any]]): Unit = {
    root.clear(); root.allocateNew()
    rows.zipWithIndex.foreach { case (row,i) => root.getFieldVectors.asScala.zip(row).foreach { case (v,x) => set(v,i,x) } }
    root.setRowCount(rows.size)
  }
}
