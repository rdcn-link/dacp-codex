package dacp.client

import _root_.xstream.data._

import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

final case class Predicate private[client] (private[client] val message: Metadata.Predicate) {
  def &&(other: Predicate): Predicate = combine("and",other)
  def ||(other: Predicate): Predicate = combine("or",other)
  def unary_! : Predicate = Predicate(Metadata.Predicate.newBuilder().setOp("not").addChildren(message).build())
  private def combine(op: String, other: Predicate): Predicate =
    Predicate(Metadata.Predicate.newBuilder().setOp(op).addChildren(message).addChildren(other.message).build())
}
object Expressions {
  def col(name: String): Column = new Column(name)
  private[client] def literal(value: Any): Metadata.Literal = {
    val (kind,text) = value match {
      case v: Boolean => "boolean" -> v.toString
      case v: Byte => "integer" -> v.toString
      case v: Short => "integer" -> v.toString
      case v: Int => "integer" -> v.toString
      case v: Long => "integer" -> v.toString
      case v: BigInt => "integer" -> v.toString
      case v: Float => require(v.isFinite,"floating literal must be finite"); "float" -> v.toString
      case v: Double => require(v.isFinite,"floating literal must be finite"); "float" -> v.toString
      case v: BigDecimal => "decimal" -> v.toString
      case v: String => "string" -> v
      case v: java.time.LocalDate => "date" -> v.toString
      case v: java.time.LocalDateTime => "datetime" -> v.toString
      case v: java.time.Instant => "timestamp" -> v.toString
      case v: java.time.OffsetDateTime => "timestamp" -> v.toInstant.toString
      case _ => throw new IllegalArgumentException("unsupported literal; use isNull for nulls")
    }
    Metadata.Literal.newBuilder().setType(kind).setValue(text).build()
  }
}
final class Column private[client] (name: String) {
  require(name != null && name.nonEmpty,"expected column name")
  private def expression(op: String,values: Seq[Any] = Seq.empty): Predicate =
    Predicate(Metadata.Predicate.newBuilder().setOp(op).setColumn(name)
      .addAllValues(values.map(Expressions.literal).asJava).build())
  def ===(value: Any): Predicate = expression("eq",Seq(value))
  def =!=(value: Any): Predicate = expression("ne",Seq(value))
  def <(value: Any): Predicate = expression("lt",Seq(value))
  def <=(value: Any): Predicate = expression("le",Seq(value))
  def >(value: Any): Predicate = expression("gt",Seq(value))
  def >=(value: Any): Predicate = expression("ge",Seq(value))
  def isin(values: Seq[Any]): Predicate = expression("in",values)
  def isNull: Predicate = expression("is_null")
  def isNotNull: Predicate = expression("is_not_null")
}
