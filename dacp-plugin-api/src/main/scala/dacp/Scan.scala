package dacp

import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}

/** Typed literals use canonical text so decimal/integer precision survives transport. */
case class Literal(kind: String, value: String)
case class Predicate(op: String, column: String = "", values: Seq[Literal] = Seq.empty,
                     children: Seq[Predicate] = Seq.empty)
case class Scan(predicate: Option[Predicate] = None, columns: Option[Seq[String]] = None,
                limit: Option[Long] = None) {
  require(limit.forall(_ >= 0), "limit must be nonnegative")
  require(columns.forall(c => c.nonEmpty && c.distinct.size == c.size), "projection requires unique columns")
  def bind(schema: Schema): BoundScan = ScanFailure.validate(new BoundScan(this,schema))
}

/** Validation happens before reading records, including branches which cannot match. */
final class BoundScan(scan: Scan, schema: Schema) {
  private val fields = schema.getFields.asScala.toVector
  require(fields.map(_.getName).distinct.size == fields.size,"records schema has duplicate columns")
  private def index(name: String): Int = {
    val i = fields.indexWhere(_.getName == name)
    require(i >= 0,s"unknown column: $name"); i
  }
  val indices: Seq[Int] = scan.columns.map(_.map(index)).getOrElse(fields.indices)
  val outputSchema = new Schema(indices.map(fields).asJava,schema.getCustomMetadata)
  private def literal(l: Literal, t: ArrowType): Any = {
    def invalid = throw new IllegalArgumentException("literal type does not match column")
    t match {
      case x: ArrowType.Int =>
        if(l.kind != "integer") invalid
        val n = BigInt(l.value); val bits = x.getBitWidth
        require(n >= (if(x.getIsSigned) -(BigInt(1) << (bits-1)) else BigInt(0)) &&
          n < (BigInt(1) << (if(x.getIsSigned) bits-1 else bits)),"integer literal out of range")
        BigDecimal(n)
      case x: ArrowType.Decimal =>
        if(!Set("integer","decimal")(l.kind)) invalid
        val n = BigDecimal(l.value)
        val exact = n.bigDecimal.stripTrailingZeros()
        require(n.signum == 0 || (exact.scale <= x.getScale && exact.precision.toLong - exact.scale.toLong <= x.getPrecision-x.getScale),
          "decimal literal out of range")
        n
      case _: ArrowType.FloatingPoint =>
        if(!Set("integer","float")(l.kind)) invalid
        val n = l.value.toDouble; require(n.isFinite,"floating literal must be finite"); n
      case _: ArrowType.Utf8 => if(l.kind != "string") invalid else l.value
      case _: ArrowType.Bool =>
        if(l.kind != "boolean" || !Set("true","false")(l.value)) invalid else l.value.toBoolean
      case _: ArrowType.Date => if(l.kind != "date") invalid else java.time.LocalDate.parse(l.value)
      case x: ArrowType.Timestamp =>
        if(x.getTimezone == null) {
          if(l.kind != "datetime") invalid else java.time.LocalDateTime.parse(l.value)
        } else if(l.kind != "timestamp") invalid else java.time.Instant.parse(l.value)
      case _ => throw new IllegalArgumentException("unsupported predicate column type")
    }
  }
  private def normalized(v: Any, t: ArrowType): Any = if(v == null) null else t match {
    case _: ArrowType.Int | _: ArrowType.Decimal => BigDecimal(v.toString)
    case _: ArrowType.FloatingPoint => v.asInstanceOf[Number].doubleValue()
    case _: ArrowType.Utf8 => v.toString
    case _: ArrowType.Date => v match {
      case d: java.time.LocalDateTime => d.toLocalDate
      case n: java.lang.Number => java.time.LocalDate.ofEpochDay(n.longValue())
      case d => java.time.LocalDate.parse(d.toString)
    }
    case x: ArrowType.Timestamp =>
      if(x.getTimezone == null) v.asInstanceOf[java.time.LocalDateTime]
      else v match {
        case n: java.lang.Number =>
          val perSecond = x.getUnit match {
            case org.apache.arrow.vector.types.TimeUnit.SECOND => 1L
            case org.apache.arrow.vector.types.TimeUnit.MILLISECOND => 1000L
            case org.apache.arrow.vector.types.TimeUnit.MICROSECOND => 1000000L
            case _ => 1000000000L
          }
          java.time.Instant.ofEpochSecond(Math.floorDiv(n.longValue(),perSecond),Math.floorMod(n.longValue(),perSecond)*(1000000000L/perSecond))
        case d: java.time.LocalDateTime => d.toInstant(java.time.ZoneOffset.UTC)
        case d: java.time.ZonedDateTime => d.toInstant
        case d: java.time.OffsetDateTime => d.toInstant
      }
    case _ => v
  }
  private def compare(a: Any,b: Any): Int = (a,b) match {
    case (x: BigDecimal,y: BigDecimal) => x.compare(y)
    case (x: Double,y: Double) => if(x == y) 0 else java.lang.Double.compare(x,y)
    case (x: String,y: String) =>
      // Unicode scalar order, independent of Java UTF-16 and source collation.
      val xs=x.codePoints().toArray; val ys=y.codePoints().toArray
      xs.zip(ys).iterator.map { case (i,j) => Integer.compare(i,j) }.find(_ != 0).getOrElse(Integer.compare(xs.length,ys.length))
    case (x: Boolean,y: Boolean) => java.lang.Boolean.compare(x,y)
    case (x: java.time.LocalDate,y: java.time.LocalDate) => x.compareTo(y)
    case (x: java.time.LocalDateTime,y: java.time.LocalDateTime) => x.compareTo(y)
    case (x: java.time.Instant,y: java.time.Instant) => x.compareTo(y)
  }
  private type Test = (VectorSchemaRoot,Int) => Option[Boolean]
  private var nodes = 0
  private def bind(p: Predicate, depth: Int): Test = {
    nodes += 1; require(depth <= 32 && nodes <= 1024,"predicate is too complex")
    p.op match {
      case "and" | "or" | "not" =>
        require(p.column.isEmpty && p.values.isEmpty && (if(p.op == "not") p.children.size == 1 else p.children.size >= 2),"invalid boolean predicate")
        val cs = p.children.map(bind(_,depth+1))
        (root,row) => {
          val vs = cs.map(_(root,row))
          p.op match {
            case "not" => vs.head.map(!_)
            case "and" => if(vs.contains(Some(false))) Some(false) else if(vs.contains(None)) None else Some(true)
            case "or" => if(vs.contains(Some(true))) Some(true) else if(vs.contains(None)) None else Some(false)
          }
        }
      case op =>
        require(Set("eq","ne","lt","le","gt","ge","in","is_null","is_not_null")(op),"unknown predicate operator")
        require(p.children.isEmpty,"invalid predicate children")
        val i = index(p.column); val t = fields(i).getType
        val nullCheck = op == "is_null" || op == "is_not_null"
        require(if(nullCheck) p.values.isEmpty else if(op == "in") p.values.size <= 10000 else p.values.size == 1,"invalid predicate literals")
        val values = p.values.map(literal(_,t))
        (root,row) => {
          val vector = root.getVector(i)
          if(nullCheck) Some(if(op == "is_null") vector.isNull(row) else !vector.isNull(row))
          else if(op == "in" && values.isEmpty) Some(false)
          else if(vector.isNull(row)) None
          else {
            val a = normalized(vector.getObject(row),t)
            def test(b: Any, operation: String): Boolean = {
              if(a.isInstanceOf[Double] && a.asInstanceOf[Double].isNaN) operation == "ne"
              else { val c = compare(a,b); operation match {
                case "eq" => c == 0; case "ne" => c != 0; case "lt" => c < 0
                case "le" => c <= 0; case "gt" => c > 0; case "ge" => c >= 0
              }}
            }
            Some(if(op == "in") values.exists(test(_,"eq")) else test(values.head,op))
          }
        }
    }
  }
  private val test = scan.predicate.map(bind(_,0))
  def matches(root: VectorSchemaRoot,row: Int): Boolean = test.forall(_(root,row).contains(true))
}

/** Safe query validation errors contain public schema/request details only. */
final class ScanFailure(message: String) extends IllegalArgumentException(message)
object ScanFailure {
  def validate[A](f: => A): A = try f catch {
    case e: ScanFailure => throw e
    case e: IllegalArgumentException => throw new ScanFailure(Option(e.getMessage).getOrElse("invalid scan").stripPrefix("requirement failed: "))
    case _: java.time.DateTimeException => throw new ScanFailure("invalid date or timestamp literal")
  }
}
