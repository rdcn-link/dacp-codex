package dacp

import org.apache.arrow.vector.types.pojo.{ArrowType,Schema}
import org.apache.parquet.filter2.predicate.{FilterApi,FilterPredicate}
import scala.jdk.CollectionConverters._

/** Only predicates with identical Parquet null/type semantics are pushed into the reader. */
private[dacp] object ParquetScanPlan {
  def filter(scan: Scan, schema: Schema): Option[FilterPredicate] = {
    scan.bind(schema)
    val types = schema.getFields.asScala.map(f => f.getName -> f.getType).toMap
    def compile(p: Predicate): Option[FilterPredicate] = p.op match {
      case "and" => p.children.flatMap(compile).reduceOption(FilterApi.and)
      case "or" =>
        val cs = p.children.map(compile)
        if(cs.forall(_.isDefined)) cs.flatten.reduceOption(FilterApi.or) else None
      // Parquet NOT has different null semantics; leave it in the common evaluator.
      case "not" => None
      case _ if !p.column.contains(".") && types(p.column).isInstanceOf[ArrowType.Int] &&
          types(p.column).asInstanceOf[ArrowType.Int].getIsSigned =>
        val column = FilterApi.longColumn(p.column)
        def eq(v: Literal): FilterPredicate = FilterApi.eq(column,java.lang.Long.valueOf(v.value))
        val nonNull = FilterApi.notEq(column,null.asInstanceOf[java.lang.Long])
        p.op match {
          case "is_null" => Some(FilterApi.eq(column,null.asInstanceOf[java.lang.Long]))
          case "is_not_null" => Some(nonNull)
          case "in" => p.values.map(eq).reduceOption(FilterApi.or)
          case op =>
            val n = java.lang.Long.valueOf(p.values.head.value)
            val filter = op match {
              case "eq" => FilterApi.eq(column,n)
              case "ne" => FilterApi.notEq(column,n)
              case "lt" => FilterApi.lt(column,n)
              case "le" => FilterApi.ltEq(column,n)
              case "gt" => FilterApi.gt(column,n)
              case "ge" => FilterApi.gtEq(column,n)
            }
            Some(FilterApi.and(nonNull,filter))
        }
      case _ => None
    }
    scan.predicate.flatMap(compile)
  }
}
