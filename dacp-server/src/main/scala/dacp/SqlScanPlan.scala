package dacp

import java.sql.PreparedStatement
import org.apache.arrow.vector.types.pojo.{ArrowType,Schema}
import scala.jdk.CollectionConverters._

/** Conservative JDBC pushdown: collation and floating-point predicates remain in the common evaluator. */
private[dacp] case class SqlScanPlan(columns: String, where: String, parameters: Seq[Any], maxRows: Option[Int]) {
  def bind(statement: PreparedStatement): Unit = parameters.zipWithIndex.foreach { case (value,index) =>
    value match {
      case n: java.math.BigDecimal => statement.setBigDecimal(index+1,n)
      case n: java.lang.Boolean => statement.setBoolean(index+1,n)
    }
  }
}
private[dacp] object SqlScanPlan {
  def build(scan: Scan, schema: Schema, quote: String => String): SqlScanPlan = {
    scan.bind(schema)
    val fields = schema.getFields.asScala.map(f => f.getName -> f.getType).toMap
    def compile(p: Predicate): Option[(String,Seq[Any])] = p.op match {
      case "and" | "or" =>
        val children = p.children.map(compile)
        if(children.forall(_.isDefined)) Some((children.flatten.map(_._1).mkString("(",s" ${p.op.toUpperCase} ",")"),children.flatten.flatMap(_._2)))
        else None
      case "not" => compile(p.children.head).map { case (sql,args) => (s"NOT ($sql)",args) }
      case "is_null" => Some((quote(p.column)+" IS NULL",Seq.empty))
      case "is_not_null" => Some((quote(p.column)+" IS NOT NULL",Seq.empty))
      case _ =>
        val t = fields(p.column)
        if(!t.isInstanceOf[ArrowType.Int] && !t.isInstanceOf[ArrowType.Decimal] && !t.isInstanceOf[ArrowType.Bool]) None
        else {
          val args = p.values.map(l => if(l.kind == "boolean") java.lang.Boolean.valueOf(l.value) else new java.math.BigDecimal(l.value))
          if(p.op == "in") {
            if(args.isEmpty) Some(("1=0",Seq.empty))
            else Some((quote(p.column)+args.map(_ => "?").mkString(" IN (",",",")"),args))
          } else Some((quote(p.column)+" "+Map("eq"->"=","ne"->"<>","lt"->"<","le"->"<=","gt"->">","ge"->">=")(p.op)+" ?",args))
        }
    }
    def conjuncts(p: Predicate): Seq[Predicate] = if(p.op == "and") p.children.flatMap(conjuncts) else Seq(p)
    val fullyHandled = scan.predicate.forall(p => compile(p).isDefined)
    val parts = scan.predicate.toSeq.flatMap(conjuncts).flatMap(compile)
    def references(p: Predicate): Seq[String] = (if(p.column.nonEmpty) Seq(p.column) else Seq.empty) ++ p.children.flatMap(references)
    val required = scan.columns.map(c => (c ++ scan.predicate.toSeq.flatMap(references)).distinct)
    SqlScanPlan(required.map(_.map(quote).mkString(",")).getOrElse("*"),
      if(parts.isEmpty) "" else parts.map(_._1).mkString(" WHERE "," AND ",""),parts.flatMap(_._2),
      scan.limit.filter(n => fullyHandled && n > 0 && n <= Int.MaxValue).map(_.toInt))
  }
}
