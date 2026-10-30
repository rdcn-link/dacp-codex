package dacp

import _root_.xstream.data._

import java.net.URI
import scala.jdk.CollectionConverters._

/** Shared author-facing source contract. SQL is lowered to the plugin Scan contract. */
object ReadItemParameters {
  def address(params:java.util.Map[String,Object],origin:Option[String]=None):String={
    require(params.keySet.asScala.toSet.subsetOf(Set("uri","view","sql","selection")),"unsupported read_item parameter")
    require(params.get("uri").isInstanceOf[String],"read_item requires uri")
    val value=params.get("uri").toString;val u=URI.create(value)
    require(u.getQuery==null && u.getFragment==null && u.getUserInfo==null,"Item address cannot contain credentials, query or fragment")
    val context=u.getScheme=="context"
    if(context)require(u.getRawAuthority==null && !u.isOpaque && value.startsWith("context:/") && !value.startsWith("context://"),"expected context:/dataset/item")
    else require(u.getScheme=="dacp" && u.getHost!=null && (u.getPort == -1 || (u.getPort>0 && u.getPort<=65535)),"uri must be context:/dataset/item or a complete DACP Item URI")
    val parts=Option(u.getPath).getOrElse("").stripPrefix("/").split("/",-1)
    require(parts.length>=2 && parts.forall(p=>p.nonEmpty && p!="." && p!="..") && !value.exists(_.isControl) && !value.contains("\\"),"read_item requires a dataset and Item path")
    view(params);query(params);selection(params)
    if(context)origin.map(_.stripSuffix("/")+u.getRawPath).getOrElse(value)else value
  }
  def view(params:java.util.Map[String,Object]):String={
    require(!params.containsKey("view") || params.get("view").isInstanceOf[String],"view must be a string")
    val v=Option(params.get("view")).map(_.toString).getOrElse(if(params.containsKey("sql"))"records"else"auto")
    require(v.matches("[a-z][a-z0-9_-]*") && (!params.containsKey("view") || v!="auto"),"unsupported read_item view")
    require(!params.containsKey("sql") || v=="records","sql requires records view");v
  }
  def selection(params:java.util.Map[String,Object]):Option[com.fasterxml.jackson.databind.JsonNode]={
    require(!params.containsKey("selection") || params.get("selection")!=null,"selection must be a mapping")
    Option(params.get("selection")).map { raw=>
      val n=Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](raw)
      require(n.isObject && Data.mapper.writeValueAsBytes(n).length<=DataViewCodecs.MaxMetadataBytes,"selection must be a bounded mapping")
      require(!params.containsKey("sql") && !Set("records","bytes")(view(params)),"selection requires a scientific view and cannot use sql")
      n
    }
  }
  def query(params:java.util.Map[String,Object]):Option[Scan]={
    require(!params.containsKey("sql") || params.get("sql")!=null,"sql must be a string")
    Option(params.get("sql")).map { value=>
    require(value.isInstanceOf[String],"sql must be a string");ItemSql.parse(value.toString)
    }
  }
}

/** Restricted single-Item SELECT syntax; no database names, joins, functions or side effects. */
object ItemSql {
  def parse(sql:String):Scan={
    require(sql.trim.nonEmpty && sql.length<=16384,"sql must contain 1 to 16384 characters")
    val token="'(?:(?:'')|[^'])*'|\"(?:(?:\"\")|[^\"])*\"|<=|>=|<>|!=|[(),*=<>;]|[+-]?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|[A-Za-z_][A-Za-z0-9_]*".r
    val matches=token.findAllMatchIn(sql).toVector;var end=0
    matches.foreach{m=>require(sql.substring(end,m.start).trim.isEmpty,"unsupported SQL syntax");end=m.end}
    require(sql.substring(end).trim.isEmpty && matches.size<=1024,"unsupported or overly complex SQL")
    val tokens=matches.map(_.matched);var i=0;var depth=0
    def peek(s:String)=i<tokens.size && tokens(i).equalsIgnoreCase(s)
    def take():String={require(i<tokens.size,"incomplete SQL query");val t=tokens(i);i+=1;t}
    def expect(s:String):Unit=require(take().equalsIgnoreCase(s),"expected SQL "+s)
    def ident():String={val s=take();require(s.matches("[A-Za-z_][A-Za-z0-9_]*") || (s.startsWith("\"") && s.endsWith("\"")),"expected column name");if(s.startsWith("\""))s.drop(1).dropRight(1).replace("\"\"","\"")else s}
    def literal():Literal={val s=take();if(s.startsWith("'"))Literal("string",s.drop(1).dropRight(1).replace("''","'"))else if(Set("true","false")(s.toLowerCase))Literal("boolean",s.toLowerCase)else {require(s.matches("[+-]?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"),"expected SQL literal");Literal(if(s.exists(c=>c=='e'||c=='E'))"float"else if(s.contains('.'))"decimal"else"integer",s)}}
    def atom():Predicate={
      depth+=1;require(depth<=32,"SQL predicate nesting exceeds 32")
      val p=if(peek("NOT")){take();Predicate("not",children=Seq(atom()))}
      else if(peek("(")){take();val p=or();expect(")");p}
      else {val col=ident();if(peek("IS")){take();val not=peek("NOT");if(not)take();expect("NULL");Predicate(if(not)"is_not_null"else"is_null",col)}
        else if(peek("IN")){take();expect("(");val values=scala.collection.mutable.ArrayBuffer(literal());while(peek(",")){take();values+=literal()};expect(")");Predicate("in",col,values.toVector)}
        else {val op=Map("="->"eq","!="->"ne","<>"->"ne","<"->"lt","<="->"le",">"->"gt",">="->"ge").getOrElse(take(),throw new IllegalArgumentException("unsupported comparison"));Predicate(op,col,Seq(literal()))}}
      depth-=1;p
    }
    def and():Predicate={var p=atom();while(peek("AND")){take();p=Predicate("and",children=Seq(p,atom()))};p}
    def or():Predicate={var p=and();while(peek("OR")){take();p=Predicate("or",children=Seq(p,and()))};p}
    expect("SELECT")
    val columns=if(peek("*")){take();None}else {val c=scala.collection.mutable.ArrayBuffer(ident());while(peek(",")){take();c+=ident()};Some(c.toVector)}
    expect("FROM");expect("source")
    val predicate=if(peek("WHERE")){take();Some(or())}else None
    val limit=if(peek("LIMIT")){take();val n=take();require(n.matches("[0-9]+"),"LIMIT must be a nonnegative integer");Some(n.toLong)}else None
    if(peek(";"))take();require(i==tokens.size,"unsupported SQL clause; use SELECT columns FROM source WHERE predicate LIMIT n")
    Scan(predicate,columns,limit)
  }
}
