package dacp
import scala.jdk.CollectionConverters._

case class SqlConfig(url: String, user: Option[String] = None, password: Option[String] = None,
                     schema: Option[String] = None, tables: Option[Seq[String]] = None,
                     fetchSize: Int = 1024, queryTimeoutSeconds: Int = 60) {
  require(url.startsWith("jdbc:"), "SQL dataset URL must be JDBC")
  require(fetchSize > 0 && queryTimeoutSeconds > 0)
  require(tables.forall(_.forall(SqlConfig.validName)), "invalid SQL table name")
  // JDBC URLs/passwords must never appear in case-class logs or error responses.
  override def toString: String = "SqlConfig(<redacted>)"
}

object SqlConfig {
  def read(source:SourceDefinition,batchSize:Int):SqlConfig={
    require(source.provider=="jdbc","connection requires jdbc source")
    val s=source.settings
    require(s.keySet.subsetOf(Set("url","user","password","user_env","password_env","schema","tables","fetch_size","query_timeout_seconds")),"unknown JDBC source setting")
    def text(k:String)=s.get(k).map(_.toString)
    def secret(k:String)=text(k+"_env").map(e=>sys.env.getOrElse(e,throw new IllegalArgumentException("missing credential environment variable"))).orElse(text(k))
    require(text("url").isDefined,"jdbc source requires url")
    SqlConfig(text("url").get,secret("user"),secret("password"),text("schema"),
      s.get("tables").map(_.asInstanceOf[java.util.List[String]].asScala.toSeq),
      text("fetch_size").map(_.toInt).getOrElse(batchSize),text("query_timeout_seconds").map(_.toInt).getOrElse(60))
  }
  def source(c:SqlConfig):SourceDefinition=SourceDefinition("jdbc",Map[String,Object]("url"->c.url,"fetch_size"->Int.box(c.fetchSize),"query_timeout_seconds"->Int.box(c.queryTimeoutSeconds)) ++
    c.user.map("user"->_) ++ c.password.map("password"->_) ++ c.schema.map("schema"->_) ++ c.tables.map(v=>"tables"->v.asJava))

  def validName(s: String): Boolean = s.nonEmpty && !s.contains("/") && !s.contains("\\") && s != "." && s != ".." && !s.exists(_.isControl)
}
