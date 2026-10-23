package dacp.unionserver

import dacp.union._
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._

/** Management dashboard across all job owners, isolated from the public DACP namespace. */
final case class WebSettings(host:String="127.0.0.1",port:Int=19880,tokenFile:Option[Path]=None) {
  require(Set("127.0.0.1","localhost","::1")(host) || tokenFile.nonEmpty,"non-loopback management Web requires token_file")
  require(host.nonEmpty && port>0 && port<=65535,"invalid web listener")
}
final case class Settings(host:String,port:Int,publicUrl:String,cert:Path,key:Path,ca:Path,state:Path,clients:Set[String],pollMs:Long=1000,timeout:Long=1800,retention:Long=300,web:Option[WebSettings]=None,executionStore:Option[dacp.ExecutionStoreDefinition]=None,pluginDirectory:Option[Path]=None,administrators:Set[String]=Set.empty,s3:dacp.S3Settings=dacp.S3Settings(),materializationStore:Option[dacp.MaterializationStoreDefinition]=None) {
  require(port>0 && port<=65535 && pollMs>=100 && timeout>0 && retention>0)
  Json.origin(publicUrl);clients.foreach(Json.identity);administrators.foreach(Json.identity)
  def tls=new Tls(cert,key,ca)
}
object Settings {
  def load(path:Path):Settings={
    val n=Json.yaml(path)
    Json.fields(n,Set("host","port","public_url","tls_cert","tls_key","tls_ca","state_directory","client_identities"),Set("poll_interval_ms","execution_timeout_seconds","retention_seconds","web","execution_store","plugin_directory","administrator_identities","s3","materialization_store"))
    def num(k:String,d:Long,min:Long,max:Long)=if(n.has(k))Json.number(n,k,min,max)else d
    Settings(Json.text(n,"host"),Json.number(n,"port",1,65535).toInt,Json.origin(Json.text(n,"public_url")),Path.of(Json.text(n,"tls_cert")),Path.of(Json.text(n,"tls_key")),Path.of(Json.text(n,"tls_ca")),Path.of(Json.text(n,"state_directory")),Json.strings(n,"client_identities").toSet,num("poll_interval_ms",1000,100,60000),num("execution_timeout_seconds",1800,1,86400),num("retention_seconds",300,1,86400),if(n.has("web")){
      val w=n.path("web");Json.fields(w,Set("host","port"),Set("token_file"))
      Some(WebSettings(Json.text(w,"host"),Json.number(w,"port",1,65535).toInt,Option(w.get("token_file")).map(v=>Path.of(v.asText))))
    }else None,if(n.has("execution_store")){
      val e=n.path("execution_store");Json.fields(e,Set("provider","settings"))
      Some(dacp.ExecutionStoreDefinition(Json.text(e,"provider"),Json.mapper.convertValue(e.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap))
    }else None,if(n.has("plugin_directory"))Some(Path.of(Json.text(n,"plugin_directory")))else None,if(n.has("administrator_identities"))Json.strings(n,"administrator_identities").toSet else Set.empty,dacp.S3Settings.read(Option(n.get("s3")).map(v=>Json.mapper.convertValue(v,classOf[java.util.LinkedHashMap[String,Object]]))),Option(n.get("materialization_store")).map { m=>
      Json.fields(m,Set("provider","settings"));dacp.MaterializationStoreDefinition(Json.text(m,"provider"),Json.mapper.convertValue(m.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap)
    })
  }
}
final class Members(settings:Settings) extends AutoCloseable {
  val tls=settings.tls
  val remote=new Remote(tls)
  private val directory=settings.state.resolve("members")
  Files.createDirectories(directory)
  val unionId:String=Json.locked(settings.state) {
    val p=settings.state.resolve("union.json")
    val n=if(Files.exists(p))Json.read(p)else {val v=Json.obj().put("union_id",Json.uuid()).put("scheduler_identity",tls.identity);Json.write(p,v);v}
    require(n.path("scheduler_identity").asText==tls.identity,"scheduler certificate changed; use a new state directory and invite nodes again")
    Json.id(Json.text(n,"union_id"))
  }
  private def file(id:String)=directory.resolve(Json.identity(id)+".json")
  def all:Vector[JsonNode]={val f=Files.list(directory);try f.iterator.asScala.filter(_.getFileName.toString.matches("[a-f0-9]{64}\\.json")).map(Json.read).toVector finally f.close()}
  def invite(url:String):JsonNode={
    val origin=Json.origin(url)
    require(origin!=Json.origin(settings.publicUrl),"cannot invite this Union itself")
    val n=remote.action(origin,"invite",Json.obj().put("union_id",unionId))
    val identity=remote.peer(origin)
    require(identity!=tls.identity,"cannot invite this Union identity")
    require(n.path("node_identity").asText==identity,"node identity mismatch")
    dacp.DacpUri.node(Json.text(n,"data_origin"))
    val member=Json.obj().put("identity",identity).put("url",origin).put("data_origin",n.path("data_origin").asText).put("state","invited")
    Json.locked(settings.state){
      require(!all.exists(m=>m.path("data_origin").asText==member.path("data_origin").asText && m.path("identity").asText!=identity),"duplicate node data origin")
      Json.write(file(identity),member)
    };member
  }
  def remove(id:String):Unit=Json.locked(settings.state){
    val p=file(id);require(Files.exists(p),"unknown member")
    val n=Json.read(p)
    // Withdraw remotely before forgetting membership; a failed withdrawal is explicit.
    remote.action(n.path("url").asText,"withdraw",Json.obj().put("union_id",unionId),pin=Some(id))
    Files.delete(p)
  }
  def inspect(member:JsonNode):JsonNode={
    val url=member.path("url").asText;val pin=Some(member.path("identity").asText)
    val n=member.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    try {
      val grant=remote.action(url,"membership",Json.obj().put("union_id",unionId),pin=pin)
      val state=if(grant.path("state").asText=="approved" && grant.path("expires_at_ms").asLong<=Json.now)"expired"else grant.path("state").asText
      n.put("state",state);n.set[JsonNode]("grant",grant)
      if(state=="approved")n.set[JsonNode]("status",remote.action(url,"node_status",Json.obj().put("union_id",unionId),pin=pin))
    }catch{case scala.util.control.NonFatal(_)=>n.put("state","unknown")}
    n.put("observed_at_ms",Json.now);n
  }
  def close():Unit=remote.close()
}
