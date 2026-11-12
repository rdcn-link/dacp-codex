package dacp.unionserver

import _root_.xstream.connector.dacp.wire.{FlightTraffic,Tls}

import _root_.xstream.execution.{ExecutionStoreDefinition,MaterializationStoreDefinition}


import dacp.union._
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._

/** Management dashboard across all job owners, isolated from the public DACP namespace. */
final case class WebSettings(host:String="127.0.0.1",port:Int=19880) {
  require(host.nonEmpty && port>0 && port<=65535,"invalid web listener")
}
final case class Settings(host:String,port:Int,publicUrl:String,cert:Path,key:Path,ca:Path,state:Path,pollMs:Long=1000,timeout:Long=1800,retention:Long=300,web:Option[WebSettings]=None,executionStore:Option[xstream.execution.ExecutionStoreDefinition]=None,pluginDirectory:Option[Path]=None,memberSettings:Vector[MemberSettings]=Vector.empty,credentialDirectory:Option[Path]=None,s3:dacp.S3Settings=dacp.S3Settings(),materializationStore:Option[xstream.execution.MaterializationStoreDefinition]=None,trustedJobSources:Map[String,Set[String]]=Map.empty,planning:PlanningSettings=PlanningSettings()) {
  require(port>0 && port<=65535 && pollMs>=100 && timeout>0 && retention>0)
  Json.origin(publicUrl)
  require(memberSettings.map(_.url).distinct.size==memberSettings.size,"duplicate configured member")
  trustedJobSources.foreach{case(subject,origins)=>
    require(subject.nonEmpty&&subject.length<=256&& !subject.exists(_.isControl),"invalid trusted job subject")
    require(origins.nonEmpty&&origins.forall(o=>Json.origin(o)==o),"trusted job sources must be canonical DACP origins")
  }
  lazy val credentials=new Credentials(credentialDirectory.getOrElse(state.resolve("credentials")))
  def tls=new Tls(cert,key,ca)
}
object Settings {
  def load(path:Path):Settings={
    val n=Json.yaml(path)
    Json.fields(n,Set("host","port","public_url","tls_cert","tls_key","tls_ca","state_directory"),Set("poll_interval_ms","execution_timeout_seconds","retention_seconds","web","execution_store","plugin_directory","members","credential_directory","s3","materialization_store","trusted_job_sources","planning"))
    def num(k:String,d:Long,min:Long,max:Long)=if(n.has(k))Json.number(n,k,min,max)else d
    Settings(Json.text(n,"host"),Json.number(n,"port",1,65535).toInt,Json.origin(Json.text(n,"public_url")),Path.of(Json.text(n,"tls_cert")),Path.of(Json.text(n,"tls_key")),Path.of(Json.text(n,"tls_ca")),Path.of(Json.text(n,"state_directory")),num("poll_interval_ms",1000,100,60000),num("execution_timeout_seconds",1800,1,86400),num("retention_seconds",300,1,86400),if(n.has("web")){
      val w=n.path("web");Json.fields(w,Set("host","port"))
      Some(WebSettings(Json.text(w,"host"),Json.number(w,"port",1,65535).toInt))
    }else None,if(n.has("execution_store")){
      val e=n.path("execution_store");Json.fields(e,Set("provider","settings"))
      Some(xstream.execution.ExecutionStoreDefinition(Json.text(e,"provider"),Json.mapper.convertValue(e.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap))
    }else None,if(n.has("plugin_directory"))Some(Path.of(Json.text(n,"plugin_directory")))else None,Option(n.get("members")).map { values=>
      require(values.isArray,"members must be a list")
      values.elements.asScala.map { v=>Json.fields(v,Set("url","credential_file"));MemberSettings(Json.origin(Json.text(v,"url")),Path.of(Json.text(v,"credential_file"))) }.toVector
    }.getOrElse(Vector.empty),Option(n.get("credential_directory")).map(v=>Path.of(v.asText)),dacp.S3Settings.read(Option(n.get("s3")).map(v=>Json.mapper.convertValue(v,classOf[java.util.LinkedHashMap[String,Object]]))),Option(n.get("materialization_store")).map { m=>
      Json.fields(m,Set("provider","settings"));xstream.execution.MaterializationStoreDefinition(Json.text(m,"provider"),Json.mapper.convertValue(m.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap)
    },Option(n.get("trusted_job_sources")).map{entries=>
      require(entries.isObject,"trusted_job_sources must be a subject-to-origins mapping")
      entries.fields.asScala.map{e=>require(e.getValue.isArray&&e.getValue.size>0&&e.getValue.elements.asScala.forall(_.isTextual),"trusted job sources must be nonempty origin lists");e.getKey->e.getValue.elements.asScala.map(v=>Json.origin(v.asText)).toSet}.toMap
    }.getOrElse(Map.empty),PlanningSettings.read(n.path("planning")))
  }
}
final case class MemberSettings(url:String,credentialFile:Path) {Json.origin(url)}
object UnionIdentity {
  def load(settings:Settings):String=Json.locked(settings.state) {
    val p=settings.state.resolve("union.json")
    val n=if(Files.exists(p))Json.read(p)else {val v=Json.obj().put("union_id",Json.uuid()).put("scheduler_identity",settings.tls.identity);Json.write(p,v);v}
    // The Union ID is stable across certificate rotation; node credentials must be rebound explicitly.
    Json.id(Json.text(n,"union_id"))
  }
}
/** Configuration is the sole member list. Persisted files retain certificate pins, never membership. */
final class Members(settings:Settings) extends AutoCloseable {
  val tls=settings.tls
  val unionId:String=UnionIdentity.load(settings)
  val traffic=new FlightTraffic()
  private val secrets=settings.memberSettings.map(m=>Json.origin(m.url)->m.credentialFile).toMap
  private def credential(origin:String):Option[String]=secrets.get(origin).flatMap(p=>scala.util.Try(Files.readString(p).trim).toOption).filter(s=>s.nonEmpty && s.length<=1024 && !s.exists(_.isWhitespace))
  val remote=new dacp.collaboration.CollaborationClient(tls,Some(traffic),origin=>credential(origin))
  private val directory=settings.state.resolve("member-identities")
  Files.createDirectories(directory)
  private val connected=new java.util.concurrent.ConcurrentHashMap[String,JsonNode]()
  private def file(url:String)=directory.resolve(Credentials.hash(Json.origin(url))+".json")
  private def placeholder(url:String):JsonNode=Json.obj().put("identity",Credentials.hash(url)).put("url",url).put("data_origin",url).put("state","unavailable")
  settings.memberSettings.foreach { m=>
    val url=Json.origin(m.url);require(url!=settings.publicUrl,"cannot include this Union itself")
    connected.put(url,if(Files.exists(file(url)))Json.read(file(url))else placeholder(url))
  }
  def all:Vector[JsonNode]=settings.memberSettings.map(m=>connected.get(Json.origin(m.url)))
  def inspect(member:JsonNode):JsonNode={
    val url=Json.origin(member.path("url").asText)
    require(secrets.contains(url),"member is not configured")
    val n=connected.get(url).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    try {
      val pin=if(Files.exists(file(url)))Some(Json.read(file(url)).path("identity").asText)else None
      val grant=remote.action(url,"membership",Json.obj().put("union_id",unionId),pin=pin,timeout=5)
      val identity=remote.peer(url,pin)
      require(grant.path("node_identity").asText==identity && identity!=tls.identity,"member identity mismatch")
      val dataOrigin=dacp.DacpUri.node(Json.text(grant,"data_origin"))
      require(!all.exists(m=>m.path("url").asText!=url && (m.path("identity").asText==identity || m.path("data_origin").asText==dataOrigin)),"duplicate member identity or origin")
      n.put("identity",identity).put("data_origin",dataOrigin).put("state","approved")
      n.set[JsonNode]("grant",grant)
      // Commit a pin only after authenticated enrollment. A later certificate change fails closed.
      val persisted=Json.obj().put("identity",identity).put("url",url).put("data_origin",dataOrigin)
      if(!Files.exists(file(url)))Json.locked(settings.state){Json.write(file(url),persisted)}
      n.set[JsonNode]("status",remote.action(url,"node_status",Json.obj().put("union_id",unionId),pin=Some(identity),timeout=5))
    }catch{case scala.util.control.NonFatal(_)=>n.put("state","unavailable")}
    n.put("observed_at_ms",Json.now);connected.put(url,n);n
  }
  def close():Unit=remote.close()
}

final case class PlanningSettings(provider:String="cost",beamWidth:Int=32,settings:Map[String,Object]=Map.empty) {
  require(provider.matches("[a-z][a-z0-9-]*")&&beamWidth>=1&&beamWidth<=256,"invalid planning provider or beam_width")
}
object PlanningSettings {
  def read(n:JsonNode):PlanningSettings={
    if(n.isMissingNode)return PlanningSettings()
    Json.fields(n,Set.empty,Set("provider","beam_width","settings"))
    require(!n.has("settings")||n.path("settings").isObject,"planning.settings must be a mapping")
    PlanningSettings(if(n.has("provider"))Json.text(n,"provider")else"cost",if(n.has("beam_width"))Json.number(n,"beam_width",1,256).toInt else 32,
      if(n.has("settings"))Json.mapper.convertValue(n.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap else Map.empty)
  }
}
