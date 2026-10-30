package dacp.repository


import com.fasterxml.jackson.databind.JsonNode
import com.sun.net.httpserver.HttpExchange
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files,Path}
import java.security.{SecureRandom,MessageDigest}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

private[repository] final class AdminApi(root:Path,admin:Administration,secureCookie:Boolean) {
  private case class Session(csrf:String,expires:Long)
  private val sessions=mutable.Map.empty[String,Session]
  private val attempts=mutable.Map.empty[String,(Long,Int)]
  private def secret():String={val b=new Array[Byte](32);new SecureRandom().nextBytes(b);b.map(x=>f"${x & 255}%02x").mkString}
  private def body(e:HttpExchange):JsonNode={
    require(Option(e.getRequestHeaders.getFirst("Content-Type")).exists(_.takeWhile(_!=';')=="application/json"),"JSON body required")
    val bytes=e.getRequestBody.readNBytes(65537);if(bytes.length>65536)throw PublishFailure(413,"request exceeds 64 KiB")
    val parser=Repository.mapper.getFactory.createParser(bytes);parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    try {val n=Repository.mapper.readTree[JsonNode](parser);require(n!=null && n.isObject && parser.nextToken()==null,"JSON object required");n}finally parser.close()
  }
  private def reply(e:HttpExchange,status:Int,n:JsonNode):Unit={val data=Repository.mapper.writeValueAsBytes(n);e.getResponseHeaders.set("Content-Type","application/json");e.getResponseHeaders.set("Cache-Control","no-store");e.sendResponseHeaders(status,data.length);e.getResponseBody.write(data)}
  private def sessionId(e:HttpExchange):String=Option(e.getRequestHeaders.getFirst("Cookie")).toSeq.flatMap(_.split(';')).map(_.trim).find(_.startsWith("dacp_admin=")).map(_.stripPrefix("dacp_admin=")).getOrElse("")
  private def sameOrigin(e:HttpExchange):Unit=Option(e.getRequestHeaders.getFirst("Origin")).foreach{o=>val uri=java.net.URI.create(o);require(uri.getRawAuthority==e.getRequestHeaders.getFirst("Host") && uri.getScheme==(if(secureCookie)"https"else "http"),"cross-origin request rejected")}
  def requireSession(e:HttpExchange,mutating:Boolean):String=synchronized {
    val now=System.currentTimeMillis();sessions.filterInPlace((_,s)=>s.expires>now)
    val id=sessionId(e);val session=sessions.getOrElse(id,throw PublishFailure(401,"administrator login required"))
    if(mutating){sameOrigin(e);val csrf=Option(e.getRequestHeaders.getFirst("X-CSRF-Token")).getOrElse("");if(!MessageDigest.isEqual(csrf.getBytes(UTF_8),session.csrf.getBytes(UTF_8)))throw PublishFailure(403,"CSRF token required")}
    session.csrf
  }
  private def cookie(e:HttpExchange,value:String,age:Int):Unit=e.getResponseHeaders.set("Set-Cookie",s"dacp_admin=$value; Path=/; HttpOnly; SameSite=Strict; Max-Age=$age"+(if(secureCookie)"; Secure"else ""))
  def handle(e:HttpExchange):Boolean={
    val path=e.getRequestURI.getRawPath;val method=e.getRequestMethod
    if(!path.startsWith("/api/admin/"))return false
    require(e.getRequestURI.getRawQuery==null,"unexpected query parameters")
    if(path=="/api/admin/login" && method=="POST"){
      sameOrigin(e)
      val peer=e.getRemoteAddress.getAddress.getHostAddress;val now=System.currentTimeMillis()
      synchronized {
        attempts.filterInPlace((_,v)=>now-v._1<60000)
        if(attempts.size>=10000 || attempts.get(peer).exists(_._2>=5))throw PublishFailure(429,"too many login attempts; retry in one minute")
        val old=attempts.getOrElse(peer,(now,0));attempts(peer)=(old._1,old._2+1)
      }
      val n=body(e);require(n.path("password").isTextual,"password required")
      if(!admin.login(n.path("password").asText))throw PublishFailure(401,"invalid administrator credentials")
      val id=secret();val csrf=secret()
      synchronized {sessions.filterInPlace((_,s)=>s.expires>now);if(sessions.size>=128)throw PublishFailure(429,"administrator session limit reached");sessions(id)=Session(csrf,now+8*3600000L);attempts.remove(peer)}
      cookie(e,id,8*3600);reply(e,200,Repository.mapper.createObjectNode().put("csrf",csrf));return true
    }
    val csrf=requireSession(e,mutating= !Set("GET","HEAD")(method))
    (method,path) match {
      case ("GET","/api/admin/session")=>reply(e,200,Repository.mapper.createObjectNode().put("csrf",csrf))
      case ("POST","/api/admin/logout")=>synchronized {sessions.remove(sessionId(e));cookie(e,"",0);reply(e,200,Repository.mapper.createObjectNode().put("status","logged_out"))}
      case ("GET","/api/admin/tokens")=>reply(e,200,admin.tokens)
      case ("POST","/api/admin/tokens")=>val n=body(e);require(n.path("submitter").isTextual,"submitter required");reply(e,201,admin.issue(n.path("submitter").asText))
      case ("DELETE",p) if p.matches("/api/admin/tokens/[a-f0-9-]{36}")=>admin.revoke(p.split('/').last);reply(e,200,Repository.mapper.createObjectNode().put("status","revoked"))
      case ("GET","/api/admin/categories")=>reply(e,200,admin.categories)
      case ("GET","/api/admin/runtimes")=>reply(e,200,admin.runtimes)
      case ("POST","/api/admin/runtimes")=>admin.addRuntime(body(e));reply(e,201,Repository.mapper.createObjectNode().put("status","registered"))
      case ("GET","/api/admin/submissions")=>reply(e,200,admin.submissions())
      case ("GET",p) if p.matches("/api/admin/submissions/[a-f0-9-]{36}")=>reply(e,200,admin.submission(p.split('/').last))
      case ("POST",p) if p.matches("/api/admin/submissions/[a-f0-9-]{36}/icon")=>
        val n=body(e);require(n.isObject && n.fieldNames.asScala.toSet==Set("icon"),"icon required")
        admin.updateIcon(p.split('/')(4),n.path("icon"))
        reply(e,200,Repository.mapper.createObjectNode().put("status","updated"))
      case ("POST",p) if p.matches("/api/admin/submissions/[a-f0-9-]{36}/categories")=>
        val n=body(e);require(n.isObject && n.fieldNames.asScala.toSet==Set("categories"),"categories required")
        admin.updateCategories(p.split('/')(4),n.path("categories"))
        reply(e,200,Repository.mapper.createObjectNode().put("status","updated"))
      case ("POST",p) if p.matches("/api/admin/submissions/[a-f0-9-]{36}/review")=>
        val n=body(e)
        require(n.fieldNames.asScala.toSet.subsetOf(Set("decision","categories","note","trust_score")) && n.path("decision").isTextual && (!n.has("note") || n.path("note").isTextual),"invalid review fields")
        val score=Option(n.get("trust_score")).map{v=>require(v.isIntegralNumber && v.canConvertToInt,"trust_score must be an integer");v.asInt}
        admin.review(p.split('/')(4),n.path("decision").asText,n.path("categories"),n.path("note").asText(""),score)
        reply(e,200,Repository.mapper.createObjectNode().put("status","reviewed"))
      case ("GET",p) if p.matches("/api/admin/submissions/[a-f0-9-]{36}/files/[a-f0-9]{64}/[A-Za-z0-9_.-]+")=>
        val parts=p.split('/');val record=admin.submission(parts(4));val hash=parts(6)
        val descriptor=admin.descriptors(record.path("manifest")).find(a=>a.path("sha256").asText==hash && a.path("filename").asText==parts(7)).getOrElse(throw PublishFailure(404,"file not found"))
        val file=Repository.blob(root,hash);e.getResponseHeaders.set("Cache-Control","no-store");e.getResponseHeaders.set("Content-Type","application/octet-stream");e.getResponseHeaders.set("Content-Disposition",s"attachment; filename=\"${descriptor.path("filename").asText}\"")
        e.sendResponseHeaders(200,Files.size(file));val in=Files.newInputStream(file);try in.transferTo(e.getResponseBody)finally in.close()
      case _=>throw PublishFailure(404,"administrator endpoint not found")
    }
    true
  }
}
