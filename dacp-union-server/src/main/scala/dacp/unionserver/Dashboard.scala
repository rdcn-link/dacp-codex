package dacp.unionserver

import com.sun.net.httpserver.{HttpExchange,HttpServer}
import dacp.union.Json
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** Public read-only view. Does not open result streams or issue execution RPCs. */
private[unionserver] final class Dashboard(settings:WebSettings,coordinator:Coordinator) extends AutoCloseable {
  private val server=HttpServer.create(new InetSocketAddress(settings.host,settings.port),16)
  private val executor=Executors.newFixedThreadPool(4)
  private val closed=new AtomicBoolean()
  private val assets=Map("/dag.js"->("dag.js","text/javascript; charset=utf-8"),"/vendor/g6-5.1.1.min.js"->("vendor/g6-5.1.1.min.js","text/javascript; charset=utf-8"),"/vendor/G6-LICENSE.txt"->("vendor/G6-LICENSE.txt","text/plain; charset=utf-8"),"/assets/dacp-logo.png"->("dacp-logo.png","image/png"),"/"->("index.html","text/html; charset=utf-8"),"/app.js"->("app.js","text/javascript; charset=utf-8"),"/style.css"->("style.css","text/css; charset=utf-8")).map {case(path,(file,kind))=>
    val stream=Option(getClass.getResourceAsStream("/web/"+file)).getOrElse(throw new IllegalStateException("missing dashboard asset"))
    try path->(stream.readAllBytes(),kind)finally stream.close()
  }
  server.setExecutor(executor)
  server.createContext("/",(exchange:HttpExchange)=>handle(exchange))
  private def send(e:HttpExchange,status:Int,data:Array[Byte],kind:String):Unit={
    e.getResponseHeaders.set("Content-Type",kind)
    e.sendResponseHeaders(status,data.length);e.getResponseBody.write(data)
  }
  private def json(e:HttpExchange,status:Int,n:com.fasterxml.jackson.databind.JsonNode):Unit=send(e,status,Json.mapper.writeValueAsBytes(n),"application/json; charset=utf-8")
  private def error(e:HttpExchange,status:Int,message:String):Unit=json(e,status,Json.obj().put("error",message))
  private def handle(e:HttpExchange):Unit=try {
    val headers=e.getResponseHeaders
    headers.set("Cache-Control","no-store")
    headers.set("X-Content-Type-Options","nosniff")
    headers.set("X-Frame-Options","DENY")
    headers.set("Referrer-Policy","no-referrer")
    headers.set("Content-Security-Policy","default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
    if(e.getRequestMethod!="GET"){headers.set("Allow","GET");error(e,405,"read-only dashboard");return}
    val path=e.getRequestURI.getRawPath
    if(path.startsWith("/api/")) {
      if(path!="/api/history" && e.getRequestURI.getRawQuery!=null){error(e,400,"unexpected query parameters");return}
      path match {
        case "/api/history"=>
          val query=Option(e.getRequestURI.getRawQuery).filter(_.nonEmpty).map(_.split("&").toVector).getOrElse(Vector.empty)
          require(query.forall(_.matches("(offset|limit)=[0-9]+")),"invalid history query")
          val pairs=query.map(_.split("=",2));require(pairs.map(_(0)).distinct.size==pairs.size,"duplicate history query")
          val values=pairs.map(p=>p(0)->p(1).toInt).toMap
          json(e,200,coordinator.historyPage(values.getOrElse("offset",0),values.getOrElse("limit",100)))
        case "/api/overview"=>json(e,200,coordinator.overview)
        case p if p.startsWith("/api/jobs/")=>
          val id=p.stripPrefix("/api/jobs/")
          coordinator.jobSnapshot(id) match {case Some(n)=>json(e,200,n);case None=>error(e,404,"job unavailable or expired")}
        case _=>error(e,404,"unknown dashboard resource")
      }
    }else assets.get(path) match {case Some((bytes,kind))=>send(e,200,bytes,kind);case None=>error(e,404,"not found")}
  }catch{
    case _:IllegalArgumentException=>error(e,400,"invalid request")
    case NonFatal(_)=>try error(e,500,"dashboard request failed")catch{case NonFatal(_)=>()}
  }finally e.close()
  def start():Unit={server.start();println(s"union dashboard listening on http://${settings.host}:${settings.port}")}
  def close():Unit=if(closed.compareAndSet(false,true)){server.stop(0);executor.shutdownNow()}
}
