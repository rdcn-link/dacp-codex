package dacp


import com.sun.net.httpserver.{HttpExchange,HttpServer}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dacp.union.Json
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ThreadPoolExecutor,ArrayBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

case class NodeWebSettings(enabled:Boolean=true,host:String="0.0.0.0",port:Option[Int]=None,publicHost:Option[String]=None) {
  require(host.nonEmpty && port.forall(p=>p>=0 && p<=65535),"invalid web address")
  require(publicHost.forall(h=>h.nonEmpty && !h.exists(_.isWhitespace) && !h.exists(c=>"/?#@".contains(c))),"invalid web.public_host")
  def bindPort(flightPort:Int):Int={val p=port.getOrElse(flightPort+1000);require(p>=0 && p<=65535,"set web.port when Flight port exceeds 64535");p}
}
object NodeWebSettings {
  def read(raw:Option[Object]):NodeWebSettings=raw.map { value=>
    require(value.isInstanceOf[java.util.Map[_,_]],"web must be a mapping")
    val m=value.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
    require(m.keySet.subsetOf(Set("enabled","host","port","public_host")),"unknown web setting")
    require(m.get("enabled").forall(_.isInstanceOf[java.lang.Boolean]),"web.enabled must be a boolean")
    require(m.get("host").forall(_.isInstanceOf[String]),"web.host must be a string")
    require(m.get("port").forall(v=>v.isInstanceOf[java.lang.Integer] || v.isInstanceOf[java.lang.Long]),"web.port must be an integer")
    require(m.get("public_host").forall(_.isInstanceOf[String]),"web.public_host must be a string")
    NodeWebSettings(m.get("enabled").forall(_.asInstanceOf[java.lang.Boolean].booleanValue),m.get("host").map(_.toString).getOrElse("0.0.0.0"),m.get("port").map(_.toString.toInt),m.get("public_host").map(_.toString))
  }.getOrElse(NodeWebSettings())
}

/** Public read-only metadata view: never opens Items or claims execution output streams. */
private[dacp] final class NodeDashboard(config:Config,flightPort:Int,startedAt:Long,live:()=>Vector[JsonNode]) extends AutoCloseable {
  private val server=HttpServer.create(new InetSocketAddress(config.web.host,config.web.bindPort(flightPort)),16)
  private val threads=new ThreadPoolExecutor(4,4,0L,java.util.concurrent.TimeUnit.MILLISECONDS,new ArrayBlockingQueue[Runnable](64),new ThreadPoolExecutor.AbortPolicy())
  private val closed=new AtomicBoolean()
  private val origin=new java.net.URI("dacp",null,config.publicHost,flightPort,null,null,null).toASCIIString
  private val files=Map("/job"->("job.html","text/html; charset=utf-8"),"/job.js"->("job.js","text/javascript; charset=utf-8"),"/job-script.js"->("job-script.js","text/javascript"),"/attempts.js"->("attempts.js","text/javascript"),"/"->("index.html","text/html"),"/duration.js"->("duration.js","text/javascript"),"/app.js"->("app.js","text/javascript"),"/dag.js"->("dag.js","text/javascript"),"/style.css"->("style.css","text/css"),"/vendor/g6-5.1.1.min.js"->("vendor/g6-5.1.1.min.js","text/javascript"),"/vendor/G6-LICENSE.txt"->("vendor/G6-LICENSE.txt","text/plain"),"/assets/dacp-favicon.png"->("dacp-favicon.png","image/png"),"/favicon.ico"->("dacp-favicon.ico","image/x-icon"),"/assets/dacp-logo.png"->("dacp-logo.png","image/png"))
  server.setExecutor(threads)
  server.createContext("/",(e:HttpExchange)=>handle(e))
  def port:Int=server.getAddress.getPort
  def publicUrl:String=new java.net.URI("http",null,config.web.publicHost.getOrElse(config.publicHost),port,"/",null,null).toASCIIString
  def start():Unit={server.start();org.slf4j.LoggerFactory.getLogger("dacp.Main").info(s"dacp dashboard listening on http://${config.web.host}:$port")}
  private def send(e:HttpExchange,status:Int,bytes:Array[Byte],kind:String):Unit={e.getResponseHeaders.set("Content-Type",kind);e.sendResponseHeaders(status,bytes.length);e.getResponseBody.write(bytes)}
  private def json(e:HttpExchange,status:Int,value:JsonNode):Unit=send(e,status,Json.mapper.writeValueAsBytes(value),"application/json; charset=utf-8")
  private def error(e:HttpExchange,status:Int,message:String):Unit=json(e,status,Json.obj().put("error",message))
  private def values(n:JsonNode):Vector[JsonNode]=if(n.isArray)n.elements.asScala.toVector else Vector.empty
  private def executionSource(record:JsonNode):String=
    if(record.path("kind").asText.startsWith("union_") || record.path("union_id").asText.nonEmpty)"union"else "client"
  private def withSource(record:JsonNode):ObjectNode={
    val n=record.deepCopy[ObjectNode]();n.put("execution_source",executionSource(record));n
  }
  private def graph(record:JsonNode):JsonNode={
    val n=withSource(record)
    n.put("definition_yaml",if(record.has("job_yaml"))record.path("job_yaml").asText else dacp.union.YamlDocument.dump(if(record.has("task_definitions"))record.path("task_definitions")else record))
    n.put("execution_policy_yaml",dacp.union.YamlDocument.dump(record.path("execution_policy").deepCopy[JsonNode]()))
    n.put("effective_execution_policy_yaml",dacp.union.YamlDocument.dump(record.path("effective_execution_policy").deepCopy[JsonNode]()))
    val policyView=Json.obj();policyView.set[JsonNode]("submitted",if(record.has("execution_policy"))record.path("execution_policy")else Json.obj());policyView.set[JsonNode]("effective",if(record.has("effective_execution_policy"))record.path("effective_execution_policy")else Json.obj());n.put("policy_yaml",dacp.union.YamlDocument.dump(policyView))
    val id=n.path("record_id").asText
    val current=live().find(_.path("execution_id").asText==n.path("execution_id").asText).filter(_=>n.path("kind").asText=="union_execution")
    current.foreach(v=>v.fields.asScala.foreach(e=>n.set[JsonNode](e.getKey,e.getValue)))
    val children=config.executionHistory.store.records.filter(c=>c.path("parent_record_id").asText==id || c.path("parent_execution_id").asText==id)
    val steps=n.putArray("step_records");children.sortBy(c=>(c.path("created_at_ms").asLong,c.path("record_id").asText)).foreach(c=>steps.add(withSource(c)))
    val tasks=n.putArray("graph_tasks");val outputs=n.putObject("graph_outputs");val ports=Json.obj()
    var definitions=values(record.path("task_definitions"))
    if(definitions.isEmpty && record.has("job_yaml")) {
      try {
        val graph=RecordedJobGraph(record.path("job_yaml").asText)
        definitions=values(graph.path("tasks")).map { task=>
          val d=task.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();d.put("node",origin)
          val bindings=task.path("inputs");val inputs=d.putObject("inputs")
          bindings.elements.asScala.foreach { b=>inputs.putObject(b.path("port").asText).put("task",b.path("task").asText).put("port",b.path("output_port").asText) };d
        }
        graph.path("outputs").fields.asScala.foreach(e=>outputs.set[JsonNode](e.getKey,e.getValue))
      } catch {case scala.util.control.NonFatal(_)=>n.put("graph_error","Stored script cannot be rendered") }
    }
    if(definitions.isEmpty && record.has("task_id")) {
      val d=Json.obj().put("id",record.path("task_id").asText).put("title",record.path("title").asText(record.path("task_id").asText)).put("operator",record.path("operator").asText)
      if(record.path("inputs").isObject)d.set[JsonNode]("inputs",record.path("inputs"))else d.putObject("inputs")
      if(record.path("output_routes").isObject)d.set[JsonNode]("output_routes",record.path("output_routes"))
      definitions=Vector(d)
    }
    val localIds=definitions.map(_.path("id").asText).toSet
    definitions.foreach { d=>
      val task=d.path("id").asText;val child=children.filter(_.path("task_id").asText==task).sortBy(c=>(c.path("created_at_ms").asLong,c.path("updated_at_ms").asLong)).lastOption
      val state=if(n.path("skipped_tasks").elements.asScala.exists(_.asText==task))"skipped"else if(n.path("tasks").isObject)n.path("tasks").path(task).asText(n.path("state").asText)else child.map(_.path("state").asText).getOrElse(if(Set("running","accepted")(n.path("state").asText))"unknown"else n.path("state").asText)
      val t=tasks.addObject().put("id",task).put("title",d.path("title").asText(task)).put("operator",d.path("operator").asText).put("node",d.path("node").asText(origin)).put("node_identity","local").put("state",state)
      val bindings=t.putArray("inputs")
      d.path("inputs").fields.asScala.foreach { e=>
        val raw=e.getValue;val b=if(raw.isTextual){val parts=raw.asText.split(":",2);Json.obj().put("task",parts(0)).put("port",if(parts.length==2)parts(1)else "result")}else raw;val v=bindings.addObject().put("port",e.getKey).put("view",b.path("view").asText("bytes"))
        if(b.has("uri"))v.put("uri",b.path("uri").asText)
        else if(localIds(b.path("task").asText))v.put("task",b.path("task").asText).put("output_port",b.path("port").asText)
        else v.put("remote",true).put("upstream_origin",b.path("origin").asText("upstream")).put("upstream_task",b.path("task").asText).put("upstream_port",b.path("port").asText).put("uri",b.path("origin").asText("upstream")+" / "+b.path("task").asText+"."+b.path("port").asText)
      }
      d.path("output_routes").fieldNames.asScala.foreach { port=>outputs.putObject(task+"."+port).put("task",task).put("port",port) }
      child.foreach { c=>c.path("ports").fields.asScala.foreach { e=>
        val parts=e.getKey.split("/",-1)
        if(parts.length==2 && Set("inputs","outputs")(parts(0)))ports.set[JsonNode](task+"/"+e.getKey,e.getValue)
        else if(parts.length==3 && parts(0)==task && Set("inputs","outputs")(parts(1)))ports.set[JsonNode](e.getKey,e.getValue)
      }}
      if(definitions.size==1 && !n.path("ports").fieldNames.asScala.exists(_.contains(task+"/")))n.path("ports").fields.asScala.foreach(e=>ports.set[JsonNode](task+"/"+e.getKey,e.getValue))
    }
    n.path("ports").fields.asScala.filter(e=>e.getKey.count(_=='/')==2).foreach(e=>ports.set[JsonNode](e.getKey,e.getValue))
    if(!record.has("task_definitions") && !record.has("job_yaml"))ports.fieldNames.asScala.filter(_.contains("/outputs/")).foreach { key=>
      val parts=key.split("/",3)
      if(localIds(parts(0)) && !outputs.elements.asScala.exists(v=>v.path("task").asText==parts(0) && v.path("port").asText==parts(2)))outputs.putObject(parts(0)+"."+parts(2)).put("task",parts(0)).put("port",parts(2))
    }
    val consumed=tasks.elements.asScala.flatMap(_.path("inputs").elements.asScala.filter(_.has("task")).map(b=>b.path("task").asText->b.path("output_port").asText)).toSet
    outputs.fields.asScala.toVector.filter(e=>consumed(e.getValue.path("task").asText->e.getValue.path("port").asText)).foreach(e=>outputs.remove(e.getKey))
    val nodes=n.putObject("graph_nodes");val node=nodes.putObject("local").put("observed_at_ms",current.map(_=>System.currentTimeMillis()).getOrElse(n.path("updated_at_ms").asLong));node.set[JsonNode]("ports",ports)
    if(record.has("job_yaml") || record.has("task_definitions")){
      n.put("logical_yaml",n.path("definition_yaml").asText)
      val logical=n.putObject("logical_plan");val lt=logical.putArray("tasks")
      tasks.elements.asScala.foreach { task=>
        val copy=task.deepCopy[ObjectNode]();Seq("node","node_identity","state","replayed").foreach(copy.remove)
        definitions.find(_.path("id").asText==task.path("id").asText).foreach(d=>if(d.has("params"))copy.set[JsonNode]("params",d.path("params").deepCopy[JsonNode]()))
        lt.add(copy)
      }
      logical.set[JsonNode]("outputs",outputs.deepCopy[JsonNode]())
    }
    n.put("job_id",id).put("output_route",if(n.path("kind").asText.startsWith("union"))"Node → downstream / Union"else "Node → client")
    n
  }
  private def handle(e:HttpExchange):Unit=try {
    val h=e.getResponseHeaders
    h.set("Cache-Control","no-store");h.set("X-Content-Type-Options","nosniff");h.set("X-Frame-Options","DENY");h.set("Referrer-Policy","no-referrer")
    h.set("Content-Security-Policy","default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
    if(e.getRequestMethod!="GET"){h.set("Allow","GET");error(e,405,"read-only dashboard");return}
    val path=e.getRequestURI.getRawPath
    if(path!="/api/history" && path!="/job" && e.getRequestURI.getRawQuery!=null){error(e,400,"unexpected query parameters");return}
    path match {
      case "/api/overview"=>
        val n=Json.obj().put("web_url",publicUrl).put("flight_uri",origin).put("started_at_ms",startedAt).put("uptime_ms",System.currentTimeMillis()-startedAt).put("capacity",config.maxConcurrentTasks).put("reserved_tasks",config.maxConcurrentTasks-config.taskSlots.availablePermits()).put("mode",if(config.debug)"DEBUG"else "NORMAL")
        n.set[JsonNode]("resources",NodeResources.snapshot())
        val executors=n.putArray("executors");config.plugins.services(classOf[OperatorExecutor]).foreach(x=>executors.add(x.id));json(e,200,n)
      case "/api/datasets"=>
        val n=Json.obj();val a=n.putArray("datasets");config.datasetInfos().foreach { d=>val item=a.addObject().put("name",d.name).put("uri",origin+"/"+d.name).put("description",d.description) };json(e,200,n)
      case "/api/history"=>
        val parts=Option(e.getRequestURI.getRawQuery).filter(_.nonEmpty).map(_.split("&").toVector).getOrElse(Vector.empty)
        require(parts.forall(_.matches("(offset|limit)=[0-9]+|source=(all|client|union)")),"invalid history query")
        val pairs=parts.map(_.split("=",2));require(pairs.map(_(0)).distinct.size==pairs.size,"duplicate history query")
        val params=pairs.map(p=>p(0)->p(1)).toMap
        val offset=params.getOrElse("offset","0").toInt;val limit=params.getOrElse("limit","100").toInt
        require(offset>=0 && limit>0 && limit<=1000,"invalid history page")
        val source=params.getOrElse("source","all")
        val records=config.executionHistory.store.records.filter(r=> !Set("job_task","union_task")(r.path("kind").asText) && r.path("parent_record_id").asText.isEmpty && r.path("parent_execution_id").asText.isEmpty)
          .filter(r=>source=="all" || executionSource(r)==source)
          .sortBy(r=>(-r.path("created_at_ms").asLong,r.path("record_id").asText)).slice(offset,offset+limit)
        val n=Json.obj().put("source",source);val a=n.putArray("records");records.foreach(r=>a.add(withSource(r)));json(e,200,n)
      case p if p.startsWith("/api/executions/")=>
        config.executionHistory.get(p.stripPrefix("/api/executions/")) match {case Some(n)=>json(e,200,graph(n));case None=>error(e,404,"execution not found")}
      case _=>files.get(path) match {
        case Some((file,kind))=>val in=getClass.getResourceAsStream("/node-web/"+file);require(in!=null,"missing dashboard asset");try send(e,200,in.readAllBytes(),kind+(if(kind.startsWith("text/"))"; charset=utf-8"else ""))finally in.close()
        case None=>error(e,404,"not found")
      }
    }
  }catch {case _:IllegalArgumentException=>error(e,400,"invalid request");case NonFatal(_)=>try error(e,503,"node metadata unavailable")catch{case NonFatal(_)=>()}}finally e.close()
  def close():Unit=if(closed.compareAndSet(false,true)){server.stop(0);threads.shutdownNow()}
}
