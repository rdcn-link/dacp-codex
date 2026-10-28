package dacp.designer

import com.fasterxml.jackson.databind.JsonNode
import com.sun.net.httpserver.{HttpExchange,HttpServer}
import dacp.client.{Source,Dataset,TlsConfig,WorkflowVariables}
import dacp.{OperatorInferenceService,PluginEnvironment,InferenceWorkerSettings,StreamCancel,StreamDescriptor,ItemViews,PreviewLimits,OutputSample,OutputSamples,InferenceDiagnostic}
import dacp.union.{Json,YamlDocument}
import java.net.{InetSocketAddress,URI}
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files,Path}
import java.time.Duration
import java.util.concurrent.{ArrayBlockingQueue,ThreadPoolExecutor,TimeUnit}
import scala.jdk.CollectionConverters._
import scala.util.Using

final class Designer(config:JsonNode) extends AutoCloseable {
  require(!config.has("targets") && !config.has("repositories"),"Configure a single target and repository")
  private val configuredTarget=config.path("target")
  private val configuredRepository=config.path("repository")
  Json.origin(configuredTarget.path("url").asText)
  require(Set("union","node")(configuredTarget.path("kind").asText),"Target kind must be union or node")
  private val repositoryUri=URI.create(configuredRepository.path("url").asText)
  require(Set("http","https")(repositoryUri.getScheme) && repositoryUri.getHost!=null,"Repository requires HTTP URL")
  private val chat=ChatPlanner.configured(config.path("chat"))
  private val http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
  private val pool=new ThreadPoolExecutor(8,8,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue[Runnable](64),new ThreadPoolExecutor.AbortPolicy())
  private val server=HttpServer.create(new InetSocketAddress(config.path("host").asText("127.0.0.1"),config.path("port").asInt(8840)),32)
  server.setExecutor(pool)
  private def tls(n:JsonNode):TlsConfig=TlsConfig(ca=Option(n.get("ca")).map(v=>Path.of(v.asText)),tokenFile=Option(n.get("token_file")).map(v=>Path.of(v.asText)))
  private def target(n:JsonNode):JsonNode=configuredTarget
  private def fetchOperators():JsonNode={
    val repo=configuredRepository
    val response=http.send(HttpRequest.newBuilder(URI.create(repo.path("url").asText.stripSuffix("/")+"/v1/operators")).timeout(Duration.ofSeconds(15)).GET().build(),HttpResponse.BodyHandlers.ofInputStream())
    val in=response.body();val bytes=try in.readNBytes(4*1024*1024+1)finally in.close()
    require(response.statusCode()==200,"Operator repository is unavailable");require(bytes.length<=4*1024*1024,"Operator list exceeds 4 MiB")
    val n=Json.mapper.readTree(bytes);require(n.isArray,"Invalid operator catalog");n.elements.asScala.foreach{o=>dacp.operator.OperatorPresentation.validate(o);dacp.operator.ParameterSchema.validate(o.get("parameters"))};n
  }
  private val inferenceCache=Path.of(config.path("operator_cache").asText(".designer-operator-cache")).toAbsolutePath
  private val inferencePlugins=new PluginEnvironment(Option(config.get("plugin_directory")).map(n=>Path.of(n.asText)))
  private val workerSettings=InferenceWorkerSettings.read(Option(config.get("inference_workers")).map(n=>Json.mapper.convertValue(n,classOf[java.util.LinkedHashMap[String,Object]]).asInstanceOf[Object]))
  private val inferenceRuntime=new OperatorInferenceService(inferenceCache.resolve("local"),Seq(configuredRepository.path("url").asText),
    inferenceCache,if(config.has("operator_max_bytes"))Json.number(config,"operator_max_bytes",1,8L*1024*1024*1024)else 8L*1024*1024*1024,inferencePlugins,workerSettings)
  private val inference=new DesignerInference(inferenceRuntime.infer,describeItem,previewItem)
  private def withItem[A](value:String,cancel:StreamCancel)(f:(Dataset,String)=>A):A={
    val uri=URI.create(value)
    require(uri.getScheme=="dacp" && uri.getHost!=null && uri.getUserInfo==null && uri.getQuery==null && uri.getFragment==null,"expected public Item URI")
    val origin=new URI("dacp",null,uri.getHost,uri.getPort,null,null,null).toString
    require(Json.origin(origin)==Json.origin(configuredTarget.path("url").asText),"Data sources must use the configured server's public Item URIs")
    val parts=Option(uri.getPath).getOrElse("").stripPrefix("/").split("/",2)
    require(parts.length==2 && parts.forall(_.nonEmpty),"expected dataset and Item path")
    cancel.active()
    Using.resource(cancel.add(Source.open(origin+"/"+parts(0),tls(configuredTarget),15)))(dataset=>f(dataset,"/"+parts(1)))
  }
  private def describeItem(value:String,view:String,cancel:StreamCancel):StreamDescriptor=withItem(value,cancel){(dataset,path)=>
    val metadata=dataset.describe(path)
    require(metadata.kind=="data","input must be a data item")
    val selected=ItemViews.select(view,metadata.views)
    val schema=if(selected=="records")metadata.schema else None
    StreamDescriptor(Set(selected),Set(metadata.format),schema,if(selected=="bytes" || schema.nonEmpty)"known"else "unknown")
  }
  private def sampleValue(value:Any):JsonNode=value match {
    case null=>Json.mapper.nullNode()
    case v:org.apache.arrow.vector.util.Text=>Json.mapper.getNodeFactory.textNode(v.toString)
    case v:java.util.Map[_,_]=>val n=Json.obj();v.asScala.foreach{case(k,x)=>n.set[JsonNode](k.toString,sampleValue(x))};n
    case v:java.util.List[_]=>val n=Json.mapper.createArrayNode();v.asScala.foreach(x=>n.add(sampleValue(x)));n
    case v:java.time.temporal.TemporalAccessor=>Json.mapper.getNodeFactory.textNode(v.toString)
    case v:Array[Byte]=>Json.mapper.getNodeFactory.textNode(java.util.Base64.getEncoder.encodeToString(v))
    case v=>Json.mapper.valueToTree[JsonNode](v)
  }
  private def previewItem(value:String,view:String,limits:PreviewLimits,cancel:StreamCancel):Option[OutputSample]=withItem(value,cancel){(dataset,path)=>
    Using.resource(cancel.add(dataset.open(path,limit=if(view=="records")Some(limits.maxRows.toLong+1)else None))){item=>
      val sample=if(view=="bytes")Using.resource(cancel.add(item.asBytes())){stream=>
        val bytes=stream.readNBytes(limits.maxBytes+1)
        OutputSample("bytes","binary",data=bytes.toVector,truncated=bytes.length>limits.maxBytes).bounded(limits)
      }else Using.resource(cancel.add(item.asRecords())){stream=>
        var result=OutputSample("records","json");var done=false
        while(!done && stream.next()){
          val root=stream.root;var i=0
          while(!done && i<root.getRowCount){
            cancel.active()
            if(result.rows.size>=limits.maxRows){result=result.copy(truncated=true);done=true}
            else {
              val row=Json.obj();root.getFieldVectors.asScala.foreach(v=>row.set[JsonNode](v.getName,sampleValue(v.getObject(i))))
              val candidate=result.copy(rows=result.rows:+row)
              if(OutputSamples.size(candidate)>limits.maxBytes){result=result.copy(truncated=true);done=true}else result=candidate
            }
            i+=1
          }
        }
        result
      }
      Some(sample)
    }
  }
  private def publicConfig:JsonNode={
    val n=Json.obj().put("chat_provider",chat.name)
    n.putObject("generator").put("name",DesignerLayout.name).put("version",DesignerLayout.softwareVersion)
    n.putObject("target").put("kind",configuredTarget.path("kind").asText).put("url",configuredTarget.path("url").asText)
    n
  }
  private def document(n:JsonNode,empty:Boolean=false):JsonNode={val j=Documents.job(n.path("job_yaml").asText,empty);Documents.variables(n.path("variables_yaml").asText);j}
  private def api(path:String,n:JsonNode):JsonNode=path match {
    case "/api/operators"=>fetchOperators()
    case "/api/nodes"=>val t=target(n);if(t.path("kind").asText=="union")Source.nodes(t.path("url").asText,tls(t),15)else {val out=Json.obj();out.putArray("nodes");out}
    case "/api/datasets"=>
      val t=target(n);val d=Source.datasets(t.path("url").asText,tls(t),15);val out=Json.obj().put("complete",d.complete).put("unavailable_sources",d.unavailableSources);val a=out.putArray("datasets")
      d.foreach(v=>a.addObject().put("name",v.name).put("uri",v.uri).put("description",v.description));out
    case "/api/items"=>
      val t=target(n);val uri=n.path("uri").asText
      val itemUri=URI.create(uri);val itemOrigin=new URI(itemUri.getScheme,null,itemUri.getHost,itemUri.getPort,null,null,null).toString
      require(Json.origin(itemOrigin)==Json.origin(t.path("url").asText),"Item browsing must use the connected server")
      val out=Json.obj();val list=out.putArray("items")
      Using.resource(Source.open(uri,tls(t),15)){ds=>ds.list(n.path("path").asText("/")).foreach{i=>val x=list.addObject().put("path",i.path).put("kind",i.kind).put("format",i.format);val vs=x.putArray("views");i.views.foreach(vs.add);i.schema.foreach(s=>x.set[JsonNode]("schema",Json.mapper.readTree(s.toJson)))}}
      out
    case "/api/mapping"=>Documents.mapping(n.path("yaml").asText)
    case "/api/document"=>val j=document(n,empty=true);val out=Json.obj().put("job_yaml",YamlDocument.dump(j)).put("variables_yaml",YamlDocument.dump(Documents.variables(n.path("variables_yaml").asText)));out.set[JsonNode]("job",j);out
    case "/api/export"=>val j=document(n);if(n.path("include_designer").asBoolean(true))DesignerLayout.stamp(j) else j.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].remove("designer");Json.obj().put("job_yaml",YamlDocument.dump(j)).put("variables_yaml",YamlDocument.dump(Documents.variables(n.path("variables_yaml").asText)))
    case "/api/infer"=>
      document(n)
      val job=Documents.job(WorkflowVariables.resolve(n.path("job_yaml").asText,Documents.variables(n.path("variables_yaml").asText)))
      Using.resource(new StreamCancel(()=>pool.isShutdown,120)){cancel=>SchemaPreview.present(inference.infer(job,cancel,Option(n.get("preview")).map(OutputSamples.limits)))}
    case "/api/chat"=>
      document(n,empty=true);require(n.path("message").isTextual && n.path("message").asText.trim.nonEmpty && n.path("message").asText.length<=8000,"Enter a message of up to 8000 characters")
      val request=n.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();request.set[JsonNode]("operators",fetchOperators())
      val t=target(n);val nodeList=if(t.path("kind").asText=="union")Source.nodes(t.path("url").asText,tls(t),15)else {val out=Json.obj();out.putArray("nodes");out}
      request.set[JsonNode]("nodes",nodeList.path("nodes"));request.put("origin",t.path("url").asText)
      val result=chat.plan(request);require(result!=null && result.isObject && result.path("reply").isTextual && result.path("job_yaml").isTextual && result.path("variables_yaml").isTextual,"Chat must return reply, job_yaml and variables_yaml strings");Documents.job(result.path("job_yaml").asText,allowEmpty=true);Documents.variables(result.path("variables_yaml").asText)
      val available=request.path("operators").elements.asScala.map(o=>o.path("name").asText+"@"+o.path("version").asText).toSet+"read_item@1.0"
      Documents.job(result.path("job_yaml").asText,true).path("tasks").elements.asScala.foreach(t=>require(available(t.path("operator").asText),"Chat proposed an operator outside the connected repository"))
      val validNodes=nodeList.path("nodes").elements.asScala.map(_.path("id").asText).toSet
      Documents.job(result.path("job_yaml").asText,true).path("tasks").elements.asScala.foreach(t=>require(!t.has("node") || validNodes(t.path("node").asText),"Chat proposed an unavailable execution node"))
      result
    case _=>throw new IllegalArgumentException("Unknown endpoint")
  }
  private def inferStream(x:HttpExchange,n:JsonNode):Unit={
    document(n)
    val job=Documents.job(WorkflowVariables.resolve(n.path("job_yaml").asText,Documents.variables(n.path("variables_yaml").asText)))
    val preview=Option(n.get("preview")).map(OutputSamples.limits)
    x.getResponseHeaders.set("Content-Type","application/x-ndjson; charset=utf-8")
    x.getResponseHeaders.set("Cache-Control","no-store")
    x.getResponseHeaders.set("X-Accel-Buffering","no")
    x.sendResponseHeaders(200,0)
    val out=x.getResponseBody
    def event(n:JsonNode):Unit={out.write(Json.mapper.writeValueAsBytes(n));out.write(10);out.flush()}
    try Using.resource(new StreamCancel(()=>pool.isShutdown,120)){cancel=>
      val result=inference.infer(job,cancel,preview,(id,r)=>{
        val wrapper=Json.obj();wrapper.putObject("outputs");wrapper.putObject("tasks").set[JsonNode](id,dacp.StructureInference.json(r))
        val step=Json.obj().put("event","step").put("step",id);step.set[JsonNode]("result",SchemaPreview.present(wrapper).path("tasks").path(id));event(step)
      })
      val done=Json.obj().put("event","complete");done.set[JsonNode]("report",SchemaPreview.present(result));event(done)
    }catch{
      case _:java.io.IOException=>() // Client disconnected; stop before the next step.
      case scala.util.control.NonFatal(e)=>try event(Json.obj().put("event","error").put("message",Option(e.getMessage).getOrElse("Inference failed")))catch{case _:java.io.IOException=>()}
    }finally x.close()
  }
  private val assets=Map("/validation.js"->("validation.js","application/javascript"),"/schema-types.js"->("schema-types.js","application/javascript"),"/yaml-editor.js"->("yaml-editor.js","application/javascript"),"/editor-licenses.txt"->("editor-licenses.txt","text/plain; charset=utf-8"),"/parameters.js"->("parameters.js","application/javascript"),"/"->("index.html","text/html; charset=utf-8"),"/app.js"->("app.js","application/javascript"),"/style.css"->("style.css","text/css"),"/dacp-logo.png"->("dacp-logo.png","image/png"),"/favicon.ico"->("dacp-favicon.png","image/png"))
  private def send(x:HttpExchange,status:Int,mime:String,data:Array[Byte]):Unit={
    x.getResponseHeaders.set("Content-Type",mime);x.getResponseHeaders.set("Cache-Control","no-store");x.getResponseHeaders.set("X-Content-Type-Options","nosniff")
    x.getResponseHeaders.set("Content-Security-Policy","default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' blob:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
    x.sendResponseHeaders(status,data.length);try x.getResponseBody.write(data)finally x.close()
  }
  server.createContext("/",(x:HttpExchange)=>{
    try {
      val path=x.getRequestURI.getPath
      if(x.getRequestMethod=="GET" && path=="/api/config")send(x,200,"application/json",Json.mapper.writeValueAsBytes(publicConfig))
      else if(x.getRequestMethod=="POST" && path.startsWith("/api/")){
        require(Option(x.getRequestHeaders.getFirst("Content-Type")).exists(_.startsWith("application/json")),"Expected application/json")
        Option(x.getRequestHeaders.getFirst("Origin")).foreach(o=>require(URI.create(o).getAuthority==x.getRequestHeaders.getFirst("Host"),"Cross-origin request denied"))
        val body=try x.getRequestBody.readNBytes(1024*1024+1)finally x.getRequestBody.close();require(body.length<=1024*1024,"Request exceeds 1 MiB")
        val n=Json.mapper.readTree(body);require(n!=null && n.isObject,"Expected JSON object");require(!n.has("target") && !n.has("repository"),"Services are selected by server configuration")
        if(path=="/api/infer_stream")inferStream(x,n)else send(x,200,"application/json",Json.mapper.writeValueAsBytes(api(path,n)))
      }else if(x.getRequestMethod=="GET" && assets.contains(path)){
        val(file,mime)=assets(path);val in=getClass.getResourceAsStream("/web/"+file);require(in!=null,"Asset unavailable");val data=try in.readAllBytes()finally in.close();send(x,200,mime,data)
      }else send(x,404,"application/json","{\"error\":\"Not found\"}".getBytes(UTF_8))
    }catch{
      case e:java.util.concurrent.TimeoutException=>send(x,504,"application/json",Json.mapper.writeValueAsBytes(Json.obj().put("error","Schema inference timed out: "+e.getMessage)))
      case e:IllegalArgumentException=>send(x,400,"application/json",Json.mapper.writeValueAsBytes(Json.obj().put("error",Option(e.getMessage).getOrElse("Invalid request"))))
      case e:org.apache.arrow.flight.FlightRuntimeException=>
        System.err.println("Designer upstream request failed: "+e.getMessage)
        send(x,502,"application/json",Json.mapper.writeValueAsBytes(Json.obj().put("error","Server request failed: "+Option(e.status().description()).getOrElse(e.status().code().toString))))
      case scala.util.control.NonFatal(e)=>System.err.println("Designer request failed: "+e.getClass.getSimpleName);send(x,502,"application/json",Json.mapper.writeValueAsBytes(Json.obj().put("error","Server, repository or inference service unavailable; check designer logs and connection configuration")))
    }
  })
  def start():Unit=server.start()
  def address:InetSocketAddress=server.getAddress
  def close():Unit={server.stop(0);pool.shutdownNow();try inferenceRuntime.close()finally inferencePlugins.close()}
}
object Main {
  def main(args:Array[String]):Unit={
    require(args.length==1,"Usage: dacp-job-designer CONFIG.yaml")
    val config=Documents.mapping(Files.readString(Path.of(args(0))))
    val designer=new Designer(config);sys.addShutdownHook(designer.close());designer.start();println("DACP Job Designer listening on "+designer.address)
  }
}
