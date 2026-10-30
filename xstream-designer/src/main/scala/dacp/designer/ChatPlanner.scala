package xstream.designer


import com.fasterxml.jackson.databind.JsonNode
import dacp.union.{Json,YamlDocument}
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.time.Duration
import scala.jdk.CollectionConverters._

/** Model adapters are independently installable with Java ServiceLoader. */
trait ChatPlanner {
  def name:String
  def plan(request:JsonNode):JsonNode
}
object ChatPlanner {
  val instructions="""Design DACP YAML jobs. Return a JSON object with reply, job_yaml and variables_yaml.
Preserve the current draft unless asked to replace it. Job fields: name,tasks,outputs; optional designer holds presentation metadata and does not affect execution. Preserve designer metadata when modifying existing steps. Step fields: id,title,operator,inputs,params.
Use only available versioned operators. Their static parameters definitions declare permitted names, types and constraints; follow these declarations. Inputs reference task[:port]; default port is result. No depends_on or inline dataset bindings.
Use read_item@1.0 with empty inputs and params.uri (context:/dataset/item for the current DataCatalog, or dacp://host:port/dataset/item for an explicit service); optional params.sql queries FROM source and requires records view for data sources. Source generators also have empty inputs.
Design a logical stream graph without execution placement. Use public dataset URIs from the configured DACP service.
An output has at most one consumer. The Job Outlet accepts at most one step output port. Empty job outputs and sink operators are permitted.
Variables are complete values like $INPUT_URI, with typed values supplied in a separate YAML mapping.
Do not invent schemas. Inference is a separate metadata request from the editor. Never run jobs or claim they ran."""
  def configured(config:JsonNode):ChatPlanner={
    val provider=config.path("provider").asText("builtin")
    if(provider=="builtin")new BuiltinChat
    else if(provider=="http")new HttpChat(config)
    else if(provider=="openai-compatible")new CompletionChat(config)
    else java.util.ServiceLoader.load(classOf[ChatPlanner]).iterator.asScala.find(_.name==provider).getOrElse(throw new IllegalArgumentException("Chat provider not installed: "+provider))
  }
}
class HttpChat(config:JsonNode) extends ChatPlanner {
  def name="http"
  private val endpoint=URI.create(config.path("url").asText)
  require(Set("http","https")(endpoint.getScheme) && endpoint.getHost!=null,"Expected chat adapter HTTP URL")
  private val client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
  def plan(request:JsonNode):JsonNode={
    val body=request.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();body.put("instructions",ChatPlanner.instructions)
    exchange(body)
  }
  protected def exchange(body:JsonNode):JsonNode={
    val builder=HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(90)).header("Content-Type","application/json")
    Option(config.get("token_env")).foreach{n=>val token=sys.env.getOrElse(n.asText,throw new IllegalArgumentException("Chat adapter token is not configured"));builder.header("Authorization","Bearer "+token)}
    val response=client.send(builder.POST(HttpRequest.BodyPublishers.ofByteArray(Json.mapper.writeValueAsBytes(body))).build(),HttpResponse.BodyHandlers.ofInputStream())
    val in=response.body();val bytes=try in.readNBytes(1024*1024+1)finally in.close()
    require(response.statusCode()==200,"Chat adapter request failed")
    require(bytes.length<=1024*1024,"Chat response exceeds 1 MiB");Json.mapper.readTree(bytes)
  }
}
/** Standard chat-completions adapter, usable with self-hosted compatible model services. */
final class CompletionChat(config:JsonNode) extends HttpChat(config) {
  override def name="openai-compatible"
  require(config.path("model").asText.trim.nonEmpty,"Configure a chat model")
  override def plan(request:JsonNode):JsonNode={
    val body=Json.obj().put("model",config.path("model").asText).put("stream",false)
    val messages=body.putArray("messages")
    messages.addObject().put("role","system").put("content",ChatPlanner.instructions+" Return only one JSON object, without markdown fences.")
    request.path("messages").elements.asScala.toVector.takeRight(12).foreach{m=>
      val role=m.path("role").asText
      require(Set("user","assistant")(role) && m.path("content").isTextual,"Invalid chat history")
      messages.addObject().put("role",role).put("content",m.path("content").asText)
    }
    val context=request.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();context.remove("messages")
    messages.addObject().put("role","user").put("content","Current draft, available operators, and request:\n"+context.toString)
    val response=exchange(body);val choice=response.path("choices").path(0)
    require(choice.path("finish_reason").asText!="length","Model response was truncated")
    val content=choice.path("message").path("content")
    require(content.isTextual,"Model returned no text proposal")
    val text=content.asText.trim.stripPrefix("```json").stripPrefix("```").stripSuffix("```").trim
    Json.mapper.readTree(text)
  }
}
/** Offline command planner; deliberately reports its limited grammar rather than posing as an LLM. */
final class BuiltinChat extends ChatPlanner {
  val name="builtin"
  def plan(request:JsonNode):JsonNode={
    val message=request.path("message").asText.trim
    val job=Documents.job(request.path("job_yaml").asText,allowEmpty=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    val tasks=job.path("tasks").asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode]
    val variables=Documents.variables(request.path("variables_yaml").asText).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    var reply="Use: add OPERATOR@VERSION; connect STEP[:PORT] to STEP:INPUT; set variable NAME = YAML_VALUE; rename JOB_NAME; or ‘create CSV filter job’. Configure a model adapter for unrestricted natural language."
    val add="(?i)(?:add|添加)\\s+([A-Za-z0-9_-]+@[A-Za-z0-9_.-]+)".r
    val connect="(?i)(?:connect|连接)\\s+([A-Za-z0-9_:-]+)\\s+(?:to|到)\\s+([A-Za-z0-9_-]+):([A-Za-z0-9_-]+)".r
    val variable="(?s)(?i)(?:set variable|变量)\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(.+)".r
    def task(id:String)=tasks.elements.asScala.find(_.path("id").asText==id).getOrElse(throw new IllegalArgumentException("Unknown step: "+id)).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    message match {
      case add(op)=>
        require(request.path("operators").elements.asScala.exists(n=>n.path("name").asText+"@"+n.path("version").asText==op) || op=="read_item@1.0","Operator not in connected repository")
        var i=tasks.size()+1;while(tasks.elements.asScala.exists(_.path("id").asText==s"step_$i"))i+=1
        val t=tasks.addObject().put("id",s"step_$i").put("title",op).put("operator",op);t.putObject("inputs");t.putObject("params");reply=s"Added step_$i. Connect its input ports in the canvas."
      case connect(from,to,port)=>task(to).withObject("/inputs").put(port,from);reply="Connected the steps."
      case variable(key,value)=>val parsed=YamlDocument.parse("value: "+value);variables.set[JsonNode](key,parsed.path("value"));reply="Updated typed variable "+key+"."
      case _ if message.toLowerCase.startsWith("rename ") || message.startsWith("重命名 ")=>job.put("name",message.substring(message.indexOf(' ')+1).trim);reply="Updated job name."
      case _ if message.toLowerCase.contains("csv") && (message.toLowerCase.contains("filter") || message.contains("过滤"))=>
        val available=request.path("operators").elements.asScala.map(n=>n.path("name").asText+"@"+n.path("version").asText).toSet
        require(Set("piflow_csv_parser@1.0","piflow_filter@1.0","piflow_csv_save@1.0").subsetOf(available),"CSV template requires parser, filter and CSV output operators in the repository")
        tasks.removeAll();job.put("name","Weather CSV filtering")
        val read=tasks.addObject().put("id","read").put("title","Read observations").put("operator","read_item@1.0");read.putObject("inputs");read.putObject("params").put("uri","$INPUT_URI").put("view","bytes")
        Seq(("parse","piflow_csv_parser@1.0","read"),("filter","piflow_filter@1.0","parse"),("export","piflow_csv_save@1.0","filter")).foreach{case(id,op,from)=>val t=tasks.addObject().put("id",id).put("title",id).put("operator",op);t.putObject("inputs").put("source",from);if(id=="filter")t.putObject("params").put("condition","$CONDITION")}
        job.putObject("outputs").put("result","export");if(!variables.has("INPUT_URI"))variables.put("INPUT_URI","context:/DATASET/ITEM");if(!variables.has("CONDITION"))variables.put("CONDITION","quality = '0'")
        reply="Created a CSV parsing, filtering and output chain. Set INPUT_URI to an Item from the connected directory."
      case _=>()
    }
    Json.obj().put("reply",reply).put("job_yaml",YamlDocument.dump(job)).put("variables_yaml",YamlDocument.dump(variables))
  }
}
