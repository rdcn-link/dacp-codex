package dacp

import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._
import org.yaml.snakeyaml.Yaml

case class InputBinding(uri: Option[String] = None, task: Option[String] = None, port: String = "result", view: String = "bytes")
case class Task(id: String, inputs: Map[String,InputBinding], operator: String, params: java.util.Map[String,Object], node: Option[String] = None, source: Option[SourceOperator] = None, title: Option[String] = None, outputPorts:Seq[String]=Seq.empty, proxyPath:Vector[String]=Vector.empty)
case class JobPlan(tasks: Vector[Task], outputs: Map[String,(String,String)], name:String="Job")
case class SourceOperator(runtime: String, source: String, entrypoint: String = "Main")
object Jobs {
  val ReadItem = "read_item@1.0"
  def dependencies(t:Task):Vector[String]=t.inputs.values.flatMap(_.task).toVector.distinct
  def roots(plan:JobPlan):Set[String]={val upstream=plan.tasks.flatMap(dependencies).toSet;plan.tasks.map(_.id).filterNot(upstream).toSet++plan.outputs.values.map(_._1)}
  def reference(value:Any):(String,String)={
    require(value.isInstanceOf[String],"stream binding must be task[:port]")
    val parts=value.toString.split(":",-1);require(parts.length==1 || parts.length==2,"stream binding must be task[:port]")
    val task=parts(0);val port=if(parts.length==1)"result"else parts(1);name(task);name(port);task->port
  }
  def readBinding(t:Task):Option[InputBinding]=if(t.operator!=ReadItem)None else {
    require(t.inputs.isEmpty && t.source.isEmpty,"read_item is a zero-input transport source")
    require(t.params.containsKey("uri") && t.params.keySet.asScala.toSet.subsetOf(Set("uri","view")),"read_item params require uri and optional view")
    val uri=t.params.get("uri");val view=t.params.get("view")
    require(uri.isInstanceOf[String] && (!t.params.containsKey("view") || view.isInstanceOf[String]),"read_item uri/view must be strings")
    require(!t.params.containsKey("view") || Set("bytes","records")(view.toString),"unsupported read_item view")
    val b=InputBinding(uri=Some(uri.toString),view=if(t.params.containsKey("view"))view.toString else "auto")
    val parsed=DacpUri.parse(uri.toString);require(parsed.getPath.stripPrefix("/").split("/",2).length==2,"read_item requires an Item URI")
    Some(b)
  }

  /** Contracting node regions must not turn an acyclic task graph into a cycle. */
  def canGroupByNode(plan:JobPlan,nodes:Map[String,String]):Boolean={
    val edges=plan.tasks.flatMap(t=>dependencies(t).map(p=>nodes(p)->nodes(t.id))).filter(e=>e._1!=e._2).distinct
    var left=nodes.values.toSet;var done=Set.empty[String]
    while(left.nonEmpty){val ready=left.filter(n=>edges.filter(_._2==n).forall(e=>done(e._1)));if(ready.isEmpty)return false;done++=ready;left--=ready}
    true
  }

  def authorizeSource(config: Config, task: Task): Unit =
    require(task.source.isEmpty || config.debug, "FORBIDDEN: source submission requires DEBUG mode")
  def source(value: Object): SourceOperator = {
    val m=mapping(value,"source operator")
    require(m.keySet.subsetOf(Set("runtime","source","entrypoint")) && Set("runtime","source").subsetOf(m.keySet),"invalid source operator")
    val result=SourceOperator(m("runtime").toString,m("source").toString,m.getOrElse("entrypoint","Main").toString)
    require(result.runtime.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"runtime must be name@version")
    require(result.source.trim.nonEmpty && result.source.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=512*1024,"source must contain 1 to 512 KiB")
    result
  }

  private def name(s: String): Unit = require(s.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"invalid task/port name")
  private def mapping(value: Any, label: String): Map[String,Object] = {
    require(value.isInstanceOf[java.util.Map[_,_]],s"$label must be a mapping")
    scala.collection.immutable.VectorMap.from(value.asInstanceOf[java.util.Map[String,Object]].asScala)
  }
  def validate(t: Task): Unit = {
    t.title.foreach(v=>require(v.trim.nonEmpty && v.length<=256 && !v.exists(_.isControl),"title must contain 1 to 256 characters without control characters"))
    name(t.id); t.inputs.keys.foreach(name)
    require(t.source.isDefined || t.operator.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"operator must be name@version")
    require(t.params != null,"params must be a mapping")
    t.inputs.values.foreach { b =>
      require(b.uri.isDefined != b.task.isDefined,"input requires exactly one of uri or task")
      require(Set("bytes","records")(b.view),"unsupported input view")
      name(b.port)
      b.task.foreach(name)
      b.uri.foreach { u => val parsed=DacpUri.parse(u); require(parsed.getPath.stripPrefix("/").split("/",2).length == 2,"operator input must address an item, not a dataset") }
    }
    readBinding(t)
    t.node.foreach(n=>if(n.startsWith("nd-"))require(n.matches("nd-[a-z2-9]{4}-[a-z2-9]{4}"),"invalid logical node")else DacpUri.node(n))
  }
  def load(path: Path): JobPlan = parse(Files.readString(path))
  def parse(text: String): JobPlan = {
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 1024*1024,"job exceeds 1 MiB")
    val options=new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val raw=try dacp.union.Json.mapper.convertValue(dacp.union.YamlDocument.parse(text),classOf[Object])catch{case e:org.yaml.snakeyaml.error.YAMLException=>throw new IllegalArgumentException("invalid job YAML",e)}
    val doc=mapping(raw,"job")
    require(doc.keySet.subsetOf(Set("name","tasks","outputs","designer")) && doc.contains("name"),"invalid job settings")
    require(doc.get("tasks").exists(_.isInstanceOf[java.util.List[_]]),"tasks must be a list")
    val tasks=doc("tasks").asInstanceOf[java.util.List[Object]].asScala.map { raw =>
      val m=mapping(raw,"task")
      require(m.keySet.subsetOf(Set("id","title","inputs","operator","params","node")) && Set("id","inputs","operator").subsetOf(m.keySet),"invalid task settings")
      val inputs=mapping(m("inputs"),"inputs").map { case (port,raw) =>
        val (task,output)=reference(raw)
        port -> InputBinding(task=Some(task),port=output)
      }
      val params=new java.util.LinkedHashMap[String,Object](); m.get("params").foreach(v => mapping(v,"params").foreach{case(k,v)=>params.put(k,v)})
      val inline=if(m("operator").isInstanceOf[java.util.Map[_,_]])Some(source(m("operator")))else None
      require(m.get("title").forall(_.isInstanceOf[String]),"title must be a string")
      val t=Task(m("id").toString,inputs,if(inline.isDefined)""else m("operator").toString,params,m.get("node").map(_.toString),inline,m.get("title").map(_.toString));validate(t);t
    }.toVector
    require(tasks.nonEmpty && tasks.size <= 64 && tasks.map(_.id).distinct.size == tasks.size,"empty, oversized, or duplicate task list")
    val ids=tasks.map(_.id).toSet
    tasks.foreach(t=>dependencies(t).foreach(id=>require(ids(id),"unknown upstream task")))
    var left=tasks; var done=Set.empty[String]; val result=Vector.newBuilder[Task]
    while(left.nonEmpty) {
      val ready=left.filter(t=>dependencies(t).forall(done))
      require(ready.nonEmpty,"cyclic task DAG")
      ready.foreach(t=>{result+=t;done+=t.id});left=left.filterNot(ready.contains)
    }
    val outputs=doc.get("outputs").map(v=>mapping(v,"outputs").map {case(label,raw)=>
      name(label);val (task,port)=reference(raw);require(ids(task),"unknown output task");label -> (task,port)
    }).getOrElse(Map.empty[String,(String,String)])
    require(doc("name").isInstanceOf[String] && doc("name").toString.trim.nonEmpty && doc("name").toString.length<=256 && !doc("name").toString.exists(_.isControl),"name must contain 1 to 256 characters without control characters")
    JobPlan(result.result(),outputs,doc("name").toString)
  }
}
