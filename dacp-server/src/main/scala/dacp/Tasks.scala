package dacp

import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._
import org.yaml.snakeyaml.Yaml

case class InputBinding(uri: Option[String] = None, task: Option[String] = None, port: String = "result", view: String = "bytes")
case class Task(id: String, inputs: Map[String,InputBinding], operator: String, params: java.util.Map[String,Object], node: Option[String] = None, source: Option[SourceOperator] = None)
case class JobPlan(tasks: Vector[Task], outputs: Map[String,(String,String)])
case class SourceOperator(runtime: String, source: String, entrypoint: String = "Main")
object Jobs {
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
    value.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
  }
  def validate(t: Task): Unit = {
    name(t.id); t.inputs.keys.foreach(name)
    require(t.inputs.nonEmpty,"named item inputs required")
    require(t.source.isDefined || t.operator.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"operator must be name@version")
    require(t.params != null,"params must be a mapping")
    t.inputs.values.foreach { b =>
      require(b.uri.isDefined != b.task.isDefined,"input requires exactly one of uri or task")
      require(Set("bytes","records")(b.view),"unsupported input view")
      name(b.port)
      b.task.foreach(name)
      b.uri.foreach { u => val parsed=DacpUri.parse(u); require(parsed.getPath.stripPrefix("/").split("/",2).length == 2,"operator input must address an item, not a dataset") }
    }
    t.node.foreach(DacpUri.node)
  }
  def load(path: Path): JobPlan = parse(Files.readString(path))
  def parse(text: String): JobPlan = {
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 1024*1024,"job exceeds 1 MiB")
    val options=new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val doc=mapping(new Yaml(options).load[Object](text),"job")
    require(doc.keySet.subsetOf(Set("job","tasks","outputs")) && doc.contains("job"),"invalid job settings")
    require(doc.get("tasks").exists(_.isInstanceOf[java.util.List[_]]),"tasks must be a list")
    val tasks=doc("tasks").asInstanceOf[java.util.List[Object]].asScala.map { raw =>
      val m=mapping(raw,"task")
      require(m.keySet.subsetOf(Set("id","inputs","operator","params","node")) && Set("id","inputs","operator").subsetOf(m.keySet),"invalid task settings")
      val inputs=mapping(m("inputs"),"inputs").map { case (port,raw) =>
        val b=mapping(raw,"input binding")
        require(b.keySet.subsetOf(Set("uri","task","port","view")),"unknown input binding setting")
        port -> InputBinding(b.get("uri").map(_.toString),b.get("task").map(_.toString),b.getOrElse("port","result").toString,b.getOrElse("view","bytes").toString)
      }
      val params=new java.util.LinkedHashMap[String,Object](); m.get("params").foreach(v => mapping(v,"params").foreach{case(k,v)=>params.put(k,v)})
      val inline=if(m("operator").isInstanceOf[java.util.Map[_,_]])Some(source(m("operator")))else None
      val t=Task(m("id").toString,inputs,if(inline.isDefined)""else m("operator").toString,params,m.get("node").map(_.toString),inline);validate(t);t
    }.toVector
    require(tasks.nonEmpty && tasks.size <= 64 && tasks.map(_.id).distinct.size == tasks.size,"empty, oversized, or duplicate task list")
    val ids=tasks.map(_.id).toSet
    tasks.foreach(_.inputs.values.flatMap(_.task).foreach(id=>require(ids(id),"unknown upstream task")))
    var left=tasks; var done=Set.empty[String]; val result=Vector.newBuilder[Task]
    while(left.nonEmpty) {
      val ready=left.filter(_.inputs.values.flatMap(_.task).forall(done))
      require(ready.nonEmpty,"cyclic task DAG")
      ready.foreach(t=>{result+=t;done+=t.id});left=left.filterNot(ready.contains)
    }
    val outputs=doc.get("outputs").map(v=>mapping(v,"outputs").map {case(label,raw)=>
      name(label);val m=mapping(raw,"output binding")
      require(m.keySet==Set("task","port"),"output requires task and port")
      val task=m("task").toString;val port=m("port").toString;name(port);require(ids(task),"unknown output task");label -> (task,port)
    }).getOrElse(Map.empty[String,(String,String)])
    JobPlan(result.result(),outputs)
  }
}
