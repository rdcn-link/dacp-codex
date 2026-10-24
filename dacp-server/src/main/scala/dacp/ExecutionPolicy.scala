package dacp

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json
import scala.jdk.CollectionConverters._

/** Policy covers connected output streams, never arbitrary operator side effects. */
final case class ExecutionPolicy(submitted:JsonNode,effective:JsonNode,materialized:Set[(String,String)],maxRetries:Int,timeoutSeconds:Option[Long])
object ExecutionPolicy {
  def connected(plan:JobPlan):Set[(String,String)]=plan.outputs.values.toSet++plan.tasks.flatMap(_.inputs.values.flatMap(b=>b.task.map(_->b.port)))
  def parse(raw:JsonNode,plan:JobPlan):ExecutionPolicy={
    Json.fields(raw,Set.empty,Set("materialization","max_retries","timeout_seconds"))
    val maxRetries=if(raw.has("max_retries"))Json.number(raw,"max_retries",0,10).toInt else 0
    val timeout=if(raw.has("timeout_seconds"))Some(Json.number(raw,"timeout_seconds",1,86400))else None
    val m=Option(raw.get("materialization")).getOrElse(Json.obj())
    Json.fields(m,Set.empty,Set("allowed","default","tasks"))
    require(!m.has("allowed") || m.path("allowed").isBoolean,"materialization.allowed must be boolean")
    val allowed=m.path("allowed").asBoolean(true)
    def mode(n:JsonNode):String={require(n.isTextual && Set("stream","materialize")(n.asText),"output mode must be stream or materialize");n.asText}
    val default=Option(m.get("default")).map(mode).getOrElse("stream")
    require(allowed || default=="stream","workflow forbids materialization")
    val tasks=Option(m.get("tasks")).getOrElse(Json.obj());require(tasks.isObject,"materialization.tasks must be an object")
    val ids=plan.tasks.map(_.id).toSet;val ports=connected(plan)
    val overrides=tasks.fields.asScala.flatMap { e=>
      require(ids(e.getKey),"unknown policy task: "+e.getKey);Json.fields(e.getValue,Set("outputs"))
      val outputs=e.getValue.path("outputs");require(outputs.isObject,"policy outputs must be an object")
      outputs.fields.asScala.map { p=>val key=e.getKey->p.getKey;require(ports(key),"unknown or unconnected policy output: "+e.getKey+"."+p.getKey);key->mode(p.getValue)}
    }.toMap
    val selected=ports.filter(p=>overrides.getOrElse(p,default)=="materialize")
    require(allowed || selected.isEmpty,"workflow forbids materialization")
    val effective=Json.obj().put("max_retries",maxRetries);timeout.foreach(effective.put("timeout_seconds",_));val em=effective.putObject("materialization").put("allowed",allowed).put("default",default);val ts=em.putObject("tasks")
    ports.toVector.sorted.groupBy(_._1).toVector.sortBy(_._1).foreach {case(task,ps)=>val out=ts.putObject(task).putObject("outputs");ps.foreach(p=>out.put(p._2,if(selected(p))"materialize"else "stream"))}
    ExecutionPolicy(raw.deepCopy[JsonNode](),effective,selected,maxRetries,timeout)
  }
  def key(execution:String,task:String):String=java.security.MessageDigest.getInstance("SHA-256").digest(Json.mapper.writeValueAsBytes(Json.obj().put("execution_id",Json.id(execution)).put("task",Json.id(task)))).map(b=>f"${b & 255}%02x").mkString
  def signature(task:JsonNode):JsonNode={val n=Json.obj();Seq("id","operator","params","inputs","depends_on").foreach(k=>n.set[JsonNode](k,task.path(k)));n}
}
