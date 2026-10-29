package dacp

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.{Json,YamlDocument}
import scala.jdk.CollectionConverters._

/** Host-supplied remote execution transport. No Union membership or server dependency. */
trait ComputePushdown {
  def nodes:Set[String]
  def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit
}
final case class PushedRegion(id:String,node:String,plan:JobPlan,outputs:Map[(String,String),String])
final case class PushdownPlan(local:JobPlan,regions:Vector[PushedRegion],placements:Map[String,String])
object ComputePushdown {
  /** Only closed source-side regions move: remote execution never needs access to local context. */
  def plan(logical:JobPlan,nodes:Set[String],policy:ExecutionPolicy):PushdownPlan={
    // Keep explicit persistence/replay ownership and its original task/port keys on this engine.
    if(policy.materialized.nonEmpty)return PushdownPlan(logical,Vector.empty,logical.tasks.map(_.id->"local:materialization_policy").toMap)
    val locations=scala.collection.mutable.Map.empty[String,Option[String]]
    logical.tasks.foreach { t=>
      val location=Jobs.readBinding(t) match {
        case Some(b)=>b.uri.filter(_.startsWith("dacp:")).map(DacpUri.origin).filter(nodes)
        case None=>
          val inputs=t.inputs.values.toVector
          val upstream=inputs.flatMap(_.task).map(id=>locations(id))
          if(t.source.isEmpty && inputs.nonEmpty && upstream.size==inputs.size && upstream.forall(_.nonEmpty) && upstream.distinct.size==1)upstream.head else None
      }
      locations(t.id)=location
    }
    val edges=ExecutionPolicy.connected(logical)
    val candidates=logical.tasks.flatMap(t=>locations(t.id).map(_->t)).groupBy(_._1).toVector.sortBy(_._1)
    val regions=candidates.flatMap {case(node,entries)=>
      val tasks=entries.map(_._2);val ids=tasks.map(_.id).toSet
      if(!tasks.exists(_.operator!=Jobs.ReadItem))None else {
        val external=edges.filter {case(id,port)=>ids(id) && (logical.outputs.values.toSet.contains(id->port) || logical.tasks.exists(t=> !ids(t.id) && t.inputs.values.exists(b=>b.task.contains(id) && b.port==port)))}
        val outputs=external.toVector.sorted.zipWithIndex.map{case(pair,i)=>pair->s"p$i"}.toMap
        val remote=tasks.map { t=>
          if(t.operator!=Jobs.ReadItem)t.copy(node=None) else {
            val params=new java.util.LinkedHashMap[String,Object](t.params)
            val uri=DacpUri.parse(params.get("uri").toString)
            params.put("uri",new java.net.URI("context",null,uri.getPath,null).toASCIIString)
            t.copy(node=None,params=params)
          }
        }
        Some(PushedRegion(tasks.last.id,node,JobPlan(remote,outputs.map{case(pair,label)=>label->pair},logical.name.take(247)+" / remote"),outputs))
      }
    }
    val member=regions.flatMap(r=>r.plan.tasks.map(t=>t.id->r)).toMap
    def remap(task:String,port:String):(String,String)=member.get(task).map(r=>r.id->r.outputs(task->port)).getOrElse(task->port)
    val physical=logical.tasks.flatMap { t=>member.get(t.id) match {
      case Some(region) if region.id!=t.id=>None
      case Some(region)=>Some(t.copy(inputs=Map.empty,outputPorts=region.outputs.values.toSeq.sorted))
      case None=>Some(t.copy(inputs=t.inputs.map {case(p,b)=>p->b.task.map { id=>val (task,port)=remap(id,b.port);b.copy(task=Some(task),port=port)}.getOrElse(b)}))
    }}
    val outputs=logical.outputs.map{case(label,(task,port))=>label->remap(task,port)}
    PushdownPlan(logical.copy(tasks=physical,outputs=outputs),regions,logical.tasks.map(t=>t.id->member.get(t.id).map(_.node).getOrElse("local")).toMap)
  }
  def yaml(plan:JobPlan):String={
    val n=Json.obj().put("name",plan.name);val tasks=n.putArray("tasks")
    plan.tasks.foreach { t=>
      require(t.source.isEmpty,"Remote execution requires repository operators")
      val task=tasks.addObject().put("id",t.id).put("operator",t.operator);t.title.foreach(task.put("title",_))
      task.set[JsonNode]("params",Json.mapper.valueToTree(t.params));val inputs=task.putObject("inputs")
      t.inputs.toVector.sortBy(_._1).foreach{case(p,b)=>inputs.put(p,b.task.get+":"+b.port)}
    }
    val outputs=n.putObject("outputs");plan.outputs.toVector.sortBy(_._1).foreach{case(label,(task,port))=>outputs.put(label,task+":"+port)}
    YamlDocument.dump(n)
  }
}

/** Ordinary client calls; receiving services enforce credentials and execution permissions. */
final class DacpComputePushdown(config:JsonNode) extends ComputePushdown {
  require(!config.has("nodes") || config.path("nodes").isArray,"nodes must be a list")
  private val configured=config.path("nodes").elements.asScala.toVector
  private val origins=configured.map(n=>DacpUri.node(n.path("url").asText))
  require(origins.distinct.size==origins.size,"Duplicate configured node origin")
  private val connections=configured.filter { n=>
    require(!n.has("compute_pushdown") || n.path("compute_pushdown").isBoolean,"nodes.compute_pushdown must be boolean")
    n.path("compute_pushdown").asBoolean(true)
  }.map(n=>DacpUri.node(n.path("url").asText)->n).toMap
  val nodes:Set[String]=connections.keySet
  def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit={
    val connection=connections(node)
    val tls=dacp.client.TlsConfig(ca=Option(connection.get("ca")).map(n=>java.nio.file.Path.of(n.asText)),tokenFile=Option(connection.get("token_file")).map(n=>java.nio.file.Path.of(n.asText)))
    cancel.active()
    val policy=Json.obj();policy.putObject("materialization").put("allowed",false).put("default","stream")
    // The client deadline bounds the call; the service keeps its own (possibly shorter) limit.
    // Sending the host's larger policy timeout would be rejected by stricter services.
    val stream=cancel.add(dacp.client.Job.run(node,ComputePushdown.yaml(plan),tls,cancel.remainingSeconds,policy))
    var reported=false;var complete=false
    try {
      while(stream.hasNext){
        cancel.active();val event=stream.next()
        if(!reported)stream.executionId.foreach{id=>onExecution(id);reported=true}
        event.kind match {
          case "start"=>emit(StreamProtocol.start(event.port,event.representation.get,event.format.get,event.schema))
          case "data"|"end"=>emit(StreamProtocol.frame(event.kind,event.port,event.data))
          case "complete"=>complete=true
          case "execution"=>()
          case _=>throw new IllegalArgumentException("Invalid remote Job event")
        }
      }
      require(complete,"Remote Job result stream truncated")
    }finally stream.close()
  }
}
