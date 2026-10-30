package dacp


import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable
import scala.util.control.NonFatal

/** Host-specific steps carry opaque payloads; propagation never depends on placement or transport. */
final case class InferenceNode[A](id:String,inputs:Map[String,(String,String)],value:A)
object InferenceGraph {
  def propagate[A](nodes:Vector[InferenceNode[A]],outputs:Map[String,(String,String)],collectErrors:Boolean=false,onStep:(String,InferenceResult)=>Unit=(_,_)=>())
    (infer:(A,Map[String,StreamDescriptor])=>InferenceResult):JsonNode={
    require(nodes.map(_.id).distinct.size==nodes.size,"duplicate inference step")
    val byId=nodes.map(n=>n.id->n).toMap
    val visiting=mutable.Set.empty[String]
    val inferred=mutable.Map.empty[String,InferenceResult]
    val report=StructureInference.mapper.createObjectNode();val tasks=report.putObject("tasks")
    // Structural errors remain fatal, including when collecting per-step diagnostics.
    val checked=mutable.Set.empty[String]
    def check(id:String):Unit=if(!checked(id)){
      require(!visiting(id),"inference graph contains a cycle")
      val node=byId.getOrElse(id,throw new IllegalArgumentException("unknown inference step: "+id))
      visiting+=id;node.inputs.values.foreach(p=>check(p._1));visiting-=id;checked+=id
    }
    nodes.foreach(n=>check(n.id));outputs.values.foreach(p=>check(p._1))
    def visit(id:String):InferenceResult=inferred.getOrElseUpdate(id,{
      require(!visiting(id),"inference graph contains a cycle")
      val node=byId.getOrElse(id,throw new IllegalArgumentException("unknown inference step: "+id))
      visiting+=id
      val upstream=node.inputs.values.map(p=>p._1->visit(p._1)).toMap
      val failed=upstream.collect{case(name,result)if result.diagnostics.exists(_.severity=="error")=>name}.toVector.sorted
      val result=if(collectErrors && failed.nonEmpty)InferenceResult(Map.empty,
        Vector(InferenceDiagnostic("error","upstream_failed","Blocked by upstream step(s): "+failed.mkString(", "))))
      else try {
        val inputs=node.inputs.map{case(port,(parent,output))=>
          port->upstream(parent).outputs.getOrElse(output,throw new IllegalArgumentException(s"unknown output $parent:$output"))
        }
        val r=infer(node.value,inputs);if(!collectErrors)r.requireValid();r
      }catch{
        case e:java.util.concurrent.CancellationException=>throw e
        case e:java.util.concurrent.TimeoutException=>throw e
        case NonFatal(e) if collectErrors=>InferenceResult(Map.empty,
          Vector(InferenceDiagnostic("error","inference_failed",Option(e.getMessage).getOrElse(e.getClass.getSimpleName))))
      }
      visiting-=id;tasks.set[JsonNode](id,StructureInference.json(result));onStep(id,result);result
    })
    nodes.foreach(n=>visit(n.id))
    val selected=outputs.flatMap{case(name,(step,port))=>visit(step).outputs.get(port) match{
      case Some(value)=>Some(name->value)
      case None if collectErrors=>None
      case None=>throw new IllegalArgumentException(s"unknown selected output $step:$port")
    }}
    report.set[JsonNode]("outputs",StructureInference.portsJson(selected));report
  }
}
