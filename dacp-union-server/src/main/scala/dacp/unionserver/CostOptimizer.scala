package dacp.unionserver

import dacp.union._

/** Conservative configurable baseline. Does not infer operator semantics from names. */
final class CostOptimizer extends PhysicalPlanOptimizer {
  val id="cost"
  private val defaults=Map("unknown_bytes"->1048576d,"compute_ms"->100d,"startup_ms"->500d,
    "network_bytes_per_second"->100000000d,"network_latency_ms"->2d,
    "local_bytes_per_second"->1000000000d,"local_latency_ms"->0.1d)
  private def value(s:Map[String,Object],k:String):Double=s.get(k).map{v=>require(v.isInstanceOf[java.lang.Number],"planning cost must be numeric: "+k);v.asInstanceOf[java.lang.Number].doubleValue}.getOrElse(defaults(k))
  override def validate(s:Map[String,Object]):Unit={
    require(s.keySet.subsetOf(defaults.keySet),"unknown cost optimizer setting")
    defaults.keys.foreach{k=>val v=value(s,k);require(java.lang.Double.isFinite(v)&&v>=0&&(!k.endsWith("bytes_per_second")||v>0),"invalid cost optimizer setting: "+k)}
  }
  override def estimate(input:PlanningInput,step:PlanningStep,node:PlanningTarget,bytes:Double,s:Map[String,Object]):Option[StepEstimate]=
    Some(StepEstimate(if(step.source.exists(_._2.nonEmpty)||step.dependencies.nonEmpty)bytes else value(s,"unknown_bytes"),
      if(step.operator=="item_source@1.0")0 else value(s,"compute_ms"),if(step.operator=="item_source@1.0")0 else value(s,"startup_ms"),
      confidence=if(step.source.exists(_._2.nonEmpty))"source_metadata"else"estimated"))
  override def link(input:PlanningInput,from:PlanningTarget,to:PlanningTarget,s:Map[String,Object]):Option[LinkEstimate]={
    val prefix=if(from.id==to.id)"local"else"network";Some(LinkEstimate(value(s,prefix+"_bytes_per_second"),value(s,prefix+"_latency_ms")))
  }
}
