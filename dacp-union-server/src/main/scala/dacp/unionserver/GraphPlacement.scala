package dacp.unionserver

import dacp.union._

final case class StepPlacement(target:String,reason:String)
final case class PlanningResult(placements:Map[String,StepPlacement],estimate:PlanEstimate,candidates:Int,score:Double)
/** Bounded whole-graph search. Plugins cannot bypass placement, resource or dependency checks. */
object GraphPlacement {
  def plan(steps:Vector[PlanningStep],targets:Vector[PlanningTarget]):Map[String,StepPlacement]=
    optimize(PlanningInput(steps,targets)).placements
  def optimize(input:PlanningInput,plugin:Option[PhysicalPlanOptimizer]=None,settings:Map[String,Object]=Map.empty,
    check:()=>Unit=()=>()):PlanningResult={
    require(input.beamWidth>=1&&input.beamWidth<=256,"beam_width must be between 1 and 256")
    val steps=input.steps;val targets=input.targets.sortBy(_.id)
    require(steps.map(_.id).distinct.size==steps.size&&targets.map(_.id).distinct.size==targets.size,"duplicate planning identity")
    require(targets.forall(t=>t.id.nonEmpty&&t.capacity>=0&&finite(t.queueMs)&&t.queueMs>=0&&t.freeMemory.forall(_>=0)),"invalid planning target")
    var seen=Set.empty[String]
    steps.foreach{s=>require(s.id.nonEmpty&&s.dependencies.distinct.size==s.dependencies.size&&s.dependencies.forall(seen),"planning steps must be in dependency order");require(s.source.forall(_._2.forall(_>=0)),"invalid source size");seen+=s.id}
    plugin.foreach(_.validate(settings))
    def evaluate(placements:Map[String,String]):PlanEstimate={
      var outputs=Map.empty[String,StepEstimate];var finish=Map.empty[String,Double];var units=Vector.empty[PlannedUnit];var edges=Vector.empty[PlannedEdge]
      var used=Map.empty[String,Int].withDefaultValue(0)
      var memoryUsed=Map.empty[String,BigInt].withDefaultValue(BigInt(0))
      steps.filter(s=>placements.contains(s.id)).foreach { s=>
        check();require(s.dependencies.forall(placements.contains),"candidate omits a dependency")
        val node=targets.find(_.id==placements(s.id)).getOrElse(throw new IllegalArgumentException("candidate has unknown target"))
        require(eligible(s,node,used(node.id)),"candidate violates placement, executor or capacity constraint: "+s.id)
        used+=node.id->(used(node.id)+1)
        val bytes=s.source.flatMap(_._2).map(_.toDouble).getOrElse(if(s.dependencies.nonEmpty)s.dependencies.map(outputs(_).outputBytes).sum else 1048576d)
        val estimate=plugin.flatMap(_.estimate(input,s,node,bytes,settings)).getOrElse(StepEstimate(bytes,if(s.source.nonEmpty)0 else 100,if(s.operator=="item_source@1.0")0 else 500,confidence=if(s.source.exists(_._2.nonEmpty))"source_metadata"else"estimated"))
        require(finite(estimate.outputBytes)&&finite(estimate.computeMs)&&finite(estimate.startupMs)&&estimate.outputBytes>=0&&estimate.computeMs>=0&&estimate.startupMs>=0&&estimate.memoryBytes>=0,"invalid or infeasible step estimate")
        memoryUsed+=node.id->(memoryUsed(node.id)+estimate.memoryBytes)
        if(node.freeMemory.exists(f=>BigInt(f)<memoryUsed(node.id)))throw new InfeasibleEstimate
        outputs+=s.id->estimate
        val profile=s.profiles.get(node.id)
        // This mirrors the worker's whole-partition fusion boundary. Partial regions are
        // not advertised as shared processes when the worker cannot launch them that way.
        val peers=steps.filter(x=>placements.get(x.id).contains(node.id))
        val compute=peers.filterNot(_.operator=="item_source@1.0")
        val key=profile.flatMap(_.fusionKey)
        val canFuse=partitionAcyclic(steps,placements)&&peers.forall(x=> !x.fusionBoundary)&&compute.size>1&&compute.forall(x=> !x.fusionBoundary&&x.profiles.get(node.id).flatMap(_.fusionKey)==key)&&key.nonEmpty&&
          peers.filter(_.operator=="item_source@1.0").forall(r=>steps.exists(_.dependencies.contains(r.id)))
        val group=if(canFuse)Some(node.id+":fusion")else None
        val uid=group.getOrElse(node.id+":"+s.id)
        val previous=units.find(_.id==uid)
        units=units.filterNot(_.id==uid):+PlannedUnit(uid,node.id,previous.map(_.steps).getOrElse(Vector.empty):+s.id,profile.map(_.runtime).getOrElse("unknown"),if(canFuse)"fusion_candidate"else if(s.operator=="item_source@1.0")"host_stream_source"else"isolated_step")
        val ready=s.dependencies.map { from=>
          val origin=targets.find(_.id==placements(from)).get
          val same=origin.id==node.id
          val shared=canFuse&&units.exists(u=>u.id==uid&&u.steps.contains(from))
          val link=plugin.flatMap(_.link(input,origin,node,settings)).getOrElse(if(same)LinkEstimate(1000000000,0.1)else LinkEstimate())
          require(finite(link.bytesPerSecond)&&link.bytesPerSecond>0&&finite(link.latencyMs)&&link.latencyMs>=0,"invalid link estimate")
          val duration=if(shared)0d else outputs(from).outputBytes/link.bytesPerSecond*1000+link.latencyMs
          edges :+= PlannedEdge(from,s.id,origin.id,node.id,outputs(from).outputBytes,duration,if(shared)"arrow_in_process_candidate"else if(same)"arrow_ipc"else"arrow_flight")
          // Pipelined edges overlap producer computation and transport instead of summing all stages.
          val start=finish(from)-outputs(from).computeMs
          math.max(finish(from),start+duration)
        }.foldLeft(node.queueMs)(math.max)
        finish+=s.id->(ready+estimate.computeMs+(if(previous.nonEmpty)0 else estimate.startupMs))
      }
      val processes=units.count(_.mode!="host_stream_source")
      val estimate=PlanEstimate(placements,finish.values.foldLeft(0d)(math.max),edges.filter(e=>e.sourceNode!=e.targetNode).map(_.bytes).sum,processes,outputs,edges,units)
      require(finite(estimate.completionMs)&&finite(estimate.transferBytes),"plan estimate overflow")
      estimate
    }
    def scored(p:PlanEstimate):(Double,Double,Int,String)={val score=plugin.map(_.score(p,settings)).getOrElse(p.completionMs);require(finite(score)&&score>=0,"optimizer score must be finite and nonnegative");(score,p.transferBytes,p.processes,p.placements.toVector.sorted.mkString)}
    var beam=Vector(Map.empty[String,String]);var evaluated=0
    steps.foreach { s=>
      check()
      val next=beam.flatMap { assigned=>
        targets.filter(t=>eligible(s,t,assigned.values.count(_==t.id))).flatMap { t=>
          val candidate=assigned+(s.id->t.id)
          try {val estimate=evaluate(candidate);evaluated+=1;Some(estimate)}catch{case e:InfeasibleEstimate=>None}
        }
      }
      require(next.nonEmpty,"no eligible execution target for step: "+s.id)
      beam=next.sortBy(scored).take(input.beamWidth).map(_.placements)
    }
    val custom=plugin.map(_.candidates(input,settings)).getOrElse(Vector.empty)
    require(custom.size<=256,"optimizer supplied too many candidates")
    custom.foreach(c=>require(c.keySet==seen,"custom candidate must place every step exactly once"))
    val complete=(beam++custom).distinct.map(evaluate);val best=complete.minBy(scored)
    val placements=steps.map{s=>s.id->StepPlacement(best.placements(s.id),if(s.requested.nonEmpty)"explicit_placement"else if(s.source.nonEmpty)"data_locality"else"lowest_estimated_cost")}.toMap
    PlanningResult(placements,best,evaluated+custom.size,scored(best)._1)
  }
  private def partitionAcyclic(steps:Vector[PlanningStep],placements:Map[String,String]):Boolean={
    val nodes=placements.values.toSet
    val edges=steps.filter(s=>placements.contains(s.id)).flatMap(s=>s.dependencies.filter(placements.contains).map(d=>placements(d)->placements(s.id))).filter(p=>p._1!=p._2).toSet
    var remaining=nodes
    while(remaining.nonEmpty){val roots=remaining.filter(n=> !edges.exists(e=>e._2==n&&remaining(e._1)));if(roots.isEmpty)return false;remaining--=roots};true
  }
  private class InfeasibleEstimate extends IllegalArgumentException("estimated member memory exceeded")
  private def finite(v:Double)=java.lang.Double.isFinite(v)
  private def eligible(s:PlanningStep,t:PlanningTarget,used:Int):Boolean=
    t.capacity>used&&s.source.forall(_._1==t.id)&&s.requested.forall(_==t.id)&&s.profiles.get(t.id).forall(p=>p.executor.isEmpty||t.executors.isEmpty||t.executors(p.executor))
}
