package dacp.union

import xstream.data.Plugin

/** Immutable extension contracts. Estimates are advisory; placement validation belongs to Union. */
final case class PlanningTarget(id:String,capacity:Int,executors:Set[String]=Set.empty,freeMemory:Option[Long]=None,queueMs:Double=0)
final case class PlanningProfile(executor:String="",runtime:String="",fusionKey:Option[String]=None)
final case class PlanningStep(id:String,dependencies:Vector[String],source:Option[(String,Option[Long])]=None,
  requested:Option[String]=None,operator:String="",parameters:String="{}",profiles:Map[String,PlanningProfile]=Map.empty,
  fusionBoundary:Boolean=false)
final case class PlanningInput(steps:Vector[PlanningStep],targets:Vector[PlanningTarget],beamWidth:Int=32)
final case class StepEstimate(outputBytes:Double,computeMs:Double=100,startupMs:Double=500,memoryBytes:Long=0,confidence:String="estimated")
final case class LinkEstimate(bytesPerSecond:Double=100000000,latencyMs:Double=2)
final case class PlannedEdge(from:String,to:String,sourceNode:String,targetNode:String,bytes:Double,transferMs:Double,mode:String)
final case class PlannedUnit(id:String,node:String,steps:Vector[String],runtime:String,mode:String)
final case class PlanEstimate(placements:Map[String,String],completionMs:Double,transferBytes:Double,processes:Int,
  steps:Map[String,StepEstimate],edges:Vector[PlannedEdge],units:Vector[PlannedUnit])

/** Loaded through PluginEnvironment and META-INF/services/dacp.union.PhysicalPlanOptimizer.
  * Implementations are administrator-installed trusted code, never Job-supplied code.
  */
trait PhysicalPlanOptimizer extends Plugin {
  def validate(settings:Map[String,Object]):Unit=require(settings.isEmpty,"optimizer does not accept settings")
  def estimate(input:PlanningInput,step:PlanningStep,node:PlanningTarget,inputBytes:Double,settings:Map[String,Object]):Option[StepEstimate]=None
  def link(input:PlanningInput,from:PlanningTarget,to:PlanningTarget,settings:Map[String,Object]):Option[LinkEstimate]=None
  def candidates(input:PlanningInput,settings:Map[String,Object]):Vector[Map[String,String]]=Vector.empty
  def score(plan:PlanEstimate,settings:Map[String,Object]):Double=plan.completionMs
}
