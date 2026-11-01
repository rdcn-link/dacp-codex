package dacp.unionserver

final case class PlanningTarget(id:String,capacity:Int) {
  require(id.nonEmpty && capacity>=0,"invalid planning target")
}
final case class PlanningStep(id:String,dependencies:Vector[String],source:Option[(String,Option[Long])]=None,
  requested:Option[String]=None)
final case class StepPlacement(target:String,reason:String)
/** Union graph placement after resolving member namespaces and credentials. */
object GraphPlacement {
  def plan(steps:Vector[PlanningStep],targets:Vector[PlanningTarget]):Map[String,StepPlacement]={
    require(steps.map(_.id).distinct.size==steps.size && targets.map(_.id).distinct.size==targets.size,"duplicate planning identity")
    val assigned=scala.collection.mutable.Map.empty[String,StepPlacement]
    val used=scala.collection.mutable.Map.empty[String,Int].withDefaultValue(0)
    val sources=steps.flatMap(s=>s.source.map(s.id->_)).toMap
    steps.foreach { step=>
      require(step.dependencies.forall(assigned.contains),"planning steps must be in dependency order")
      val preferred=(step.source.map(_._1).toVector++step.dependencies.map(assigned(_).target)).distinct
      val eligible=targets.filter(t=>t.capacity>used(t.id) && step.source.forall(_._1==t.id) && step.requested.forall(_==t.id))
      require(eligible.nonEmpty,"no eligible execution target for step: "+step.id)
      val nearest=eligible.filter(t=>preferred.contains(t.id))
      val pool=if(nearest.nonEmpty)nearest else eligible
      val known=step.dependencies.flatMap(sources.get)
      val complete=known.nonEmpty && known.size==step.dependencies.size && known.forall(_._2.nonEmpty)
      val candidates=pool.map { target=>
        val transfer=if(complete)known.filter(_._1!=target.id).map(s=>BigInt(s._2.get)).sum else BigInt(if(preferred.contains(target.id))0 else 1)
        target.id->transfer
      }
      val target=candidates.minBy(c=>(c._2,c._1))._1
      val reason=if(step.requested.nonEmpty)"explicit_placement"else if(step.source.nonEmpty)"data_locality"else if(complete)"minimize_transfer"else if(preferred.contains(target))"upstream_locality"else"available_capacity"
      assigned(step.id)=StepPlacement(target,reason);used(target)+=1
    }
    assigned.toMap
  }
}
