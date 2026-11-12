package example

import dacp.union._

/** A domain policy can also override estimate/link/candidates without changing core code. */
final class TransferAwareOptimizer extends PhysicalPlanOptimizer {
  val id="transfer-aware"
  private def penalty(settings:Map[String,Object]):Double=settings.get("transfer_penalty_ms_per_mb").map { raw=>
    require(raw.isInstanceOf[java.lang.Number],"transfer penalty must be numeric")
    raw.asInstanceOf[java.lang.Number].doubleValue
  }.getOrElse(10d)
  override def validate(settings:Map[String,Object]):Unit={
    require(settings.keySet.subsetOf(Set("transfer_penalty_ms_per_mb")),"unknown transfer-aware setting")
    require(java.lang.Double.isFinite(penalty(settings))&&penalty(settings)>=0,"invalid transfer penalty")
  }
  override def score(plan:PlanEstimate,settings:Map[String,Object]):Double=
    plan.completionMs+plan.transferBytes/1000000d*penalty(settings)
}
