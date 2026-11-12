package dacp

/** Stable business operation names for request access logs. */
object AccessOperations {
  def metadata(path:Seq[String]):String =
    if(path.size==4 && path(2)=="count")"item.count"
    else if(path.size==2 || (path.size==4 && path(2)=="list"))"item.list"
    else "item.describe"

  def action(name:String):String=Map(
    "dacp.capabilities"->"service.capabilities",
    "dacp.catalog"->"catalog.describe",
    "dacp.infer"->"step.infer",
    "dacp.infer_job"->"job.infer",
    "dacp.union.membership"->"union.membership.get",
    "dacp.union.node_status"->"node.status",
    "dacp.union.planning_profile"->"job.planning.profile",
    "dacp.union.materialized_status"->"step.materialization.status",
    "dacp.nodes"->"node.list",
    "dacp.union.submit"->"job.submit",
    "dacp.union.replay_job"->"job.replay",
    "dacp.union.job_status"->"job.status",
    "dacp.union.cancel_job"->"job.cancel",
    "dacp.union.prepare"->"job.prepare",
    "dacp.union.start"->"job.start",
    "dacp.union.execution_status"->"job.status",
    "dacp.union.cancel_execution"->"job.cancel"
  ).getOrElse(name,"request.unsupported")
}
