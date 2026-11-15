package dacp



import org.apache.arrow.flight.FlightStatusCode._

import dacp.union.{Json,UnionFailure}

import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._

case class NodeUnionSettings(state:Path,executionTimeoutSeconds:Long=1800,retentionSeconds:Long=300,preparationTimeoutSeconds:Long=1800) {
  require(executionTimeoutSeconds>0 && retentionSeconds>0 && preparationTimeoutSeconds>0)
}
object NodeUnionSettings {
  def read(n:JsonNode):NodeUnionSettings={
    Json.fields(n,Set("state_directory"),Set("execution_timeout_seconds","retention_seconds","preparation_timeout_seconds"))
    NodeUnionSettings(Path.of(Json.text(n,"state_directory")),
      if(n.has("execution_timeout_seconds"))Json.number(n,"execution_timeout_seconds",1,86400)else 1800,
      if(n.has("retention_seconds"))Json.number(n,"retention_seconds",1,86400)else 300,
      if(n.has("preparation_timeout_seconds"))Json.number(n,"preparation_timeout_seconds",1,86400)else 1800)
  }
}
/** Node-issued service credentials replace invitations, approvals and per-resource grants. */
final class UnionMembership(credentials:dacp.union.Credentials,capacity:Int) {
  def member(id:String,identity:String):JsonNode={
    val c=credentials.union(Json.id(id),Json.identity(identity)).getOrElse(throw UnionFailure(UNAUTHORIZED,"Union service credential revoked, expired or not configured"))
    Json.obj().put("union_id",id).put("scheduler_identity",identity).put("state","approved")
      .put("max_concurrent_tasks",capacity).put("expires_at_ms",if(c.expires==0)Long.MaxValue else c.expires)
  }
}
