package dacp

import org.apache.arrow.flight.FlightStatusCode._

import dacp.union.{Json,UnionFailure}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._

case class NodeUnionSettings(state:Path,executionTimeoutSeconds:Long=1800,retentionSeconds:Long=300) {
  require(executionTimeoutSeconds>0 && retentionSeconds>0)
}
object NodeUnionSettings {
  def read(n:JsonNode):NodeUnionSettings={
    Json.fields(n,Set("state_directory"),Set("execution_timeout_seconds","retention_seconds"))
    NodeUnionSettings(Path.of(Json.text(n,"state_directory")),
      if(n.has("execution_timeout_seconds"))Json.number(n,"execution_timeout_seconds",1,86400)else 1800,
      if(n.has("retention_seconds"))Json.number(n,"retention_seconds",1,86400)else 300)
  }
}
final class UnionMembership(val directory:Path) {
  Files.createDirectories(directory)
  private def file(id:String)=directory.resolve(Json.id(id)+".json")
  def get(id:String):JsonNode={val p=file(id);if(!Files.exists(p))throw UnionFailure(NOT_FOUND,"union invitation not found");Json.read(p)}
  def list():Vector[JsonNode]={val in=Files.list(directory);try in.iterator.asScala.filter(_.getFileName.toString.matches("[A-Za-z0-9_-]+\\.json")).map(Json.read).toVector finally in.close()}
  def invite(id:String,identity:String):JsonNode=synchronized { Json.locked(directory) {
    val previous=if(Files.exists(file(id)))Some(get(id))else None
    if(previous.isEmpty && list().size>=128)throw UnionFailure(RESOURCE_EXHAUSTED,"invitation capacity reached")
    previous.foreach(p=>if(p.path("scheduler_identity").asText!=identity)throw UnionFailure(UNAUTHORIZED,"union identity differs"))
    if(previous.exists(p=>Set("pending","approved")(p.path("state").asText)))previous.get
    else {val n=Json.obj().put("union_id",Json.id(id)).put("scheduler_identity",Json.identity(identity)).put("state","pending").put("invited_at_ms",Json.now);Json.write(file(id),n);n}
  }}
  def member(id:String,identity:String,approved:Boolean=true):JsonNode={
    val n=get(id)
    if(n.path("scheduler_identity").asText!=identity)throw UnionFailure(UNAUTHORIZED,"scheduler identity denied")
    if(approved && (n.path("state").asText!="approved" || n.path("expires_at_ms").asLong<=Json.now))throw UnionFailure(UNAUTHORIZED,"union is not approved or has expired")
    n
  }
  def approve(id:String,policy:JsonNode):JsonNode=synchronized { Json.locked(directory) {
    Json.fields(policy,Set("scheduler_identity","input_prefixes","operators","max_concurrent_tasks","expires_at_ms"))
    val n=get(id).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    require(n.path("state").asText=="pending","invitation must be pending")
    require(Json.identity(Json.text(policy,"scheduler_identity"))==n.path("scheduler_identity").asText,"scheduler identity mismatch")
    Json.strings(policy,"input_prefixes").foreach(p=>DacpUri.parse(p,false))
    val operators=Json.strings(policy,"operators");require(operators.nonEmpty && operators.forall(_.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*")),"explicit operator references required")
    Json.number(policy,"max_concurrent_tasks",1,64);Json.number(policy,"expires_at_ms",Json.now+1,Long.MaxValue)
    policy.fields.asScala.foreach(e=>n.set[JsonNode](e.getKey,e.getValue))
    n.put("state","approved");Json.write(file(id),n);n
  }}
  def revoke(id:String,state:String="revoked"):JsonNode=synchronized { Json.locked(directory) {
    require(Set("revoked","rejected")(state));val n=get(id).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();n.put("state",state);Json.write(file(id),n);n
  }}
}
object UnionNodeAdmin {
  def main(args:Array[String]):Unit={
    require(args.length>=2,"union list STATE | union approve STATE UNION_ID POLICY.yaml | union reject STATE UNION_ID | union leave STATE UNION_ID")
    val store=new UnionMembership(Path.of(args(1)))
    val result=args(0) match {
      case "list"=>require(args.length==2);Json.mapper.valueToTree[JsonNode](store.list().asJava)
      case "approve"=>require(args.length==4);store.approve(args(2),Json.yaml(Path.of(args(3))))
      case "reject"=>require(args.length==3);store.revoke(args(2),"rejected")
      case "leave"=>require(args.length==3);store.revoke(args(2))
      case _=>throw new IllegalArgumentException("unknown union administration command")
    }
    println(result.toPrettyString)
  }
}
