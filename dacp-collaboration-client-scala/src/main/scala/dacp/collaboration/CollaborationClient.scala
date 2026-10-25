package dacp.collaboration

import dacp.union._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.flight.FlightClient

/** Separate service SDK artifact; every operation is still authorized by the receiving server. */
final class CollaborationClient(tls:Tls,traffic:Option[FlightTraffic]=None,credential:String=>Option[String]=_=>None) extends AutoCloseable {
  private val transport=new Remote(Some(tls),traffic,credential)
  def dataClient(origin:String,pin:Option[String]):FlightClient=transport.dataClient(origin,pin)
  def peer(origin:String,pin:Option[String]=None):String=transport.peer(origin,pin)
  def catalog(origin:String,pin:Option[String]=None,timeout:Long=5,hops:Vector[String]=Vector.empty)=transport.catalog(origin,pin,timeout,hops)
  def action(origin:String,name:String,body:JsonNode=Json.obj(),pin:Option[String]=None,timeout:Long=30,hops:Vector[String]=Vector.empty):JsonNode=
    transport.action(origin,name,body,pin,timeout,hops)
  def stream(origin:String,kind:String,body:JsonNode,pin:Option[String]=None,timeout:Long=1800,hops:Vector[String]=Vector.empty):BinarySource=
    transport.stream(origin,kind,body,pin,timeout,hops)
  def membership(origin:String,unionId:String,pin:Option[String]=None):JsonNode=action(origin,"membership",Json.obj().put("union_id",unionId),pin)
  def nodeStatus(origin:String,unionId:String,pin:Option[String]=None):JsonNode=action(origin,"node_status",Json.obj().put("union_id",unionId),pin)
  def prepare(origin:String,plan:JsonNode,pin:Option[String]=None):JsonNode=action(origin,"prepare",plan,pin)
  def start(origin:String,executionId:String,pin:Option[String]=None):JsonNode=action(origin,"start",Json.obj().put("execution_id",executionId),pin)
  def status(origin:String,executionId:String,pin:Option[String]=None):JsonNode=action(origin,"execution_status",Json.obj().put("execution_id",executionId),pin)
  def cancel(origin:String,executionId:String,pin:Option[String]=None):JsonNode=action(origin,"cancel_execution",Json.obj().put("execution_id",executionId),pin)
  def close():Unit=transport.close()
}
