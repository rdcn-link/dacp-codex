package dacp.union

import io.grpc.ClientStreamTracer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import scala.jdk.CollectionConverters._

/** Actual uncompressed gRPC message sizes; excludes transport framing, headers and TLS. */
final class FlightTraffic {
  private val counters=new ConcurrentHashMap[(String,String,String),LongAdder]()
  private val started=Json.now
  def factory(origin:String,method:String):ClientStreamTracer.Factory=new ClientStreamTracer.Factory {
    override def newClientStreamTracer(info:ClientStreamTracer.StreamInfo,headers:io.grpc.Metadata):ClientStreamTracer={
      val kind=if(method.endsWith("/DoGet") || method.endsWith("/DoExchange"))"stream"else "control"
      def add(direction:String,bytes:Long):Unit=if(bytes>=0)counters.computeIfAbsent((origin,kind,direction),_=>new LongAdder()).add(bytes)
      new ClientStreamTracer {
        override def outboundUncompressedSize(bytes:Long):Unit=add("sent",bytes)
        override def inboundUncompressedSize(bytes:Long):Unit=add("received",bytes)
      }
    }
  }
  def snapshot:com.fasterxml.jackson.databind.JsonNode={
    val n=Json.obj().put("started_at_ms",started).put("observed_at_ms",Json.now)
    val rows=n.putArray("counters")
    counters.asScala.foreach{case((origin,kind,direction),v)=>rows.addObject().put("origin",origin).put("kind",kind).put("direction",direction).put("bytes",v.sum())};n
  }
}
