package dacp.unionserver

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json

/** Actual received task data payload, excluding metadata and control frames. */
private[unionserver] final class ResultTraffic {
  private case class Counter(source:String,attempt:String,step:String,port:String,var bytes:Long,var at:Long)
  private val counters=scala.collection.mutable.LinkedHashMap.empty[(String,String,String,String),Counter]
  def record(source:String,attempt:String,step:String,port:String,bytes:Long,at:Long):Unit=synchronized {
    require(bytes>=0,"negative result payload")
    val c=counters.getOrElseUpdate((source,attempt,step,port),Counter(source,attempt,step,port,0L,at))
    c.bytes+=bytes;c.at=at
  }
  def snapshot:JsonNode=synchronized {
    val rows=Json.mapper.createArrayNode()
    counters.values.foreach(c=>rows.addObject().put("source",c.source).put("attempt_id",c.attempt).put("step",c.step).put("port",c.port).put("payload_bytes",c.bytes).put("observed_at_ms",c.at))
    rows
  }
}
