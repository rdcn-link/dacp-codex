package dacp

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import java.util.concurrent.atomic.AtomicLong
import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json
/** Payload counters count each input/output independently, excluding framing/TLS overhead. */
private[dacp] final class UnionPortMetrics(allocator:BufferAllocator) {
  private val bytes=new AtomicLong();private val remoteBytes=new AtomicLong();private val rows=new AtomicLong()
  @volatile private var records=false
  private val last=new AtomicLong()
  def observe(f:OperatorFrame,remote:Boolean):Unit={
    val h=StreamProtocol.header(f);last.set(Json.now)
    if(h.path("type").asText=="start")records=h.path("representation").asText=="records"
    if(h.path("type").asText=="data") {
      bytes.addAndGet(f.data.length);if(remote)remoteBytes.addAndGet(f.data.length)
      if(records){val r=new ArrowStreamReader(new java.io.ByteArrayInputStream(f.data),allocator);try {require(r.loadNextBatch(),"missing records");rows.addAndGet(r.getVectorSchemaRoot.getRowCount)}finally r.close()}
    }
  }
  def summary(h:JsonNode,remote:Boolean=false):Unit={
    last.set(Json.now)
    if(h.path("phase").asText=="start")records=h.path("representation").asText=="records"
    val count=h.path("payload_bytes").asLong;require(count>=0,"invalid group metric")
    bytes.addAndGet(count);if(remote)remoteBytes.addAndGet(count)
    if(h.has("records"))rows.addAndGet(h.path("records").asLong)
  }
  def snapshot:JsonNode={val n=Json.obj().put("payload_bytes",bytes.get).put("remote_payload_bytes",remoteBytes.get).put("last_activity_ms",last.get);if(records)n.put("records",rows.get);n}
}
