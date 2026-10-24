package dacp

import dacp.union.Json
import com.fasterxml.jackson.databind.JsonNode
import java.lang.management.ManagementFactory

/** JVM-supported environment metrics; unsupported readings are absent, never invented as zero. */
object NodeResources {
  def snapshot():JsonNode={
    val n=Json.obj().put("observed_at_ms",Json.now).put("processors",Runtime.getRuntime.availableProcessors())
    ManagementFactory.getOperatingSystemMXBean match {
      case os:com.sun.management.OperatingSystemMXBean=>
        def fraction(key:String,value:Double):Unit=if(java.lang.Double.isFinite(value) && value>=0 && value<=1)n.put(key,value)
        fraction("cpu_load",os.getCpuLoad);fraction("process_cpu_load",os.getProcessCpuLoad)
        val total=os.getTotalMemorySize;val free=os.getFreeMemorySize
        if(total>0 && free>=0 && free<=total)n.put("memory_total_bytes",total).put("memory_used_bytes",total-free)
      case _=>()
    }
    val heap=ManagementFactory.getMemoryMXBean.getHeapMemoryUsage
    n.put("heap_used_bytes",heap.getUsed).put("heap_committed_bytes",heap.getCommitted)
    if(heap.getMax>0)n.put("heap_max_bytes",heap.getMax)
    n.put("threads",ManagementFactory.getThreadMXBean.getThreadCount);n
  }
}
