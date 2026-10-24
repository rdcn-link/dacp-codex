package dacp.unionserver

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json
import scala.jdk.CollectionConverters._

/** Count receiver-side payload once, from each job's latest attempt; never infer network traffic. */
private[unionserver] object NodeTopology {
  def build(members:Vector[JsonNode],jobs:Vector[JsonNode],at:Long,staleAfter:Long):JsonNode={
    val n=Json.obj().put("at_ms",at).put("job_count",jobs.size).put("job_limit",100).put("scope","recent_jobs_latest_attempt").put("stale_after_ms",staleAfter)
    val ms=n.putArray("members");members.foreach(ms.add)
    val known=members.map(_.path("identity").asText).toSet
    val edges=scala.collection.mutable.LinkedHashMap.empty[(String,String),com.fasterxml.jackson.databind.node.ObjectNode]
    jobs.foreach { job=>
      val tasks=job.path("tasks").elements.asScala.map(t=>t.path("id").asText->t).toMap
      tasks.values.foreach { t=>t.path("inputs").elements.asScala.foreach { input=>
        tasks.get(input.path("task").asText).foreach { parent=>
          val from=parent.path("node_identity").asText;val to=t.path("node_identity").asText
          if(from!=to && known(from) && known(to)){
            val edge=edges.getOrElseUpdate(from->to,{
              val e=Json.obj().put("source",from).put("target",to).put("payload_bytes",0L).put("observed",false)
              e.putArray("samples");e
            })
            val node=job.path("nodes").path(to)
            val metric=node.path("ports").path(t.path("id").asText+"/inputs/"+input.path("port").asText)
            if(metric.has("remote_payload_bytes")){
              val bytes=metric.path("remote_payload_bytes").asLong
              edge.put("payload_bytes",edge.path("payload_bytes").asLong+bytes).put("observed",true)
              edge.withArray("samples").addObject().put("key",job.path("job_id").asText+"/"+job.path("attempt_id").asText+"/"+t.path("id").asText+"/"+input.path("port").asText)
                .put("bytes",bytes).put("observed_at_ms",node.path("observed_at_ms").asLong)
                .put("active",!Set("succeeded","failed","cancelled","expired","interrupted")(job.path("state").asText))
            }
          }
        }
      }}
    }
    val es=n.putArray("edges");edges.values.foreach(es.add);n
  }
}
