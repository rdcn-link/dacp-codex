package dacp.unionserver

import _root_.xstream.data._


import dacp.union.Json
import org.scalatest.funsuite.AnyFunSuite

class NodeTopologySpec extends AnyFunSuite {
  val members=Vector(Json.obj().put("identity","a"),Json.obj().put("identity","b"))
  def job=Json.parse("""{"job_id":"j","attempt_id":"try2","state":"running","tasks":[{"id":"send","node_identity":"a","inputs":[]},{"id":"receive","node_identity":"b","inputs":[{"port":"source","task":"send","output_port":"result"}]}],"nodes":{"a":{"ports":{"send/outputs/result":{"remote_payload_bytes":100}}},"b":{"observed_at_ms":900,"ports":{"receive/inputs/source":{"remote_payload_bytes":40}}}}}""")
  test("receiver payload is counted once and samples are separated by attempt") {
    val result=NodeTopology.build(members,Vector(job),1000,5000)
    val edge=result.path("edges").get(0)
    assert(edge.path("source").asText=="a" && edge.path("target").asText=="b")
    assert(edge.path("payload_bytes").asLong==40 && edge.path("observed").asBoolean)
    val sample=edge.path("samples").get(0)
    assert(sample.path("key").asText=="j/try2/receive/source" && sample.path("observed_at_ms").asLong==900 && sample.path("active").asBoolean)
  }
  test("missing counters are unknown and same-node or removed-member edges are excluded") {
    val missing=job;missing.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].remove("nodes")
    val edge=NodeTopology.build(members,Vector(missing),1000,5000).path("edges").get(0)
    assert(!edge.path("observed").asBoolean && edge.path("samples").isEmpty)
    assert(NodeTopology.build(members.take(1),Vector(job),1000,5000).path("edges").isEmpty)
    val local=job;local.path("tasks").get(1).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("node_identity","a")
    assert(NodeTopology.build(members,Vector(local),1000,5000).path("edges").isEmpty)
  }
}
