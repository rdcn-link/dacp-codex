package dacp.unionserver

import dacp.union.Json
import org.scalatest.funsuite.AnyFunSuite

class ResultTrafficSpec extends AnyFunSuite {
  test("actual payload ledger accumulates separately by producer and attempt") {
    val ledger=new ResultTraffic
    ledger.record("a","old","result","out",64,100)
    ledger.record("a","old","result","out",32,200)
    ledger.record("a","latest","result","out",10,300)
    val rows=ledger.snapshot
    assert(rows.size==2 && rows.get(0).path("payload_bytes").asLong==96)
    assert(rows.get(0).path("observed_at_ms").asLong==200)
    assertThrows[IllegalArgumentException](ledger.record("a","latest","result","out",-1,400))
  }
  test("union topology excludes previous attempts and removed members; missing is unknown") {
    val ledger=new ResultTraffic
    ledger.record("a","old","result","out",100,100)
    ledger.record("a","latest","result","out",40,900)
    ledger.record("removed","latest","result","out",200,900)
    val job=Json.obj().put("job_id","j").put("attempt_id","latest").put("state","succeeded")
    job.set[com.fasterxml.jackson.databind.JsonNode]("union_result_streams",ledger.snapshot)
    val members=Vector(Json.obj().put("identity","a"),Json.obj().put("identity","b"))
    val edges=NodeTopology.unionEdges(members,Vector(job),"u")
    assert(edges.size==2)
    val a=edges.get(0);assert(a.path("source").asText=="a" && a.path("target").asText=="u")
    assert(a.path("observed").asBoolean && a.path("payload_bytes").asLong==40)
    assert(!a.path("samples").get(0).path("active").asBoolean)
    assert(!edges.get(1).path("observed").asBoolean && edges.get(1).path("samples").isEmpty)
    val unobserved=Json.obj().put("job_id","unobserved").put("attempt_id","x")
    assert(!NodeTopology.unionEdges(members,Vector(unobserved),"u").get(0).path("observed").asBoolean)
  }
}
