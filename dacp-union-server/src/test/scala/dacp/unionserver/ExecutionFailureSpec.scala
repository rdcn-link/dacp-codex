package dacp.unionserver

import dacp.union.{ExecutionFailure,Json}
import org.scalatest.funsuite.AnyFunSuite

class ExecutionFailureSpec extends AnyFunSuite {
  test("missing operators are diagnosed without exposing arbitrary exception messages") {
    val missing=new IllegalArgumentException("requirement failed: operator not found locally or in configured libraries: emergency_terrain@1.0")
    val f=ExecutionFailure.summary(new RuntimeException("private token=secret /srv/private",missing),"preparation","request","execution")
    assert(f.path("code").asText=="OPERATOR_NOT_FOUND")
    assert(f.path("operator").asText=="emergency_terrain@1.0")
    assert(!f.toString.contains("secret") && !f.toString.contains("/srv"))
    val unknown=ExecutionFailure.summary(new Exception("password=secret"),"execution","r","e")
    assert(!unknown.toString.contains("secret"))
    assert(!ExecutionFailure.publicCopy(Json.obj().put("stack","secret").put("message","safe")).has("stack"))
  }
}
