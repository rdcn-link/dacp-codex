package dacp.client

import org.scalatest.funsuite.AnyFunSuite
import dacp.union.Json

class WorkflowVariablesSpec extends AnyFunSuite {
  test("typed nested substitution is nonrecursive and preserves literal text and keys") {
    val vars=Json.mapper.readTree("""{"N":123,"B":true,"O":{"x":2},"A":[1,2],"NULL":null,"TEXT":"$N"}""")
    val result=Json.mapper.readTree(WorkflowVariables.resolve(
      """params: {n: $N, nested: [$B, $O, $A, $NULL], text: $TEXT, literal: "x$N", "$N": key}""",vars))
    assert(result.path("params").path("n").isInt)
    assert(result.path("params").path("nested").get(0).asBoolean)
    assert(result.path("params").path("nested").get(1).path("x").asInt==2)
    assert(result.path("params").path("nested").get(2).isArray)
    assert(result.path("params").path("nested").get(3).isNull)
    assert(result.path("params").path("text").asText=="$N")
    assert(result.path("params").path("literal").asText=="x$N")
    assert(result.path("params").path("$N").asText=="key")
  }
  test("missing variables and invalid names fail before submission") {
    intercept[IllegalArgumentException](WorkflowVariables.resolve("params: {n: $N}",Json.obj()))
    intercept[IllegalArgumentException](WorkflowVariables.resolve("params: {}",Json.obj().put("bad-name",1)))
  }
  test("omitting variables preserves original text") {
    assert(WorkflowVariables.resolve("params: {n: $N}",null)=="params: {n: $N}")
  }
}
