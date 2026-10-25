package dacp

import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class WorkflowInferenceSpec extends AnyFunSuite {
  val bytes=StreamDescriptor(Set("bytes"),Set("json"),certainty="known")
  def plan=Jobs.parse("""job: infer
    |tasks:
    |- {id: source, operator: source@1, inputs: {}}
    |- {id: copy, operator: copy@1, inputs: {source: source}}
    |- {id: sink, operator: sink@1, inputs: {source: copy}}
    |outputs: {}
    |""".stripMargin)
  test("metadata propagates along default ports and sinks return empty output sets") {
    var called=Vector.empty[String]
    val report=WorkflowInference.propagate(plan){(task,inputs)=>
      called :+= task.id
      if(task.id!="source")assert(inputs("source")==bytes)else assert(inputs.isEmpty)
      InferenceResult(if(task.id=="sink")Map.empty else Map("result"->bytes))
    }
    assert(called==Vector("source","copy","sink"))
    assert(report.path("tasks").path("sink").path("outputs").isEmpty)
  }
  test("missing ports and error diagnostics fail before downstream scheduling") {
    intercept[IllegalArgumentException](WorkflowInference.propagate(plan){(_,_)=>InferenceResult(Map.empty)})
    intercept[IllegalArgumentException](WorkflowInference.propagate(plan){(_,_)=>InferenceResult(Map.empty,Vector(InferenceDiagnostic("error","bad","bad schema")))})
  }
  test("unknown schemas propagate and do not imply a false known schema") {
    val report=WorkflowInference.propagate(plan){(task,_)=>InferenceResult(if(task.id=="sink")Map.empty else Map("result"->StreamDescriptor(Set("records"))))}
    assert(report.path("tasks").path("source").path("outputs").path("result").path("certainty").asText=="unknown")
  }
  test("input representation, format and known schema incompatibilities are rejected") {
    val op=ResolvedOperator("image",None,None,OperatorCommand(),Map("source"->PortContract(Set("records"),Set.empty,None)),Map.empty)
    intercept[IllegalArgumentException](OperatorInference.validateInputs(op,Map("source"->bytes)))
    OperatorInference.validateInputs(op,Map("source"->StreamDescriptor(Set("records"))))
    intercept[IllegalArgumentException](OperatorInference.validateInputs(op,Map.empty))
  }
}
