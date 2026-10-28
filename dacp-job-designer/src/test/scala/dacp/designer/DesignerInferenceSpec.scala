package dacp.designer

import dacp._
import dacp.client.WorkflowVariables
import dacp.union.Json
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

class DesignerInferenceSpec extends AnyFunSuite {
  test("source view defaults to metadata and source samples are read only on explicit preview requests") {
    var reads=0
    val schema=org.apache.arrow.vector.types.pojo.Schema.fromJSON("""{"fields":[{"name":"temperature","nullable":true,"type":{"name":"floatingpoint","precision":"DOUBLE"},"children":[]}]}""")
    val inference=new DesignerInference((_,request,_)=>{
      assert(request.preview.nonEmpty && request.inputs("source").sample.nonEmpty)
      throw new IllegalArgumentException("input ports do not match operator declaration; missing: left, right")
    },(_,view,_)=>{
      assert(view=="auto");StreamDescriptor(Set("records"),Set("arrow"),Some(schema),"known")
    },(_,view,limits,_)=>{
      reads+=1;assert(view=="records")
      Some(OutputSample("records","json",Vector.fill(8)(Json.obj().put("temperature",20.5))).bounded(limits))
    })
    val source="name: Preview\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'dacp://union.example/ds-weather/benchmark'}\noutputs: {result: read}\n"
    val cancel=new StreamCancel()
    try {
      val plain=inference.infer(Documents.job(source),cancel)
      assert(reads==0 && plain.path("tasks").path("read").path("outputs").path("result").has("schema"))
      val preview=inference.infer(Documents.job(source),cancel,Some(PreviewLimits(2,256)))
      assert(reads==1 && preview.path("outputs").path("result").path("sample").path("rows").size==2)
      val broken=source.replace("outputs: {result: read}","- id: broken\n  operator: join@1\n  inputs: {source: read}\noutputs: {result: broken}")
      val partial=inference.infer(Documents.job(broken),cancel,Some(PreviewLimits(2,256)))
      assert(partial.path("tasks").path("read").path("outputs").path("result").has("schema"))
      assert(partial.path("tasks").path("broken").path("diagnostics").get(0).path("message").asText.contains("missing: left, right"))
    }finally cancel.close()
  }
  test("editing uses local operator inference regardless of placement; only sources request metadata") {
    val calls=ArrayBuffer.empty[String]
    val inference=new DesignerInference((reference,request,_)=>{
      calls+=reference
      assert(request.inputs("source").formats==Set("csv"))
      InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set(request.params.path("format").asText),certainty="known")))
    },(uri,view,_)=>{calls+=uri;assert(view=="bytes");StreamDescriptor(Set("bytes"),Set("csv"),certainty="known")})
    val yaml="""name: Local preview
      |tasks:
      |- id: read
      |  operator: read_item@1.0
      |  inputs: {}
      |  params: {uri: 'dacp://union.example/ds-weather/observations', view: bytes}
      |- id: convert
      |  node: nd-missing-node
      |  operator: convert@1
      |  inputs: {source: read:result}
      |  params: {format: $FORMAT}
      |outputs: {result: convert}
      |""".stripMargin
    val cancel=new StreamCancel()
    try {
      Seq("json","xml").foreach { format=>
        val job=Documents.job(WorkflowVariables.resolve(yaml,Json.obj().put("FORMAT",format)))
        val result=inference.infer(job,cancel)
        assert(result.path("outputs").path("result").path("formats").get(0).asText==format)
      }
      assert(calls.toVector==Vector("dacp://union.example/ds-weather/observations","convert@1","dacp://union.example/ds-weather/observations","convert@1"))
    }finally cancel.close()
  }
  test("source-free generators do not contact any node and cancellation stops local inference") {
    var calls=0
    val inference=new DesignerInference((_,request,_)=>{calls+=1;assert(request.inputs.isEmpty);InferenceResult(Map.empty)},(_,_,_)=>throw new AssertionError("no metadata request expected"))
    val job=Documents.job("name: Generator\ntasks:\n- id: generator\n  operator: generate@1\n  inputs: {}\noutputs: {}\n")
    val cancel=new StreamCancel()
    inference.infer(job,cancel);assert(calls==1);cancel.close()
    intercept[java.util.concurrent.CancellationException](inference.infer(job,cancel));assert(calls==1)
  }
}
