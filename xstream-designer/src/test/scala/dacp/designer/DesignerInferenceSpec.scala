package xstream.designer


import dacp._
import dacp.client.WorkflowVariables
import dacp.union.Json
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

class DesignerInferenceSpec extends AnyFunSuite {
  test("service-relative paths resolve before query schema and bounded sample inference") {
    val seen=ArrayBuffer.empty[String]
    val schema=org.apache.arrow.vector.types.pojo.Schema.fromJSON("""{"fields":[{"name":"station","nullable":true,"type":{"name":"utf8"},"children":[]}]}""")
    val inference=new DesignerInference((_,_,_)=>throw new AssertionError("operator worker not needed"),
      (_,_,_)=>throw new AssertionError("query callback required"),queryItem=Some((uri,view,scan,_)=>{
        seen+=uri;assert(view=="records" && scan.columns.contains(Seq("station")))
        StreamDescriptor(Set("records"),Set("arrow"),Some(scan.bind(schema).outputSchema),"known")
      }),querySample=Some((_,_,_,limits,_)=>Some(OutputSample("records","json",Vector(Json.obj().put("station","A"))).bounded(limits))),origin=Some("dacp://union:19860"))
    val yaml="name: Query\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: context:/ds-example/observations, sql: 'SELECT station FROM source LIMIT 1'}\noutputs: {result: read}\n"
    val cancel=new StreamCancel()
    try {
      val result=inference.infer(Documents.job(yaml),cancel,Some(PreviewLimits(1,1024)))
      assert(seen.toVector==Vector("dacp://union:19860/ds-example/observations"))
      assert(result.path("tasks").path("read").path("outputs").path("result").path("sample").path("rows").size==1)
    }finally cancel.close()
  }
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
  test("editing uses local operator inference through the public service; only sources request metadata") {
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
  test("canvas edits only recompute changed steps and their descendants, with separate schema/sample phases") {
    val calls=ArrayBuffer.empty[String]
    val inference=new DesignerInference((ref,request,_)=>{
      calls+=ref+(if(request.preview.isDefined)":sample"else ":schema")
      InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),certainty="known")))
    },(_,_,_)=>throw new AssertionError("no remote Item"))
    val job=Json.mapper.readTree("""{"name":"Branches","tasks":[
      {"id":"a","operator":"a@1","inputs":{},"params":{"value":1}},
      {"id":"b","operator":"b@1","inputs":{"source":"a"},"params":{}},
      {"id":"c","operator":"c@1","inputs":{"source":"b"},"params":{}},
      {"id":"x","operator":"x@1","inputs":{},"params":{}}],"outputs":{"result":"c"}}""")
    val cancel=new StreamCancel()
    val session=Some("a"*32)
    def run(preview:Option[PreviewLimits]=None):Vector[String]={
      calls.clear();inference.infer(job,cancel,preview,editSession=session);calls.toVector
    }
    try {
      assert(run()==Vector("a@1:schema","b@1:schema","c@1:schema","x@1:schema"))
      assert(run().isEmpty)
      job.path("tasks").get(0).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("title","Renamed").put("node","other-node")
      job.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("name","Renamed job")
      assert(run().isEmpty)
      assert(run(Some(PreviewLimits(10,16384))).size==4)
      assert(run(Some(PreviewLimits(10,16384))).isEmpty)
      job.path("tasks").get(1).withObject("/params").put("threshold",2)
      assert(run()==Vector("b@1:schema","c@1:schema"))
      assert(run(Some(PreviewLimits(10,16384)))==Vector("b@1:sample","c@1:sample"))
      job.path("tasks").asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode].remove(2)
      job.withObject("/outputs").put("result","b")
      assert(run().isEmpty && run(Some(PreviewLimits(10,16384))).isEmpty)
      job.path("tasks").get(1).withObject("/inputs").put("source","x:result")
      assert(run()==Vector("b@1:schema"))
      calls.clear();inference.infer(job,cancel,editSession=Some("b"*32));assert(calls.size==3)
    }finally cancel.close()
  }
  test("resolved variables invalidate only consumers, and unchanged Item sources are not reopened") {
    val calls=ArrayBuffer.empty[String]
    val inference=new DesignerInference((ref,_,_)=>{calls+=ref;InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),certainty="known")))},
      (uri,_,_)=>{calls+=uri;StreamDescriptor(Set("bytes"),certainty="known")})
    val text="""name: Variables
      |tasks:
      |- id: read
      |  operator: read_item@1.0
      |  inputs: {}
      |  params: {uri: 'dacp://example/weather/data'}
      |- id: filter
      |  operator: filter@1
      |  inputs: {source: read}
      |  params: {threshold: $THRESHOLD}
      |outputs: {result: filter}
      |""".stripMargin
    val cancel=new StreamCancel()
    def run(threshold:Int,unused:Int):Unit={calls.clear();inference.infer(Documents.job(WorkflowVariables.resolve(text,Json.obj().put("THRESHOLD",threshold).put("UNUSED",unused))),cancel,editSession=Some("c"*32))}
    try {
      run(1,0);assert(calls.size==2)
      run(1,2);assert(calls.isEmpty)
      run(2,2);assert(calls.toVector==Vector("filter@1"))
    }finally cancel.close()
  }

}
