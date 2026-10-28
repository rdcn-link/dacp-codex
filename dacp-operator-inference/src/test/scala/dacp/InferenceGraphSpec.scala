package dacp

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

class InferenceGraphSpec extends AnyFunSuite {
  private val bytes=StreamDescriptor(Set("bytes"),Set("text"),certainty="known")
  test("completed steps are published before downstream inference starts"){
    val emitted=ArrayBuffer.empty[String]
    val nodes=Vector(InferenceNode("read",Map.empty[String,(String,String)],"read"),InferenceNode("next",Map("source"->("read","result")),"next"))
    InferenceGraph.propagate(nodes,Map("result"->("next","result")),onStep=(id,_)=>emitted+=id){(id,_)=>
      if(id=="next")assert(emitted.toVector==Vector("read"))
      InferenceResult(Map("result"->bytes))
    }
    assert(emitted.toVector==Vector("read","next"))
  }
  test("authoring reports step errors without erasing successful upstream or independent outputs") {
    val nodes=Vector(
      InferenceNode("read",Map.empty[String,(String,String)],"read"),
      InferenceNode("broken",Map("wrong"->("read","result")),"broken"),
      InferenceNode("dependent",Map("source"->("broken","result")),"dependent"),
      InferenceNode("independent",Map.empty[String,(String,String)],"independent"))
    val called=ArrayBuffer.empty[String]
    val result=InferenceGraph.propagate(nodes,Map("bad"->("dependent","result"),"good"->("read","result")),collectErrors=true){(name,_)=>
      called+=name
      if(name=="broken")throw new IllegalArgumentException("missing input source")
      InferenceResult(Map("result"->bytes))
    }
    assert(result.path("tasks").path("read").path("outputs").has("result"))
    assert(result.path("tasks").path("independent").path("outputs").has("result"))
    assert(result.path("tasks").path("broken").path("diagnostics").get(0).path("message").asText.contains("missing input source"))
    assert(result.path("tasks").path("dependent").path("diagnostics").get(0).path("code").asText=="upstream_failed")
    assert(!called.contains("dependent") && result.path("outputs").has("good") && !result.path("outputs").has("bad"))
    intercept[java.util.concurrent.CancellationException](InferenceGraph.propagate(nodes.take(1),Map.empty[String,(String,String)],collectErrors=true){(_,_)=>throw new java.util.concurrent.CancellationException("cancelled")})
  }
  test("out-of-order graphs propagate fresh parameter-dependent metadata and preserve structured outputs") {
    val calls=ArrayBuffer.empty[String]
    def run(format:String)={
      val nodes=Vector(InferenceNode("convert",Map("source"->("read","result")),format),InferenceNode("read",Map.empty[String,(String,String)],"source"))
      InferenceGraph.propagate(nodes,Map("result"->("convert","result"))){(value,inputs)=>
        calls+=value
        if(value=="source")InferenceResult(Map("result"->bytes))
        else {assert(inputs("source")==bytes);InferenceResult(Map("result"->StreamDescriptor(Set("image"),Set(value),certainty="known",structure=Some(Data.mapper.readTree("""{"shape":[2,3]}""")))))}
      }
    }
    val first=run("png");val second=run("tiff")
    assert(calls.toVector==Vector("source","png","source","tiff"))
    assert(first.path("outputs").path("result").path("formats").get(0).asText=="png")
    assert(second.path("outputs").path("result").path("formats").get(0).asText=="tiff")
    assert(second.path("outputs").path("result").path("structure").path("shape").size==2)
  }
  test("sinks remain valid and graph errors do not produce invented schemas") {
    val nodes=Vector(InferenceNode("read",Map.empty[String,(String,String)],"read"),InferenceNode("sink",Map("source"->("read","result")),"sink"))
    val result=InferenceGraph.propagate(nodes,Map.empty[String,(String,String)]){(id,_)=>InferenceResult(if(id=="read")Map("result"->bytes)else Map.empty)}
    assert(result.path("tasks").path("sink").path("outputs").size==0 && result.path("outputs").size==0)
    intercept[IllegalArgumentException](InferenceGraph.propagate(nodes,Map("result"->("sink","result")))((_,_)=>InferenceResult(Map.empty)))
    val cycle=Vector(InferenceNode("a",Map("source"->("b","result")),"a"),InferenceNode("b",Map("source"->("a","result")),"b"))
    intercept[IllegalArgumentException](InferenceGraph.propagate(cycle,Map.empty[String,(String,String)])((_,_)=>InferenceResult(Map("result"->bytes))))
    intercept[IllegalArgumentException](InferenceGraph.propagate(nodes,Map.empty[String,(String,String)])((_,_)=>InferenceResult(Map.empty,Vector(InferenceDiagnostic("error","invalid","bad parameters")))) )
  }
}
