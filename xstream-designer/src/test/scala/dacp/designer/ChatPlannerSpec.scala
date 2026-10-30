package xstream.designer


import org.scalatest.funsuite.AnyFunSuite
import dacp.union.Json
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference
import com.fasterxml.jackson.databind.JsonNode

class TestChatPlugin extends ChatPlanner {
  def name="test-contributed-provider"
  def plan(request:JsonNode):JsonNode=request
}
class ChatPlannerSpec extends AnyFunSuite {
  test("compatible chat model receives current draft and history and returns a JSON proposal"){
    val observed=new AtomicReference[JsonNode]()
    val model=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    val proposal=Json.obj().put("reply","Renamed").put("job_yaml","name: Updated\ntasks: []\noutputs: {}\n").put("variables_yaml","N: 123\n")
    model.createContext("/v1/chat/completions",x=>{
      observed.set(Json.mapper.readTree(x.getRequestBody.readAllBytes()))
      val response=Json.obj();response.putArray("choices").addObject().put("finish_reason","stop").putObject("message").put("role","assistant").put("content",proposal.toString)
      val bytes=Json.mapper.writeValueAsBytes(response);x.sendResponseHeaders(200,bytes.length);x.getResponseBody.write(bytes);x.close()
    });model.start()
    try{
      val config=Json.obj().put("provider","openai-compatible").put("url",s"http://127.0.0.1:${model.getAddress.getPort}/v1/chat/completions").put("model","test-model")
      val planner=ChatPlanner.configured(config)
      val request=Json.obj().put("job_yaml","name: Old\ntasks: []\noutputs: {}\n").put("message","Rename the job");request.putArray("messages").addObject().put("role","user").put("content","Rename the job")
      request.putArray("operators");request.putArray("nodes")
      assert(planner.plan(request)==proposal)
      assert(observed.get.path("model").asText=="test-model")
      assert(observed.get.path("messages").get(0).path("role").asText=="system")
      assert(observed.get.path("messages").get(2).path("content").asText.contains("name: Old"))
      assert(!observed.get.path("stream").asBoolean)
    }finally model.stop(0)
  }
  test("contributed planners are discovered through the shared ServiceLoader contract"){
    val planner=ChatPlanner.configured(Json.obj().put("provider","test-contributed-provider"))
    val request=Json.obj().put("message","test")
    assert(planner.plan(request)==request)
  }
  test("unknown external provider fails clearly"){
    intercept[IllegalArgumentException](ChatPlanner.configured(Json.obj().put("provider","not-installed")))
  }
}
