package dacp.designer

import org.scalatest.funsuite.AnyFunSuite
import dacp.union.{Json,YamlDocument}
import com.sun.net.httpserver.HttpServer
import java.net.{InetSocketAddress,URI}
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8

class DesignerSpec extends AnyFunSuite {
  val workflow="""name: Weather
                  |tasks:
                  |  - id: read
                  |    operator: read_item@1.0
                  |    inputs: {}
                  |    params: {uri: $INPUT_URI, view: bytes}
                  |  - id: parse
                  |    operator: piflow_csv_parser@1.0
                  |    node: nd-abcd-efgh
                  |    inputs: {source: read}
                  |outputs: {result: parse}
                  |""".stripMargin
  test("Outlet rejects multiple step output bindings"){
    val error=intercept[IllegalArgumentException](Documents.job(workflow.replace("outputs: {result: parse}","outputs: {result: parse, second: read}")))
    assert(error.getMessage.contains("only one step output"))
  }
  test("round-trip YAML, typed variables, cross-node placement and sinks"){
    val j=Documents.job(workflow);assert(Documents.job(YamlDocument.dump(j))==j)
    val vars=Documents.variables("N: 123\nENABLED: true\nNAME: '001'\n")
    assert(vars.path("N").isInt && vars.path("ENABLED").isBoolean && vars.path("NAME").asText=="001")
    assert(Documents.job(workflow.replace("outputs: {result: parse}","outputs: {}" )).path("outputs").size()==0)
    assert(Documents.job("name: Empty\ntasks: []\noutputs: {}\n",true).path("tasks").size()==0)
    intercept[IllegalArgumentException](Documents.job("name: Empty\ntasks: []\noutputs: {}\n"))
  }
  test("designer layout and provenance round-trip and reject invalid metadata"){
    val text=workflow+"""designer:
      |  version: 1
      |  generator: {name: DACP Job Designer, version: 0.1.0-SNAPSHOT}
      |  exported_at: '2026-10-28T12:34:56Z'
      |  positions: {read: {x: 120, y: 250}, parse: {x: 600, y: 90}}
      |  node_areas: {nd-abcd-efgh: {x: 500, y: 40}}
      |  outlet: {x: 1200, y: 400}
      |  viewport: {zoom: 0.8, scroll_left: 100, scroll_top: 200}
      |""".stripMargin
    val parsed=Documents.job(text)
    assert(Documents.job(YamlDocument.dump(parsed))==parsed)
    intercept[IllegalArgumentException](Documents.job(text.replace("2026-10-28T12:34:56Z","yesterday")))
    intercept[IllegalArgumentException](Documents.job(text.replace("zoom: 0.8","zoom: 0")))
  }
  test("reject cycles, duplicate consumers, object bindings, obsolete dependencies and unsafe YAML"){
    intercept[IllegalArgumentException](Documents.job(workflow.replace("source: read","source: parse")))
    intercept[IllegalArgumentException](Documents.job(workflow.replace("result: parse","result: read")))
    intercept[IllegalArgumentException](Documents.job(workflow.replace("source: read","source: {task: read}")))
    intercept[IllegalArgumentException](Documents.job(workflow.replace("    node:","    depends_on: [read]\n    node:")))
    intercept[IllegalArgumentException](Documents.variables("x: !!java.net.URL ['https://example.com']"))
    intercept[IllegalArgumentException](Documents.variables("x: 1\nx: 2\n"))
  }
  test("schema preview decodes real Arrow fields and preserves explicit unknowns"){
    val schema=org.apache.arrow.vector.types.pojo.Schema.fromJSON("""{"fields":[{"name":"temperature","nullable":true,"type":{"name":"floatingpoint","precision":"DOUBLE"},"children":[]}]}""")
    val report=Json.obj();val outputs=report.putObject("outputs");outputs.putObject("result").put("schema",java.util.Base64.getEncoder.encodeToString(schema.serializeAsMessage())).put("certainty","known");outputs.putObject("unknown").put("certainty","unknown")
    val preview=SchemaPreview.present(report)
    assert(preview.path("outputs").path("result").path("schema_json").path("fields").get(0).path("name").asText=="temperature")
    assert(!preview.path("outputs").path("unknown").has("schema_json"))
    assert(!report.path("outputs").path("result").has("schema_json"))
  }
  test("public HTTP editor, export and repository-driven command chat without leaking credentials"){
    val repo=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    repo.createContext("/v1/operators",x=>{val bytes="""[{"name":"piflow_csv_parser","version":"1.0","category":"Data processing","icon":{"path":"M3 5h18v14H3z","color":"#14866b"},"parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false},"inputs":{"source":{"representations":["bytes"]}}},{"name":"piflow_filter","version":"1.0","category":"Data processing","icon":{"path":"M3 5h18v14H3z","color":"#14866b"},"parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false},"inputs":{"source":{"representations":["records"]}}}]""".getBytes(UTF_8);x.sendResponseHeaders(200,bytes.length);x.getResponseBody.write(bytes);x.close()});repo.start()
    val config=Documents.mapping(s"""host: 127.0.0.1
                                  |port: 0
                                  |target:
                                  |  {kind: node, url: 'dacp://localhost:18861', token_file: '/private/secret.token'}
                                  |repository:
                                  |  {url: 'http://127.0.0.1:${repo.getAddress.getPort}'}
                                  |""".stripMargin)
    val site=new Designer(config);site.start();val client=HttpClient.newHttpClient()
    def get(path:String)=client.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${site.address.getPort}"+path)).GET().build(),HttpResponse.BodyHandlers.ofString())
    def post(path:String,body:com.fasterxml.jackson.databind.JsonNode)=client.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${site.address.getPort}"+path)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body.toString)).build(),HttpResponse.BodyHandlers.ofString())
    try{
      assert(get("/").body().contains("id=\"outputs-tab\""));assert(!get("/").body().contains("id=\"infer\""));assert(!get("/").body().contains("id=\"tab-schema\""));assert(!get("/api/config").body().contains("secret.token"))
      assert(get("/").body().contains("/yaml-editor.js"))
      val editor=get("/yaml-editor.js")
      assert(editor.statusCode()==200 && editor.body().contains("CodeMirror"))
      assert(!editor.body().contains("https://esm.sh"))
      assert(get("/editor-licenses.txt").body().contains("@codemirror/lang-yaml"))
      val export=post("/api/export",Json.obj().put("job_yaml",workflow).put("variables_yaml","N: 123"))
      assert(export.statusCode()==200);val exported=Documents.job(Json.mapper.readTree(export.body()).path("job_yaml").asText)
      val layout=exported.path("designer")
      assert(layout.path("generator").path("name").asText=="DACP Job Designer")
      assert(layout.path("generator").path("version").asText==DesignerLayout.softwareVersion)
      java.time.Instant.parse(layout.path("exported_at").asText)
      assert(exported.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].remove("designer")!=null)
      assert(exported==Documents.job(workflow))
      val plain=post("/api/export",Json.obj().put("job_yaml",workflow).put("variables_yaml","{}").put("include_designer",false))
      assert(plain.statusCode()==200)
      assert(!Documents.job(Json.mapper.readTree(plain.body()).path("job_yaml").asText).has("designer"))
      val request=Json.obj().put("job_yaml",workflow.replace("    node: nd-abcd-efgh\n","")).put("variables_yaml","{}").put("message","add piflow_filter@1.0")
      val response=post("/api/chat",request);assert(response.statusCode()==200,response.body())
      assert(Documents.job(Json.mapper.readTree(response.body()).path("job_yaml").asText).path("tasks").size()==3)
      request.put("message","add imaginary@1.0");assert(post("/api/chat",request).statusCode()==400)
      assert(post("/api/nodes",Json.obj()).statusCode()==200)
      assert(post("/api/connect",Json.obj()).statusCode()==400)
      assert(post("/api/operators",Json.obj().put("repository","ops")).statusCode()==400)
      assert(post("/api/nodes",Json.obj().put("target","local")).statusCode()==400)
      assert(!get("/").body().contains("id=\"toggle-connections\""))
      assert(get("/../build.sbt").statusCode()==404)
      assert(post("/api/submit",Json.obj()).statusCode()==400)
    }finally{site.close();repo.stop(0)}
  }
}
