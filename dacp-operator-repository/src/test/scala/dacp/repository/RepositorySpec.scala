package dacp.repository

import java.nio.file.{Files, Path}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class RepositorySpec extends AnyFunSuite {
  private def withRoot(f: Path => Unit): Unit = {
    val root = Files.createTempDirectory("operator-repository-")
    try f(root) finally {
      val paths = Files.walk(root)
      try paths.iterator.asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally paths.close()
    }
  }
  private val ports=Repository.mapper.readTree("""{"icon":{"path":"M3 5h18v14H3zM3 10h18M9 5v14","color":"#14866b"},"parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false},"inputs":{"source":{"representations":["bytes"],"formats":[]}}}""")
  private def publish(root:Path, reference:String, image:String, exporter:Repository.ImageExporter, description:String="",
    executor:String="container",entrypoint:String="",artifact:Option[Path]=None):String =
    Repository.publish(root,reference,image,ports,exporter,description,executor,entrypoint,artifact)
  private val image = "sha256:" + "a" * 64
  private val exporter = new Repository.ImageExporter {
    def inspect(tag: String): String = image
    def save(id: String, destination: Path): Unit = {
      assert(id == image)
      Files.writeString(destination, "image archive")
    }
  }
  test("executor-owned configuration survives publication and seeding without plugin dependencies") { withRoot { root =>
    val settings=Repository.mapper.readTree("""{"args":["a-z","A-Z"],"stdin":"source","stdout":{"port":"result","representation":"bytes","format":"text"}}""")
    val published=Repository.mapper.readTree(Repository.publish(root,"command@1","native:1",ports,exporter,
      executor="command",entrypoint="/usr/bin/tr",configuration=settings))
    assert(published.path("execution").path("config")==settings)
    val definition=root.resolve("definitions/seeded/1/operator.yaml")
    Files.createDirectories(definition.getParent)
    val node=Repository.mapper.createObjectNode().put("name","seeded").put("version","1").put("image","native:1")
      .put("executor","command").put("entrypoint","/usr/bin/tr")
    node.set[com.fasterxml.jackson.databind.JsonNode]("inputs",ports.path("inputs"))

    node.set[com.fasterxml.jackson.databind.JsonNode]("icon",Repository.mapper.readTree("""{"path":"M3 5h18v14H3z","color":"#14866b"}"""))
    node.set[com.fasterxml.jackson.databind.JsonNode]("parameters",ports.path("parameters"))
    node.set[com.fasterxml.jackson.databind.JsonNode]("executor_config",settings)
    Files.writeString(definition,node.toString)
    assert(Repository.seed(root,root.resolve("definitions"),exporter)==Seq("seeded@1"))
    assert(Repository.mapper.readTree(Files.readString(root.resolve("seeded/1/manifest.json"))).path("execution").path("config")==settings)
    intercept[IllegalArgumentException](Repository.publish(root,"invalid@1","native:1",ports,exporter,
      configuration=Repository.mapper.createArrayNode()))
  }}
  test("starter operators seed a fresh catalog and preserve immutable versions on repeated setup") { withRoot { root =>
    var exports = 0
    val counting = new Repository.ImageExporter {
      def inspect(tag: String): String = exporter.inspect(tag)
      def save(id: String, destination: Path): Unit = { exports += 1; exporter.save(id, destination) }
    }
    val definitions = root.resolve("definitions")
    val bundle = root.resolve("bundle.jar"); Files.writeString(bundle, "operator code")
    Seq("format_convert", "identity").foreach { name =>
      val target = definitions.resolve(s"$name/1.1/operator.yaml")
      Files.createDirectories(target.getParent)
      val source = Files.readString(Path.of(s"../operators/$name/1.1/operator.yaml"))
      Files.writeString(target, source.replaceAll("(?m)^artifact:.*$", "artifact: " + bundle.toString))
    }
    val expected = Seq("format_convert@1.1", "identity@1.1")
    assert(Repository.seed(root, definitions, counting) == expected)
    assert(exports == 1)
    assert(Repository.catalog(root).size() == 2)
    assert(Repository.catalog(root).get(0).path("description").asText.contains("Parquet"))
    assert(Repository.seed(root, definitions, counting) == expected)
    assert(exports == 1)
    Files.writeString(Repository.blob(root, Repository.catalog(root).get(0).path("runtime").path("archive").path("sha256").asText), "corrupt")
    intercept[IllegalArgumentException](Repository.seed(root, definitions, counting))
  }}
  test("seeding discovers contributed definitions and rejects mismatched identities") { withRoot { root =>
    val definitions = root.resolve("definitions")
    val definition = definitions.resolve("custom/2.0/operator.yaml")
    Files.createDirectories(definition.getParent)
    Files.writeString(definition, "name: custom\nversion: '2.0'\nimage: custom:2\nicon: {path: M3 5h18v14H3z, color: '#14866b'}\nparameters: {\"type\":\"object\",\"properties\":{},\"required\":[],\"additionalProperties\":false}\ninputs: {source: {representations: [bytes], formats: []}}\n")
    val catalog = root.resolve("catalog")
    assert(Repository.seed(catalog, definitions, exporter) == Seq("custom@2.0"))
    Files.writeString(definition, "name: other\nversion: '2.0'\nimage: custom:2\nicon: {path: M3 5h18v14H3z, color: '#14866b'}\nparameters: {\"type\":\"object\",\"properties\":{},\"required\":[],\"additionalProperties\":false}\ninputs: {source: {representations: [bytes], formats: []}}\n")
    intercept[IllegalArgumentException](Repository.seed(catalog, definitions, exporter))
    Files.writeString(definition, "name: custom\nversion: '2.0'\nimage: custom:2\nimage: replaced\n")
    intercept[org.yaml.snakeyaml.error.YAMLException](Repository.seed(catalog, definitions, exporter))
  }}
  test("publication pins the image, verifies bytes and refuses overwrites") { withRoot { root =>
    val json = Repository.mapper.readTree(publish(root, "identity@1.0", "mutable:tag", exporter))
    assert(json.path("inputs")==ports.path("inputs"))
    assert(json.path("api_version").asInt == 2)
    assert(json.path("name").asText == "identity" && json.path("version").asText == "1.0")
    assert(json.path("runtime").path("image").asText == image)
    assert(json.path("runtime").path("archive").path("size").asLong == 13)
    assert(json.path("runtime").path("archive").path("sha256").asText == Repository.checksum(Repository.blob(root, json.path("runtime").path("archive").path("sha256").asText)))
    intercept[IllegalArgumentException](publish(root,"identity@1.0","other",exporter))
    assert(Files.readString(root.resolve("identity/1.0/manifest.json")) == Repository.mapper.writeValueAsString(json))
  }}
  test("invalid references and failed exports leave no published or partial artifacts") { withRoot { root =>
    Seq("../x@1", "x@..", "x@y/z", "x", "x@1@2", "x@", "x@%2e").foreach { ref =>
      intercept[IllegalArgumentException](Repository.parts(ref))
    }
    val broken = new Repository.ImageExporter {
      def inspect(tag: String): String = image
      def save(id: String, destination: Path): Unit = { Files.writeString(destination, "partial"); throw new RuntimeException("failed") }
    }
    intercept[RuntimeException](publish(root,"identity@1.0","tag",broken))
    assert(!Files.exists(root.resolve("identity/1.0")))
    val files = Files.list(root.resolve("identity"))
    try assert(!files.iterator.asScala.exists(_.getFileName.toString.startsWith(".publish-"))) finally files.close()
    publish(root,"identity@1.0","tag",exporter)
  }}
  test("homepage selects the newest numeric version independently of catalog order") {
    def item(v:String)={val n=ports.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();n.put("name","demo").put("version",v)}
    val versions=Seq("1.9","1.10","1.2","1.10-rc1").map(item)
    val page=WebPages.index(versions,"")
    assert(page.contains("/operators/demo/1.10\""))
    assert(!page.contains("/operators/demo/1.9\""))
    assert(!page.contains("/operators/demo/1.10-rc1"))
    assert(page.sliding("class=\"operator-card\"".length).count(_=="class=\"operator-card\"")==1)
    assert(WebPages.index(versions.reverse,"")==page)
    assert(WebPages.versionOrdering.gt("2.0","1.99"))
    assert(WebPages.versionOrdering.gt("2.0-rc10","2.0-rc2"))
    assert(WebPages.versionOrdering.gt("2.0","2.0-rc10"))
  }
  test("public web search, operator details and downloads work without authentication") { withRoot { root =>
    publish(root, "convert@1.0", "tag", exporter, "Convert CSV to Parquet")
    publish(root, "convert@2.0", "tag", exporter, "Convert tables")
    publish(root, "identity@1.0", "tag", exporter, "Copy <script>alert(1)</script> & preserve bytes")
    ReviewTestSupport.approveLocal(root)
    val server = new RepositoryServer(root, port=0)
    server.start()
    val client = HttpClient.newHttpClient()
    def request(path: String, method: String = "GET") = client.send(
      HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${server.boundPort}$path"))
        .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString())
    try {
      val home = request("/")
      assert(home.statusCode() == 200 && home.headers().firstValue("Content-Type").get().startsWith("text/html"))
      assert(home.body().contains("/operators/convert/2.0") && home.body().contains("/operators/identity/1.0"))
      assert(!home.body().contains("/operators/convert/1.0"))
      assert(home.body().contains("2 operators"))
      assert(home.body().contains("&lt;script&gt;") && !home.body().contains("<script>"))
      val filtered = request("/?q=PARQUET")
      assert(filtered.statusCode() == 200 && filtered.body().contains("/operators/convert/2.0"))
      assert(!filtered.body().contains("/operators/identity/1.0") && !filtered.body().contains("/operators/convert/1.0"))
      assert(request("/?q=nonexistent").body().contains("No matching operators"))
      assert(request("/?q=%3Cscript%3E").body().contains("&lt;script&gt;"))
      assert(!request("/?q=%22%3E%3Cscript%3E").body().contains("<script>"))
      assert(request("/?q=" + "a" * 201).statusCode() == 400)
      assert(request("/?q=a&q=b").statusCode() == 400)
      val detail = request("/operators/convert/1.0")
      assert(detail.statusCode() == 200 && detail.body().contains("operator: convert@1.0"))
      assert(detail.body().contains("/operators/convert/2.0"))
      assert(detail.body().contains("/v1/blobs/sha256/"))
      assert(detail.body().contains(Repository.catalog(root).get(0).path("runtime").path("archive").path("sha256").asText))
      assert(request("/operators/missing/1.0").statusCode() == 404)
      assert(request("/operators/%2e%2e/1.0").statusCode() == 404)
      assert(home.body().contains("/assets/dacp-logo.png") && !home.body().contains("brand-mark"))
      val logo = client.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${server.boundPort}/assets/dacp-logo.png")).build(), HttpResponse.BodyHandlers.ofByteArray())
      assert(logo.statusCode() == 200 && logo.headers().firstValue("Content-Type").get() == "image/png")
      assert(logo.body().sameElements(Files.readAllBytes(java.nio.file.Path.of("../assets/dacp-logo.png"))))
      val logoHead = request("/assets/dacp-logo.png", "HEAD")
      assert(logoHead.statusCode() == 200 && logoHead.body().isEmpty)
      assert(logoHead.headers().firstValue("Content-Length").get().toInt == logo.body().length)
      val css = request("/assets/repository.css")
      assert(css.statusCode() == 200 && css.headers().firstValue("Content-Type").get().startsWith("text/css"))
      val archive = request("/v1/blobs/sha256/" + Repository.catalog(root).get(0).path("runtime").path("archive").path("sha256").asText)
      assert(archive.body() == "image archive")
      assert(archive.headers().firstValue("Content-Disposition").get().startsWith("attachment;"))
      val blobUrl="/v1/blobs/sha256/"+Repository.catalog(root).get(0).path("runtime").path("archive").path("sha256").asText
      val named=request(blobUrl+"/runtime.tar")
      assert(named.statusCode()==200 && named.body()==archive.body())
      assert(named.headers().firstValue("Content-Disposition").get()=="attachment; filename=\"runtime.tar\"")
      val namedHead=request(blobUrl+"/runtime.tar","HEAD")
      assert(namedHead.body().isEmpty && namedHead.headers().firstValue("Content-Disposition").get()=="attachment; filename=\"runtime.tar\"")
      assert(request(blobUrl+"/bad%0D%0Aname.zip").statusCode()==404)

      val head = request("/v1/blobs/sha256/" + Repository.catalog(root).get(0).path("runtime").path("archive").path("sha256").asText, "HEAD")
      assert(head.statusCode() == 200 && head.body().isEmpty && head.headers().firstValue("Content-Length").get() == "13")
      assert(request("/", "HEAD").body().isEmpty)
      assert(home.headers().firstValue("Content-Security-Policy").get().contains("default-src 'none'"))
    } finally server.close()
  }}
  test("code bundles and runtimes are shared across operator identities") { withRoot { root =>
    val code = root.resolve("operators.jar"); Files.writeString(code, "small code bundle")
    val first = Repository.mapper.readTree(publish(root, "first@1", "runtime", exporter,
      executor="jvm", entrypoint="example.First", artifact=Some(code)))
    val second = Repository.mapper.readTree(publish(root, "second@1", "runtime", exporter,
      executor="jvm", entrypoint="example.Second", artifact=Some(code)))
    assert(first.path("artifact") == second.path("artifact"))
    assert(first.path("runtime") == second.path("runtime"))
    val blobs = Files.list(root.resolve("blobs/sha256"))
    try assert(blobs.count() == 2) finally blobs.close()
    assert(!Files.exists(root.resolve("first/1/image.tar")))
    ReviewTestSupport.approveLocal(root)
    val server = new RepositoryServer(root, port=0); server.start()
    try {
      val page = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
        s"http://127.0.0.1:${server.boundPort}/operators/first/1")).build(), HttpResponse.BodyHandlers.ofString())
      assert(page.body().contains("Operator code") && page.body().contains("Shared runtime"))
      assert(page.body().contains("SHA-256: " + first.path("artifact").path("sha256").asText))
      assert(!page.body().contains("{{"))
    } finally server.close()
  }}
  test("HTTP contract serves manifests and archives and rejects traversal and writes") { withRoot { root =>
    val manifest = publish(root,"identity@1.0","tag",exporter)
    ReviewTestSupport.approveLocal(root)
    val server = new RepositoryServer(root,port=0)
    server.start()
    val client = HttpClient.newHttpClient()
    def request(path: String, method: String = "GET") = client.send(
      HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${server.boundPort}$path"))
        .method(method,HttpRequest.BodyPublishers.noBody()).build(),HttpResponse.BodyHandlers.ofString())
    try {
      assert(request("/health").body() == "ok\n")
      val listing = request("/v1/operators")
      assert(listing.statusCode() == 200)
      val catalog = Repository.mapper.readTree(listing.body())
      assert(catalog.size() == 1 && catalog.get(0).path("name").asText == "identity")
      Files.createDirectories(root.resolve("unfinished/1"))
      assert(Repository.mapper.readTree(request("/v1/operators").body()).size() == 1)
      val json = request("/v1/operators/identity/1.0")
      assert(json.statusCode() == 200 && Repository.mapper.readTree(json.body()).path("name")==Repository.mapper.readTree(manifest).path("name"))
      assert(json.headers().firstValue("Content-Type").get() == "application/json")
      val archive = request("/v1/blobs/sha256/" + catalog.get(0).path("runtime").path("archive").path("sha256").asText)
      assert(archive.statusCode() == 200 && archive.body() == "image archive")
      assert(archive.headers().firstValue("Content-Length").get() == "13")
      Seq("/v1/operators/../1.0", "/v1/operators/%2e%2e/1", "/etc/passwd", "/v1/operators/missing/1", "/v1/operators/identity/1.0?x=y").foreach { path =>
        assert(request(path).statusCode() == 404)
      }
      assert(request("/v1/operators/identity/1.0","POST").statusCode() == 405)
      val outside = Files.createTempDirectory("outside-catalog-")
      try {
        Files.writeString(outside.resolve("manifest.json"),"private")
        Files.createDirectories(root.resolve("escape"))
        Files.createSymbolicLink(root.resolve("escape/1"),outside)
        assert(request("/v1/operators/escape/1").statusCode() == 404)
      } finally { Files.delete(outside.resolve("manifest.json")); Files.delete(outside) }
    } finally server.close()
  }}
  test("sources and sinks publish while missing port mappings are rejected") { withRoot { root =>
    val source=ports.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()

    source.set[com.fasterxml.jackson.databind.JsonNode]("icon",Repository.mapper.readTree("""{"path":"M3 5h18v14H3z","color":"#14866b"}"""))
    source.set[com.fasterxml.jackson.databind.JsonNode]("parameters",ports.path("parameters"))
    source.putObject("inputs")
    val result=Repository.mapper.readTree(Repository.publish(root,"ticks@1","runtime",source,exporter))
    assert(result.path("inputs").isObject && result.path("inputs").size()==0)
    source.remove("inputs")
    intercept[IllegalArgumentException](Repository.validatePorts(source))

    source.set[com.fasterxml.jackson.databind.JsonNode]("icon",Repository.mapper.readTree("""{"path":"M3 5h18v14H3z","color":"#14866b"}"""))
    source.set[com.fasterxml.jackson.databind.JsonNode]("parameters",ports.path("parameters"))
    source.putObject("inputs")
    Repository.validatePorts(source)
    source.putObject("outputs")
    intercept[IllegalArgumentException](Repository.validatePorts(source))
  }}
  test("port declarations are mandatory before exporting any runtime") { withRoot { root=>
    val rejecting=new Repository.ImageExporter {
      def inspect(tag:String):String=fail("must validate ports before Docker")
      def save(id:String,destination:Path):Unit=fail("must not export")
    }
    intercept[IllegalArgumentException](Repository.publish(root,"bad@1","image",Repository.mapper.createObjectNode(),rejecting))
    val invalid=ports.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    invalid.path("inputs").path("source").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].putArray("representations").add("unknown")
    intercept[IllegalArgumentException](Repository.publish(root,"bad@1","image",invalid,rejecting))
  }}

  test("parameter declarations survive publication and public views; malformed contracts cannot publish") {withRoot{root=>
    val metadata=ports.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    val parameters=Repository.mapper.readTree("""{"type":"object","properties":{"limit":{"type":"integer","minimum":1,"default":10,"description":"Maximum rows"}},"required":[],"additionalProperties":false}""")

    metadata.set[com.fasterxml.jackson.databind.JsonNode]("icon",Repository.mapper.readTree("""{"path":"M3 5h18v14H3z","color":"#14866b"}"""))
    metadata.set[com.fasterxml.jackson.databind.JsonNode]("parameters",parameters)
    val published=Repository.mapper.readTree(Repository.publish(root,"typed@1","runtime",metadata,exporter))
    assert(published.path("parameters")==parameters)
    assert(Repository.catalog(root).get(0).path("parameters")==parameters)
    assert(WebPages.detail(published,Seq(published)).contains("Maximum rows"))
    metadata.remove("parameters")
    intercept[IllegalArgumentException](Repository.publish(root,"missing@1","runtime",metadata,exporter))

    metadata.set[com.fasterxml.jackson.databind.JsonNode]("icon",Repository.mapper.readTree("""{"path":"M3 5h18v14H3z","color":"#14866b"}"""))
    metadata.set[com.fasterxml.jackson.databind.JsonNode]("parameters",parameters)
    parameters.path("properties").path("limit").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("default","bad")
    intercept[IllegalArgumentException](Repository.publish(root,"bad@1","runtime",metadata,exporter))
  }}
  test("all PiFlow build definitions carry valid language-neutral parameter contracts") {
    val specs=Repository.mapper.readTree(Files.readString(Path.of("../examples/operators/piflow/specs.json")))
    specs.elements.asScala.foreach{n=>dacp.operator.OperatorPresentation.validate(n);dacp.operator.ParameterSchema.validate(n.get("parameters"))}
  }
  test("presentation metadata is required on publication and rendered in public search and detail") {withRoot {root=>
    for(field<-Seq("icon")){
      val bad=ports.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();bad.remove(field)
      intercept[IllegalArgumentException](Repository.publish(root,"missing@1","runtime",bad,exporter))
    }
    val n=Repository.mapper.readTree(Repository.publish(root,"visible@1","runtime",ports,exporter))
    assert(!n.has("category") && n.path("icon")==ports.path("icon"))
    n.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].putArray("categories").add("Data processing").add("Tables")
    val index=WebPages.index(Seq(n),"Data processing")
    assert(index.contains("/operators/visible/1") && index.contains("M3 5h18v14H3zM3 10h18M9 5v14"))
    assert(WebPages.detail(n,Seq(n)).contains("Data processing"))
  }}

}
