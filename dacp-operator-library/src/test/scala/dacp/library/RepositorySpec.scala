package dacp.library

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
  private val image = "sha256:" + "a" * 64
  private val exporter = new Repository.ImageExporter {
    def inspect(tag: String): String = image
    def save(id: String, destination: Path): Unit = {
      assert(id == image)
      Files.writeString(destination, "image archive")
    }
  }
  test("starter operators seed a fresh catalog and preserve immutable versions on repeated setup") { withRoot { root =>
    var exports = 0
    val counting = new Repository.ImageExporter {
      def inspect(tag: String): String = exporter.inspect(tag)
      def save(id: String, destination: Path): Unit = { exports += 1; exporter.save(id, destination) }
    }
    val expected = Seq("format_convert@1.0", "identity@1.0")
    assert(Repository.seed(root, Path.of("../operators"), counting) == expected)
    assert(exports == 2)
    assert(Repository.catalog(root).size() == 2)
    assert(Repository.seed(root, Path.of("../operators"), counting) == expected)
    assert(exports == 2)
    Files.writeString(root.resolve("identity/1.0/image.tar"), "corrupt")
    intercept[IllegalArgumentException](Repository.seed(root, Path.of("../operators"), counting))
  }}
  test("seeding discovers contributed definitions and rejects mismatched identities") { withRoot { root =>
    val definitions = root.resolve("definitions")
    val definition = definitions.resolve("custom/2.0/operator.yaml")
    Files.createDirectories(definition.getParent)
    Files.writeString(definition, "name: custom\nversion: '2.0'\nimage: custom:2\n")
    val catalog = root.resolve("catalog")
    assert(Repository.seed(catalog, definitions, exporter) == Seq("custom@2.0"))
    Files.writeString(definition, "name: other\nversion: '2.0'\nimage: custom:2\n")
    intercept[IllegalArgumentException](Repository.seed(catalog, definitions, exporter))
    Files.writeString(definition, "name: custom\nversion: '2.0'\nimage: custom:2\nimage: replaced\n")
    intercept[org.yaml.snakeyaml.error.YAMLException](Repository.seed(catalog, definitions, exporter))
  }}
  test("publication pins the image, verifies bytes and refuses overwrites") { withRoot { root =>
    val json = Repository.mapper.readTree(Repository.publish(root, "identity@1.0", "mutable:tag", exporter))
    assert(json.path("api_version").asInt == 1)
    assert(json.path("name").asText == "identity" && json.path("version").asText == "1.0")
    assert(json.path("image").asText == image)
    assert(json.path("archive").path("size").asLong == 13)
    assert(json.path("archive").path("sha256").asText == Repository.checksum(root.resolve("identity/1.0/image.tar")))
    intercept[IllegalArgumentException](Repository.publish(root,"identity@1.0","other",exporter))
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
    intercept[RuntimeException](Repository.publish(root,"identity@1.0","tag",broken))
    assert(!Files.exists(root.resolve("identity/1.0")))
    val files = Files.list(root.resolve("identity"))
    try assert(!files.iterator.asScala.exists(_.getFileName.toString.startsWith(".publish-"))) finally files.close()
    Repository.publish(root,"identity@1.0","tag",exporter)
  }}
  test("HTTP contract serves manifests and archives and rejects traversal and writes") { withRoot { root =>
    val manifest = Repository.publish(root,"identity@1.0","tag",exporter)
    val server = new LibraryServer(root,port=0)
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
      Files.writeString(root.resolve("unfinished/1/manifest.json"), "{}")
      assert(Repository.mapper.readTree(request("/v1/operators").body()).size() == 1)
      val json = request("/v1/operators/identity/1.0")
      assert(json.statusCode() == 200 && json.body() == manifest)
      assert(json.headers().firstValue("Content-Type").get() == "application/json")
      val archive = request("/v1/operators/identity/1.0/image.tar")
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
}
