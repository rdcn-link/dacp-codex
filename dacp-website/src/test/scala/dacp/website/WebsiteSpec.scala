package dacp.website
import org.scalatest.funsuite.AnyFunSuite
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files

class WebsiteSpec extends AnyFunSuite {
  test("English pages, packaged documentation, safe downloads and unavailable resources") {
    val dir = Files.createTempDirectory("dacp-website-test")
    Files.writeString(dir.resolve("dacp-server-0.1.zip"), "archive-content")
    Files.write(dir.resolve("dacp_client-0.1-py3-none-any.whl"), new Array[Byte](2 * 1024 * 1024 + 3 * 1024))
    Files.writeString(dir.resolve("dacp-website-0.1.zip"), "excluded")
    Files.writeString(dir.resolve("dacp-client-scala-development.zip"), "excluded")
    val site = new Website("127.0.0.1", 0, dir)
    site.start()
    val client = HttpClient.newHttpClient()
    def get(path: String) = client.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${site.address.getPort}" + path)).build(), HttpResponse.BodyHandlers.ofString())
    try {
      assert(get("/").body().contains("Stream data."))
      assert(get("/docs").body().contains("Administrator guide"))
      val doc = get("/guide/docs/users/README_en.md")
      assert(doc.statusCode() == 200)
      assert(doc.body().contains("<article>"))
      assert(doc.headers().firstValue("Content-Security-Policy").isPresent)
      assert(get("/downloads").body().contains("dacp-server-0.1.zip"))
      assert(get("/downloads").body().contains("2.0MB"))
      assert(!get("/downloads").body().contains("dacp-website-0.1.zip"))
      assert(!get("/downloads").body().contains("dacp-client-scala-development.zip"))
      assert(get("/download/dacp-website-0.1.zip").statusCode() == 404)
      Files.write(dir.resolve("dacp_client-0.1-py3-none-any.whl"), new Array[Byte](28 * 1024 + 900))
      assert(get("/downloads").body().contains("28kB"))
      val download = get("/download/dacp-server-0.1.zip")
      assert(download.body() == "archive-content")
      assert(download.headers().firstValue("Content-Disposition").get().contains("dacp-server-0.1.zip"))
      assert(get("/download/../build.sbt").statusCode() == 404)
      assert(get("/download/%2e%2e%2fbuild.sbt").statusCode() == 404)
      assert(get("/guide/docs/unknown_en.md").statusCode() == 404)
      assert(get("/assets/dacp-favicon.png").statusCode() == 200)
      Files.createSymbolicLink(dir.resolve("dacp-union-server-0.1.zip"), dir.getParent.resolve("outside"))
      assert(get("/download/dacp-union-server-0.1.zip").statusCode() == 404)
    } finally { site.close(); Files.deleteIfExists(dir.resolve("dacp-union-server-0.1.zip")); Files.delete(dir.resolve("dacp-server-0.1.zip")); Files.delete(dir.resolve("dacp_client-0.1-py3-none-any.whl")); Files.delete(dir.resolve("dacp-website-0.1.zip")); Files.delete(dir.resolve("dacp-client-scala-development.zip")); Files.delete(dir) }
  }
}
