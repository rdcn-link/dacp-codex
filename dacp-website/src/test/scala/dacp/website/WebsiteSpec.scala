package dacp.website
import org.scalatest.funsuite.AnyFunSuite
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files

class WebsiteSpec extends AnyFunSuite {
  test("English pages, packaged documentation, safe downloads and unavailable resources") {
    val dir = Files.createTempDirectory("dacp-website-test")
    Files.writeString(dir.resolve("example.zip"), "archive-content")
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
      assert(get("/downloads").body().contains("example.zip"))
      val download = get("/download/example.zip")
      assert(download.body() == "archive-content")
      assert(download.headers().firstValue("Content-Disposition").get().contains("example.zip"))
      assert(get("/download/../build.sbt").statusCode() == 404)
      assert(get("/download/%2e%2e%2fbuild.sbt").statusCode() == 404)
      assert(get("/guide/docs/unknown_en.md").statusCode() == 404)
      assert(get("/assets/dacp-favicon.png").statusCode() == 200)
      Files.createSymbolicLink(dir.resolve("outside.zip"), dir.getParent.resolve("outside"))
      assert(get("/download/outside.zip").statusCode() == 404)
    } finally { site.close(); Files.deleteIfExists(dir.resolve("outside.zip")); Files.delete(dir.resolve("example.zip")); Files.delete(dir) }
  }
}
