package dacp.website

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.Executors
import org.commonmark.parser.Parser
import org.commonmark.renderer.html.HtmlRenderer
import org.commonmark.ext.gfm.tables.TablesExtension
import org.commonmark.ext.heading.anchor.HeadingAnchorExtension
import scala.jdk.CollectionConverters._

final class Website(host: String, port: Int, downloads: Path) extends AutoCloseable {
  private val server = HttpServer.create(new InetSocketAddress(host, port), 0)
  private val pool = Executors.newFixedThreadPool(8)
  private val extensions = java.util.Arrays.asList(TablesExtension.create(), HeadingAnchorExtension.create())
  private val parser = Parser.builder().extensions(extensions).build()
  private val renderer = HtmlRenderer.builder().extensions(extensions).escapeHtml(true).sanitizeUrls(true).build()
  private val downloadRoot = downloads.toAbsolutePath.normalize()
  server.setExecutor(pool)
  private def escape(s: String): String = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")
  private def resource(name: String): Option[Array[Byte]] = Option(getClass.getResourceAsStream(name)).map { in => try in.readAllBytes() finally in.close() }
  private def page(title: String, body: String): Array[Byte] = (
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">" +
    "<title>" + escape(title) + " · DACP</title><link rel=\"icon\" href=\"/assets/dacp-favicon.png\"><link rel=\"stylesheet\" href=\"/style.css\"></head>" +
    "<body><header><a href=\"/\"><img src=\"/assets/dacp-logo.png\" alt=\"DACP\"></a><nav><a href=\"/docs\">Documentation</a><a href=\"/downloads\">Downloads</a><a href=\"https://github.com/rdcn-link/dacp-codex\">GitHub ↗</a></nav></header><main>" + body +
    "</main><footer>DACP · Data Access and Collaboration Protocol · Development builds, no stable release yet.</footer></body></html>").getBytes(UTF_8)
  private def send(x: HttpExchange, code: Int, mime: String, data: Array[Byte]): Unit = {
    x.getResponseHeaders.set("Content-Type", mime)
    x.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
    x.getResponseHeaders.set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; base-uri 'self'; frame-ancestors 'none'")
    x.getResponseHeaders.set("Cache-Control", "no-cache")
    x.sendResponseHeaders(code, data.length.toLong)
    try x.getResponseBody.write(data) finally x.close()
  }
  private def html(x: HttpExchange, title: String, body: String): Unit = send(x, 200, "text/html; charset=utf-8", page(title, body))
  private def softwareArtifact(name: String): Boolean =
    name.matches("dacp-(server|union-server)-[0-9][A-Za-z0-9._-]*\\.zip") ||
      name.matches("dacp_client-[0-9][A-Za-z0-9._-]*\\.whl")
  private def downloadSize(bytes: Long): String = {
    if (bytes >= 1024 * 1024) String.format(java.util.Locale.ROOT, "%.1fMB", Double.box(bytes.toDouble / (1024 * 1024)))
    else if (bytes >= 1024) s"${bytes / 1024}kB"
    else s"${bytes}B"
  }
  private def missing(x: HttpExchange): Unit = send(x, 404, "text/html; charset=utf-8", page("Not found", "<h1>Not found</h1><p>This resource is unavailable.</p>"))
  server.createContext("/", (x: HttpExchange) => {
    try {
      val path = x.getRequestURI.getPath
      if (x.getRequestMethod != "GET") { x.getResponseHeaders.set("Allow", "GET"); send(x, 405, "text/plain", "Method not allowed".getBytes(UTF_8)) }
      else if (path == "/") html(x, "Stream data. Collaborate across nodes.", new String(resource("/web/home.html").get, UTF_8))
      else if (path == "/docs" || path == "/docs/") html(x, "Documentation", new String(resource("/web/docs.html").get, UTF_8))
      else if (path == "/downloads") {
        val files = if (Files.isDirectory(downloadRoot)) { val s = Files.list(downloadRoot); try s.iterator().asScala.filter(p => Files.isRegularFile(p) && !Files.isSymbolicLink(p) && p.getFileName.toString.matches("[A-Za-z0-9._-]+") && softwareArtifact(p.getFileName.toString)).toVector.sortBy(_.getFileName.toString) finally s.close() } else Vector.empty
        val rows = files.map { p => val n = p.getFileName.toString; s"<tr><td><a href='/download/$n'>${escape(n)}</a></td><td>${downloadSize(Files.size(p))}</td></tr>" }.mkString
        html(x, "Downloads", "<p class='eyebrow'>SOFTWARE</p><h1>Choose your tools.</h1><p>Development snapshots for Linux servers and cross-platform clients. JDK 17+ for JVM software; Python 3.10+ for the Python SDK and shell. Operator execution needs Linux and Docker.</p><p>Download DACP Server, Union Server or the Python client with dacp-shell. Server archives include launch scripts and runtime libraries; pip installs client dependencies. Sizes use 1024-byte units: MB with one decimal place, kB as whole numbers.</p>" +
          (if (files.isEmpty) "<p>No artifacts have been published on this instance yet. <a href='https://github.com/rdcn-link/dacp-codex'>Build from source</a>.</p>" else "<table><thead><tr><th>Artifact</th><th>Size</th></tr></thead><tbody>" + rows + "</tbody></table>") +
          "<h2>Install and verify</h2><pre>sha256sum --ignore-missing -c SHA256SUMS\nunzip dacp-server-*.zip\npython -m pip install dacp_client-*.whl</pre><p>Download <a href='/download/SHA256SUMS'>SHA256SUMS</a> alongside the desired artifacts; use sha256sum --ignore-missing -c SHA256SUMS to verify your selected downloads. See the <a href='/guide/docs/administrators/README.md'>administrator guide</a> for certificates and configuration, and the <a href='/guide/docs/users/README.md'>user guide</a> for shell commands.</p>")
      }
      else if (path.startsWith("/download/")) {
        val n = path.stripPrefix("/download/")
        val file = downloadRoot.resolve(n).normalize()
        if (!n.matches("[A-Za-z0-9._-]+") || (!softwareArtifact(n) && n != "SHA256SUMS") || file.getParent != downloadRoot || !Files.isRegularFile(file) || Files.isSymbolicLink(file)) missing(x)
        else {
          x.getResponseHeaders.set("Content-Disposition", "attachment; filename=\"" + n + "\"")
          x.getResponseHeaders.set("Content-Type", "application/octet-stream")
          x.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
          x.sendResponseHeaders(200, Files.size(file))
          try Files.copy(file, x.getResponseBody) finally x.close()
        }
      }
      else if (path.startsWith("/guide/") && path.endsWith(".md") && !path.endsWith("_zh.md") && !path.contains("..")) {
        resource("/site/" + path.stripPrefix("/guide/")) match {
          case Some(bytes) =>
            val rendered = renderer.render(parser.parse(new String(bytes, UTF_8).replaceAll("\\[中文\\]\\([^)]*\\)\\s*(·\\s*)?", "")))
            // Relative Markdown links resolve under /guide/; source/config links point to GitHub.
            val links = "(href|src)=\"([^\"]+)\"".r.replaceAllIn(rendered, m => {
              val raw = m.group(2)
              val target = if (raw.startsWith("#") || raw.startsWith("https://") || raw.startsWith("http://")) raw
              else { val resolved = URI.create(path).resolve(raw).normalize().toString
                if (resolved.takeWhile(_ != '#').endsWith(".md")) resolved
                else "https://github.com/rdcn-link/dacp-codex/blob/main/" + resolved.stripPrefix("/guide/") }
              java.util.regex.Matcher.quoteReplacement(m.group(1) + "=\"" + target + "\"")
            })
            html(x, "Documentation", "<article>" + links + "</article>")
          case None => missing(x)
        }
      }
      else if (path == "/style.css") send(x, 200, "text/css", resource("/web/style.css").get)
      else if (Set("/assets/dacp-logo.png", "/assets/dacp-favicon.png", "/assets/dacp-favicon.ico").contains(path)) send(x, 200, if (path.endsWith(".ico")) "image/x-icon" else "image/png", resource("/site" + path).get)
      else if (path == "/favicon.ico") send(x, 200, "image/x-icon", resource("/site/assets/dacp-favicon.ico").get)
      else missing(x)
    } catch { case scala.util.control.NonFatal(e) => System.err.println("Website request failed: " + e.getClass.getSimpleName); try send(x, 500, "text/plain", "Internal server error".getBytes(UTF_8)) catch { case scala.util.control.NonFatal(_) => x.close() } }
  })
  def start(): Unit = server.start()
  def address: InetSocketAddress = server.getAddress
  override def close(): Unit = { server.stop(0); pool.shutdownNow() }
}
object Main {
  def main(args: Array[String]): Unit = {
    require(args.length % 2 == 0, "Usage: dacp-website [--host 0.0.0.0] [--port 8830] [--downloads /path/to/downloads]")
    val options = args.grouped(2).map(a => a(0) -> a(1)).toMap
    require(options.keySet.subsetOf(Set("--host", "--port", "--downloads")), "Unknown option")
    val site = new Website(options.getOrElse("--host", "0.0.0.0"), options.getOrElse("--port", "8830").toInt, Paths.get(options.getOrElse("--downloads", "downloads")))
    sys.addShutdownHook(site.close()); site.start(); println(s"DACP website listening on ${site.address}")
  }
}
