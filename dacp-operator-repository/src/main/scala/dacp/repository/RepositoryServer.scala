package dacp.repository

import com.sun.net.httpserver.{HttpServer, HttpsServer, HttpsConfigurator, HttpExchange}
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ThreadPoolExecutor
import javax.net.ssl.SSLContext
import scala.jdk.CollectionConverters._

final class RepositoryServer(root: Path, host: String = "127.0.0.1", port: Int = 8820,
                             tls: Option[SSLContext] = None) extends AutoCloseable {
  Files.createDirectories(root)
  private val catalog = root.toRealPath()
  private val server: HttpServer = tls match {
    case Some(context) =>
      val https = HttpsServer.create(new InetSocketAddress(host, port), 64)
      https.setHttpsConfigurator(new HttpsConfigurator(context)); https
    case None => HttpServer.create(new InetSocketAddress(host, port), 64)
  }
  private val executor = new ThreadPoolExecutor(8, 8, 0L, java.util.concurrent.TimeUnit.SECONDS,
    new java.util.concurrent.ArrayBlockingQueue[Runnable](64), new ThreadPoolExecutor.CallerRunsPolicy())
  server.setExecutor(executor)
  private val part = "([A-Za-z0-9_-][A-Za-z0-9_.-]*)"
  private val artifactRoute = (s"/v1/operators/$part/$part" + "(/image[.]tar)?").r
  private val detailRoute = s"/operators/$part/$part".r
  private val css = {
    val in = getClass.getResourceAsStream("/web/repository.css")
    require(in != null, "missing repository stylesheet")
    try new String(in.readAllBytes(), UTF_8) finally in.close()
  }
  private def headers(exchange: HttpExchange, status: Int, size: Long, contentType: String): Unit = {
    exchange.getResponseHeaders.set("Content-Type", contentType)
    exchange.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
    exchange.getResponseHeaders.set("Referrer-Policy", "no-referrer")
    exchange.getResponseHeaders.set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
    if (exchange.getRequestMethod == "HEAD") {
      exchange.getResponseHeaders.set("Content-Length", size.toString)
      exchange.sendResponseHeaders(status, -1)
    } else exchange.sendResponseHeaders(status, size)
  }
  private def reply(exchange: HttpExchange, status: Int, text: String, contentType: String = "text/plain; charset=utf-8"): Unit = {
    val bytes = text.getBytes(UTF_8)
    headers(exchange, status, bytes.length, contentType)
    if (exchange.getRequestMethod != "HEAD") exchange.getResponseBody.write(bytes)
  }
  private def html(exchange: HttpExchange, status: Int, page: String): Unit = reply(exchange, status, page, "text/html; charset=utf-8")
  private def search(rawQuery: String): String = {
    if (rawQuery == null || rawQuery.isEmpty) ""
    else {
      require(rawQuery.length <= 2048, "search query is too long")
      val fields = rawQuery.split("&", -1).map { field =>
        val values = field.split("=", 2)
        URLDecoder.decode(values(0), UTF_8) -> (if (values.length == 2) URLDecoder.decode(values(1), UTF_8) else "")
      }
      require(fields.length == 1 && fields(0)._1 == "q", "expected a single q search parameter")
      val query = fields(0)._2.trim
      require(query.length <= 200, "search query is too long")
      query
    }
  }
  server.createContext("/", exchange => {
    try {
      val uri = exchange.getRequestURI
      if (!Set("GET", "HEAD").contains(exchange.getRequestMethod)) {
        exchange.getResponseHeaders.set("Allow", "GET, HEAD"); reply(exchange, 405, "method not allowed\n")
      } else if (uri.getRawPath == "/") {
        val query = search(uri.getRawQuery)
        html(exchange, 200, WebPages.index(Repository.catalog(catalog).elements.asScala.toSeq, query))
      } else if (uri.getRawQuery != null) reply(exchange, 404, "not found\n")
      else uri.getRawPath match {
        case "/health" => reply(exchange, 200, "ok\n")
        case "/assets/repository.css" => reply(exchange, 200, css, "text/css; charset=utf-8")
        case "/v1/operators" =>
          reply(exchange, 200, Repository.mapper.writeValueAsString(Repository.catalog(catalog)), "application/json")
        case detailRoute(name, version) =>
          val versions = Repository.catalog(catalog).elements.asScala.filter(_.path("name").asText == name).toSeq
          versions.find(_.path("version").asText == version) match {
            case Some(item) => html(exchange, 200, WebPages.detail(item, versions))
            case None => html(exchange, 404, WebPages.notFound)
          }
        case artifactRoute(name, version, archive) =>
          val file = catalog.resolve(name).resolve(version).resolve(if (archive == null) "manifest.json" else "image.tar")
          val real = try Some(file.toRealPath()) catch { case _: java.nio.file.NoSuchFileException => None }
          real match {
            case Some(path) if path.startsWith(catalog) && Files.isRegularFile(path) =>
              if (archive != null) exchange.getResponseHeaders.set("Content-Disposition", s"attachment; filename=\"$name-$version.tar\"")
              val in = Files.newInputStream(path)
              try {
                headers(exchange, 200, Files.size(path), if (archive == null) "application/json" else "application/octet-stream")
                if (exchange.getRequestMethod != "HEAD") in.transferTo(exchange.getResponseBody)
              } finally in.close()
            case _ => reply(exchange, 404, "not found\n")
          }
        case _ => html(exchange, 404, WebPages.notFound)
      }
    } catch {
      case error: IllegalArgumentException => if (exchange.getResponseCode == -1) reply(exchange, 400, error.getMessage + "\n")
      case _: java.io.IOException => () // Includes clients disconnecting while downloading.
      case scala.util.control.NonFatal(error) =>
        System.err.println(s"operator repository request failed: ${error.getMessage}")
        if (exchange.getResponseCode == -1) reply(exchange, 500, "internal error\n")
    } finally exchange.close()
  })
  def start(): Unit = server.start()
  def boundPort: Int = server.getAddress.getPort
  def close(): Unit = { server.stop(0); executor.shutdownNow() }
}
