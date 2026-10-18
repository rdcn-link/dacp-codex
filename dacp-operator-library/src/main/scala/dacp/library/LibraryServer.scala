package dacp.library

import com.sun.net.httpserver.{HttpServer, HttpsServer, HttpsConfigurator, HttpExchange}
import java.net.InetSocketAddress
import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadPoolExecutor
import javax.net.ssl.SSLContext

final class LibraryServer(root: Path, host: String = "127.0.0.1", port: Int = 8820,
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
  private val route = "/v1/operators/([A-Za-z0-9_-][A-Za-z0-9_.-]*)/([A-Za-z0-9_-][A-Za-z0-9_.-]*)(/image[.]tar)?".r
  private def reply(exchange: HttpExchange, status: Int, text: String): Unit = {
    val bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(status, bytes.length)
    exchange.getResponseBody.write(bytes)
  }
  server.createContext("/", exchange => {
    try {
      if (exchange.getRequestMethod != "GET") {
        exchange.getResponseHeaders.set("Allow", "GET"); reply(exchange, 405, "method not allowed\n")
      } else if (exchange.getRequestURI.toString == "/health") reply(exchange, 200, "ok\n")
      else exchange.getRequestURI.toString match {
        case "/v1/operators" =>
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          reply(exchange, 200, Repository.mapper.writeValueAsString(Repository.catalog(catalog)))
        case route(name, version, archive) =>
          val file = catalog.resolve(name).resolve(version).resolve(if (archive == null) "manifest.json" else "image.tar")
          val real = try Some(file.toRealPath()) catch { case _: java.nio.file.NoSuchFileException => None }
          real match {
            case Some(path) if path.startsWith(catalog) && Files.isRegularFile(path) =>
              val in = Files.newInputStream(path)
              try {
                exchange.getResponseHeaders.set("Content-Type", if (archive == null) "application/json" else "application/octet-stream")
                exchange.sendResponseHeaders(200, Files.size(path))
                in.transferTo(exchange.getResponseBody)
              } finally in.close()
            case _ => reply(exchange, 404, "not found\n")
          }
        case _ => reply(exchange, 404, "not found\n")
      }
    } catch {
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
