package dacp.repository


import com.sun.net.httpserver.{HttpServer, HttpsServer, HttpsConfigurator, HttpExchange}
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ThreadPoolExecutor
import javax.net.ssl.SSLContext
import scala.jdk.CollectionConverters._

final class RepositoryServer(root: Path, host: String = "127.0.0.1", port: Int = 8820,
                             tls: Option[SSLContext] = None, adminPassword: Option[String] = None,
                             publishMaxBytes: Long = 8589934592L, private[repository] val buildEngine: SourceBuildEngine = new DockerSourceBuild()) extends AutoCloseable {
  Files.createDirectories(root)
  private val catalog = root.toRealPath()
  private val administration=new Administration(catalog,adminPassword)
  private val server: HttpServer = try {tls match {
    case Some(context) =>
      val https = HttpsServer.create(new InetSocketAddress(host, port), 64)
      https.setHttpsConfigurator(new HttpsConfigurator(context)); https
    case None => HttpServer.create(new InetSocketAddress(host, port), 64)
  }
  }catch {case scala.util.control.NonFatal(e)=>administration.close();throw e}
  private val pipeline=new SourcePipeline(catalog,administration,buildEngine)
  private val adminApi=new AdminApi(catalog,administration,tls.isDefined)
  private val publishing = new Publishing(catalog,administration,pipeline,adminApi,publishMaxBytes)
  def bootstrapPasswordPath:Path=administration.bootstrapPasswordPath
  private def resource(name:String):String={val in=getClass.getResourceAsStream("/web/"+name);try new String(in.readAllBytes(),UTF_8)finally in.close()}
  private val executor = new ThreadPoolExecutor(8, 8, 0L, java.util.concurrent.TimeUnit.SECONDS,
    new java.util.concurrent.ArrayBlockingQueue[Runnable](64), new ThreadPoolExecutor.CallerRunsPolicy())
  server.setExecutor(executor)
  private val part = "([A-Za-z0-9_-][A-Za-z0-9_.-]*)"
  private val artifactRoute = s"/v1/operators/$part/$part".r
  private val blobRoute = s"/v1/blobs/sha256/([a-f0-9]{64})(?:/$part)?".r
  private val detailRoute = s"/operators/$part/$part".r
  private val css = {
    val in = getClass.getResourceAsStream("/web/repository.css")
    require(in != null, "missing repository stylesheet")
    try new String(in.readAllBytes(), UTF_8) finally in.close()
  }
  private val logo = {
    val in = getClass.getResourceAsStream("/web/dacp-logo.png")
    require(in != null, "missing DACP logo")
    try in.readAllBytes() finally in.close()
  }
  private val favicons=Seq("/assets/dacp-favicon.png"->("dacp-favicon.png","image/png"),"/favicon.ico"->("dacp-favicon.ico","image/x-icon")).map {case(path,(file,kind))=>
    val in=getClass.getResourceAsStream("/web/"+file)
    require(in!=null,"missing favicon")
    try path->(in.readAllBytes(),kind)finally in.close()
  }.toMap
  private def headers(exchange: HttpExchange, status: Int, size: Long, contentType: String): Unit = {
    exchange.getResponseHeaders.set("Content-Type", contentType)
    exchange.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
    exchange.getResponseHeaders.set("Referrer-Policy", "no-referrer")
    exchange.getResponseHeaders.set("Content-Security-Policy", "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
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
      if (publishing.handle(exchange) || adminApi.handle(exchange)) ()
      else if (!Set("GET", "HEAD").contains(exchange.getRequestMethod)) {
        exchange.getResponseHeaders.set("Allow", "GET, HEAD"); reply(exchange, 405, "method not allowed\n")
      } else if (uri.getRawPath == "/") {
        val query = search(uri.getRawQuery)
        html(exchange, 200, WebPages.index(administration.publicCatalog.elements.asScala.toSeq, query))
      } else if (uri.getRawQuery != null) reply(exchange, 404, "not found\n")
      else uri.getRawPath match {
        case "/admin" => html(exchange,200,resource("admin.html"))
        case "/assets/admin.js" => reply(exchange,200,resource("admin.js"),"text/javascript; charset=utf-8")
        case "/runtimes" => html(exchange,200,WebPages.runtimes(administration.runtimes))
        case "/v1/build-languages" => reply(exchange,200,Repository.mapper.writeValueAsString(administration.buildLanguages),"application/json")
        case "/v1/runtimes" => reply(exchange,200,Repository.mapper.writeValueAsString(administration.runtimes),"application/json")
        case "/v1/categories" => reply(exchange,200,Repository.mapper.writeValueAsString(administration.categories),"application/json")
        case artifactRoute(name,version) =>
          administration.publicCatalog.elements.asScala.find(n=>n.path("name").asText==name && n.path("version").asText==version) match {
            case Some(item)=>reply(exchange,200,Repository.mapper.writeValueAsString(item),"application/json")
            case None=>reply(exchange,404,"not found\n")
          }
        case "/health" => reply(exchange, 200, "ok\n")
        case route if favicons.contains(route) =>
          val (bytes,kind)=favicons(route)
          headers(exchange,200,bytes.length,kind)
          if(exchange.getRequestMethod!="HEAD")exchange.getResponseBody.write(bytes)
        case "/assets/dacp-logo.png" =>
          headers(exchange, 200, logo.length, "image/png")
          if (exchange.getRequestMethod != "HEAD") exchange.getResponseBody.write(logo)
        case "/assets/repository.css" => reply(exchange, 200, css, "text/css; charset=utf-8")
        case "/v1/operators" =>
          reply(exchange, 200, Repository.mapper.writeValueAsString(administration.publicCatalog), "application/json")
        case detailRoute(name, version) =>
          val versions = administration.publicCatalog.elements.asScala.filter(_.path("name").asText == name).toSeq
          versions.find(_.path("version").asText == version) match {
            case Some(item) => html(exchange, 200, WebPages.detail(item, versions, administration.runtimes.elements.asScala.toVector))
            case None => html(exchange, 404, WebPages.notFound)
          }
        case route if blobRoute.pattern.matcher(route).matches() =>
          val (file, archive, filename) = route match {
            case artifactRoute(name, version) => (catalog.resolve(name).resolve(version).resolve("manifest.json"), false, None)
            case blobRoute(hash, name) => (Repository.blob(catalog, hash), true, Option(name))
          }
          val real = try Some(file.toRealPath()) catch { case _: java.nio.file.NoSuchFileException => None }
          real match {
            case Some(path) if path.startsWith(catalog) && Files.isRegularFile(path) && administration.publicBlob(file.getFileName.toString) =>
              if (archive) exchange.getResponseHeaders.set("Content-Disposition", s"attachment; filename=\"${filename.getOrElse(file.getFileName.toString)}\"")
              val in = Files.newInputStream(path)
              try {
                headers(exchange, 200, Files.size(path), if (!archive) "application/json" else "application/octet-stream")
                if (exchange.getRequestMethod != "HEAD") in.transferTo(exchange.getResponseBody)
              } finally in.close()
            case _ => reply(exchange, 404, "not found\n")
          }
        case _ => html(exchange, 404, WebPages.notFound)
      }
    } catch {
      case error: PublishFailure =>
        if(error.status == 401) exchange.getResponseHeaders.set("WWW-Authenticate", "Bearer")
        if(exchange.getResponseCode == -1) reply(exchange, error.status, error.detail + "\n")
      case _: com.fasterxml.jackson.core.JsonProcessingException => if(exchange.getResponseCode == -1) reply(exchange, 400, "invalid JSON manifest\n")
      case error: IllegalArgumentException => if (exchange.getResponseCode == -1) reply(exchange, 400, error.getMessage + "\n")
      case _: java.io.IOException => () // Includes clients disconnecting while downloading.
      case scala.util.control.NonFatal(error) =>
        System.err.println(s"operator repository request failed: ${error.getMessage}")
        if (exchange.getResponseCode == -1) reply(exchange, 500, "internal error\n")
    } finally exchange.close()
  })
  def start(): Unit = server.start()
  def boundPort: Int = server.getAddress.getPort
  private val closed=new java.util.concurrent.atomic.AtomicBoolean(false)
  def close(): Unit = if(closed.compareAndSet(false,true)){ server.stop(0); executor.shutdownNow();pipeline.close();administration.close() }
}
