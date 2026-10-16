package dacp

import java.nio.file.{Files,Path}
import java.net.InetSocketAddress
import com.sun.net.httpserver.HttpServer
import org.scalatest.funsuite.AnyFunSuite

class OperatorLibrarySpec extends AnyFunSuite {
  test("remote catalog failures and corrupt archives fail before touching Docker and never fall back locally") {
    val root = Files.createTempDirectory("operator-test-")
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    var mode = "missing"
    server.createContext("/", exchange => {
      val body = if (exchange.getRequestURI.getPath.endsWith("image.tar")) "corrupt" else
        s"""{"api_version":1,"name":"identity","version":"1.0","image":"sha256:${"a" * 64}","archive":{"size":7,"sha256":"${"b" * 64}"}}"""
      val bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(if(mode == "missing") 404 else 200,bytes.length)
      val out = exchange.getResponseBody; try out.write(bytes) finally out.close()
    })
    server.start()
    val config = Config("localhost",8815,"localhost",Map.empty,Seq.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"),
      operatorLibraryUrl=Some(s"http://127.0.0.1:${server.getAddress.getPort}"),operatorCache=root)
    try {
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"identity@1.0",null)).getMessage.contains("404"))
      mode = "corrupt"
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"identity@1.0",null)).getMessage.contains("checksum"))
      val files = Files.walk(root)
      try assert(!files.toArray.exists(_.toString.contains("download-"))) finally files.close()
      intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"../escape@1",null))
      val tooSmall = config.copy(operatorMaxBytes=2)
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(tooSmall,"identity@1.0",null)).getMessage.contains("size limit"))
    } finally {
      server.stop(0)
      val files = Files.walk(root)
      try files.toArray.map(_.asInstanceOf[Path]).sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally files.close()
    }
  }
}
