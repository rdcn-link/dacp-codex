package dacp

import java.nio.file.{Files,Path}
import java.net.InetSocketAddress
import com.sun.net.httpserver.HttpServer
import org.scalatest.funsuite.AnyFunSuite

class OperatorLibrarySpec extends AnyFunSuite {
  test("local images take precedence and downloaded operators are reused offline") {
    val root = Files.createTempDirectory("operator-success-")
    val image = "sha256:" + "a" * 64
    val bytes = "image archive".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val checksum = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
    var requests = 0
    var loads = 0
    def proxy[T](kind: Class[T])(f: java.lang.reflect.Method => AnyRef): T =
      java.lang.reflect.Proxy.newProxyInstance(kind.getClassLoader,Array[Class[_]](kind),
        new java.lang.reflect.InvocationHandler {
          def invoke(p: AnyRef, m: java.lang.reflect.Method, args: Array[AnyRef]): AnyRef = f(m)
        }).asInstanceOf[T]
    val inspect = proxy(classOf[com.github.dockerjava.api.command.InspectImageCmd]) { _ =>
      new com.github.dockerjava.api.command.InspectImageResponse().withId(image)
    }
    val load = proxy(classOf[com.github.dockerjava.api.command.LoadImageCmd]) { _ => loads += 1; null }
    val docker = proxy(classOf[com.github.dockerjava.api.DockerClient]) { m =>
      if (m.getName == "inspectImageCmd") inspect else if (m.getName == "loadImageCmd") load
      else throw new AssertionError(m.getName)
    }
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    server.createContext("/", exchange => {
      requests += 1
      val body = if(exchange.getRequestURI.getPath.endsWith("image.tar")) bytes else
        s"""{"api_version":1,"name":"identity","version":"1.0","image":"$image","archive":{"size":${bytes.length},"sha256":"$checksum"}}""".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(200,body.length)
      val out = exchange.getResponseBody; try out.write(body) finally out.close()
    })
    server.start()
    val config = Config("localhost",8815,"localhost",Map.empty,Seq.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"),
      localOperatorLibrary=root.resolve("local"),operatorCache=root.resolve("cache"),
      remoteOperatorLibraryUrls=Seq(s"http://127.0.0.1:${server.getAddress.getPort}"))
    try {
      val local = config.localOperatorLibrary.resolve("identity/1.0/operator.yaml")
      Files.createDirectories(local.getParent); Files.writeString(local,"image: local-image")
      assert(OperatorLibrary.resolve(config,"identity@1.0",docker) == image)
      assert(requests == 0)
      Files.delete(local)
      assert(OperatorLibrary.resolve(config,"identity@1.0",docker) == image)
      assert(requests == 2 && loads == 1)
      server.stop(0)
      assert(OperatorLibrary.resolve(config,"identity@1.0",docker) == image)
      assert(requests == 2 && loads == 1)
    } finally {
      server.stop(0)
      val files = Files.walk(root)
      try files.toArray.map(_.asInstanceOf[Path]).sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally files.close()
    }
  }
  test("remote libraries are searched in order and corrupt archives fail before touching Docker") {
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
      remoteOperatorLibraryUrls=Seq(s"http://127.0.0.1:${server.getAddress.getPort}"),operatorCache=root,localOperatorLibrary=root.resolve("local"))
    try {
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"identity@1.0",null)).getMessage.contains("operator not found"))
      val missing = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
      var misses = 0
      missing.createContext("/", exchange => {
        misses += 1
        exchange.sendResponseHeaders(404,-1); exchange.close()
      })
      missing.start()
      mode = "corrupt"
      try {
        val ordered = config.copy(remoteOperatorLibraryUrls=Seq(s"http://127.0.0.1:${missing.getAddress.getPort}") ++ config.remoteOperatorLibraryUrls)
        assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(ordered,"identity@1.0",null)).getMessage.contains("checksum"))
        assert(misses == 1)
      } finally missing.stop(0)
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"identity@1.0",null)).getMessage.contains("checksum"))
      val files = Files.walk(root)
      try assert(!files.toArray.exists(_.toString.contains("download-"))) finally files.close()
      intercept[IllegalArgumentException](OperatorLibrary.resolve(config,"../escape@1",null))
      intercept[IllegalArgumentException](config.copy(remoteOperatorLibraryUrls=Seq("file:///tmp/operators")))
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(config.copy(remoteOperatorLibraryUrls=Seq.empty),"identity@1.0",null)).getMessage.contains("operator not found"))
      val local = config.localOperatorLibrary.resolve("identity/1.0")
      Files.createDirectories(local)
      Files.writeString(local.resolve("operator.yaml"), "invalid: [")
      intercept[org.yaml.snakeyaml.error.YAMLException](OperatorLibrary.resolve(config,"identity@1.0",null))
      Files.delete(local.resolve("operator.yaml"))
      val tooSmall = config.copy(operatorMaxBytes=2)
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(tooSmall,"identity@1.0",null)).getMessage.contains("size limit"))
    } finally {
      server.stop(0)
      val files = Files.walk(root)
      try files.toArray.map(_.asInstanceOf[Path]).sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally files.close()
    }
  }
}
