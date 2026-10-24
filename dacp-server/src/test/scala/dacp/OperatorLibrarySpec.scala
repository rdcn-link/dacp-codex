package dacp

import java.nio.file.{Files,Path}
import java.net.InetSocketAddress
import com.sun.net.httpserver.HttpServer
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class OperatorLibrarySpec extends AnyFunSuite {
  test("source tasks and operator contracts accept explicitly empty inputs only") {
    val source="""job: source
    |tasks:
    |- id: ticks
    |  operator: live_ticks@1.0
    |  inputs: {}
    |outputs:
    |  result: ticks
    |""".stripMargin
    assert(Jobs.parse(source).tasks.head.inputs.isEmpty)
    intercept[IllegalArgumentException](Jobs.parse(source.replace("  inputs: {}\n", "")))
    val empty=Data.mapper.readTree("{}")
    assert(OperatorPorts.read(empty).isEmpty)
    intercept[IllegalArgumentException](OperatorPorts.read(Data.mapper.readTree("[]")))
    intercept[IllegalArgumentException](OperatorPorts.read(Data.mapper.readTree("null")))
  }
  private val image = "sha256:" + "a" * 64
  private val archive = "shared runtime".getBytes(java.nio.charset.StandardCharsets.UTF_8)
  private val code = "small operator bundle".getBytes(java.nio.charset.StandardCharsets.UTF_8)
  private def hash(bytes: Array[Byte]): String = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
  private def proxy[T](kind: Class[T])(f: java.lang.reflect.Method => AnyRef): T =
    java.lang.reflect.Proxy.newProxyInstance(kind.getClassLoader,Array[Class[_]](kind),
      new java.lang.reflect.InvocationHandler {
        def invoke(p: AnyRef, m: java.lang.reflect.Method, args: Array[AnyRef]): AnyRef = f(m)
      }).asInstanceOf[T]
  private def manifest(name: String, version: String = "1", executor: String = "jvm"): String = {
    val node = Data.mapper.createObjectNode().put("api_version",2).put("name",name).put("version",version)
    node.putObject("runtime").put("image",image).putObject("archive").put("size",archive.length).put("sha256",hash(archive)).put("filename","runtime.tar")
    node.putObject("execution").put("executor",executor).put("entrypoint",if(executor == "container") "" else "example.Main")
    if(executor != "container") node.putObject("artifact").put("size",code.length).put("sha256",hash(code)).put("filename","operators.jar")
    Seq("inputs"->"source","outputs"->"result").foreach {case(key,port)=>
      val p=node.putObject(key).putObject(port);p.putArray("representations").add("bytes");p.putArray("formats")
    }
    // Review evidence is deliberately unavailable: execution must never fetch it.
    node.putObject("provenance").putObject("source_archive").put("sha256","b"*64).put("size",100).put("filename","sources.zip")
    node.toString
  }
  private def config(root: Path, base: String) = Config("localhost",8815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"),
    localOperatorLibrary=root.resolve("local"),operatorCache=root.resolve("cache"),remoteOperatorLibraryUrls=Seq(base))
  private def cleanup(root: Path): Unit = { val paths = Files.walk(root)
    try paths.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally paths.close() }

  test("remote and cached command definitions preserve executor configuration and port contracts") {
    val root=Files.createTempDirectory("operator-command-")
    val inspect=proxy(classOf[com.github.dockerjava.api.command.InspectImageCmd]) { _ =>
      new com.github.dockerjava.api.command.InspectImageResponse().withId(image)
    }
    val docker=proxy(classOf[com.github.dockerjava.api.DockerClient]) { m =>
      if(m.getName=="inspectImageCmd") inspect else throw new AssertionError(m.getName)
    }
    val node=Data.mapper.readTree(manifest("native",executor="command"))
    val execution=node.path("execution").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    execution.put("entrypoint","/operator/program")
    execution.set[com.fasterxml.jackson.databind.JsonNode]("config",Data.mapper.readTree(
      """{"args":["--literal","$(false)"],"stdin":"source","stdout":{"port":"result","representation":"bytes","format":"text"}}"""))
    node.path("artifact").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("filename","program")
    val server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    server.createContext("/",exchange=>{
      val path=exchange.getRequestURI.getPath
      val bytes=if(path.endsWith(hash(archive)))archive else if(path.endsWith(hash(code)))code else node.toString.getBytes
      exchange.sendResponseHeaders(200,bytes.length)
      try exchange.getResponseBody.write(bytes) finally exchange.close()
    });server.start()
    val c=config(root,s"http://127.0.0.1:${server.getAddress.getPort}")
    try {
      val resolved=OperatorLibrary.resolve(c,"native@1",docker)
      val payload=Data.mapper.readTree(resolved.command.entrypoint.get.last)
      assert(payload.path("argv").get(2).asText=="$(false)")
      assert(payload.path("artifact").asBoolean)
      assert(resolved.inputs.keySet==Set("source") && resolved.outputs.keySet==Set("result"))
      server.stop(0)
      assert(OperatorLibrary.resolve(c,"native@1",docker).command==resolved.command)
    } finally {server.stop(0);cleanup(root)}
  }
  test("local definitions win; distinct remote operators share runtime/code downloads and work offline without fetching review evidence") {
    val root = Files.createTempDirectory("operator-shared-")
    var requests = Vector.empty[String]; var loads = 0; var installed = false
    val inspect = proxy(classOf[com.github.dockerjava.api.command.InspectImageCmd]) { _ =>
      if(!installed) throw new com.github.dockerjava.api.exception.NotFoundException("missing")
      new com.github.dockerjava.api.command.InspectImageResponse().withId(image)
    }
    val load = proxy(classOf[com.github.dockerjava.api.command.LoadImageCmd]) { _ => loads += 1; installed = true; null }
    val docker = proxy(classOf[com.github.dockerjava.api.DockerClient]) { m =>
      if(m.getName == "inspectImageCmd") inspect else if(m.getName == "loadImageCmd") load else throw new AssertionError(m.getName)
    }
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    server.createContext("/", exchange => {
      val path = exchange.getRequestURI.getPath; requests :+= path
      val bytes = if(path.endsWith(hash(archive))) archive else if(path.endsWith(hash(code))) code
        else manifest(path.split("/").dropRight(1).last).getBytes(java.nio.charset.StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(200,bytes.length)
      try exchange.getResponseBody.write(bytes) finally exchange.close()
    }); server.start()
    val c = config(root,s"http://127.0.0.1:${server.getAddress.getPort}")
    try {
      installed = true
      val local = c.localOperatorLibrary.resolve("first/1/operator.yaml")
      Files.createDirectories(local.getParent); Files.writeString(local,"name: first\nversion: '1'\nimage: local\nprovenance: {source_archive: missing-sources.zip, license: MIT}\ninputs: {source: {representations: [bytes], formats: []}}\noutputs: {result: {representations: [bytes], formats: []}}\n")
      assert(OperatorLibrary.resolve(c,"first@1",docker).image == image && requests.isEmpty)
      Files.delete(local); installed = false
      val first = OperatorLibrary.resolve(c,"first@1",docker)
      val second = OperatorLibrary.resolve(c,"second@1",docker)
      assert(first.artifact == second.artifact)
      assert(first.command.entrypoint.get.contains("example.Main"))
      assert(requests.count(_.contains("/blobs/")) == 2 && loads == 1)
      assert(requests.size == 4)
      // A second repository URL has separate metadata but shares verified content.
      val mirror = c.copy(remoteOperatorLibraryUrls=Seq(c.remoteOperatorLibraryUrls.head + "/mirror"))
      assert(OperatorLibrary.resolve(mirror,"second@1",docker).artifact == first.artifact)
      assert(requests.size == 5 && requests.count(_.contains("/blobs/")) == 2)
      server.stop(0)
      assert(OperatorLibrary.resolve(c,"second@1",docker).artifact == first.artifact)
      assert(requests.size == 5)
      // Completed caches remain verified, even offline.
      Files.writeString(first.artifact.get,"corrupt")
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(c,"first@1",docker)).getMessage.contains("checksum"))
    } finally { server.stop(0); cleanup(root) }
  }
  test("missing libraries are searched in order; corrupt artifacts fail before Docker and partial files are removed") {
    val root = Files.createTempDirectory("operator-corrupt-")
    var blobRequests = 0
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
    server.createContext("/", exchange => {
      val path = exchange.getRequestURI.getPath
      if(path.startsWith("/missing/")) { exchange.sendResponseHeaders(404,-1); exchange.close() }
      else {
        val bytes = if(path.contains("/blobs/")) { blobRequests += 1; "bad".getBytes } else manifest("identity").getBytes
        exchange.sendResponseHeaders(200,bytes.length)
        try exchange.getResponseBody.write(bytes) finally exchange.close()
      }
    }); server.start()
    val base = s"http://127.0.0.1:${server.getAddress.getPort}"
    val c = config(root,base).copy(remoteOperatorLibraryUrls=Seq(base+"/missing",base))
    try {
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(c,"identity@1",null)).getMessage.contains("checksum"))
      assert(blobRequests == 1)
      val paths = Files.walk(root)
      try assert(!paths.iterator.asScala.exists(_.getFileName.toString.startsWith("download-"))) finally paths.close()
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(c.copy(remoteOperatorLibraryUrls=Seq(base+"/missing")),"identity@1",null)).getMessage.contains("not found"))
      intercept[IllegalArgumentException](OperatorLibrary.resolve(c,"../escape@1",null))
      intercept[IllegalArgumentException](c.copy(remoteOperatorLibraryUrls=Seq("file:///tmp")))
      assert(intercept[IllegalArgumentException](OperatorLibrary.resolve(c.copy(operatorMaxBytes=2),"identity@1",null)).getMessage.contains("size"))
    } finally { server.stop(0); cleanup(root) }
  }
}
