package dacp

import java.nio.file.{Files,Path}
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import scala.sys.process._
import scala.jdk.CollectionConverters._

class DacpServerSpec extends AnyFunSuite {
  private def fixture(run:(Path,Path,Path)=>Unit):Unit={
    val dir=Files.createTempDirectory("embedded-dacp-");val cert=dir.resolve("cert.pem");val key=dir.resolve("key.pem")
    try {
      val status=Process(Seq("openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-subj","/CN=localhost","-addext","subjectAltName=DNS:localhost","-keyout",key.toString,"-out",cert.toString)).!(ProcessLogger(_=>(),_=>()))
      assert(status==0);run(dir,cert,key)
    }finally Data.deleteTree(dir)
  }
  private def config(dir:Path,cert:Path,key:Path):Config={
    val socket=new ServerSocket(0);val port=try socket.getLocalPort finally socket.close()
    Config("localhost",port,"localhost",Map.empty,cert,key,cert,s3=S3Settings(port=Some(0)),web=NodeWebSettings(port=Some(0)),executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->(dir.resolve("history")).toString))))
  }
  private def catalog(counter:AtomicInteger)=new DatasetCatalog {
    def list():Seq[DatasetInfo]=Seq(DatasetInfo("public"))
    def open(name:String):PublishedDataset=throw new java.io.FileNotFoundException("no items")
    override def close():Unit={counter.incrementAndGet();()}
  }
  test("embedded mTLS server starts without blocking; discovery works and closing releases catalog and port") {fixture{(dir,cert,key)=>
    val count=new AtomicInteger();val c=config(dir,cert,key)
    val server=DacpServer.builder().config(c).catalog(catalog(count)).build()
    server.start()
    val allocator=new RootAllocator()
    val caIn=Files.newInputStream(cert);val certIn=Files.newInputStream(cert);val keyIn=Files.newInputStream(key)
    val token=c.credentials.create("alice")._2
    val client=try dacp.union.ClientTransport.open(allocator,c.uri(""),Some(cert),None,None,()=>Some(token))
      finally {caIn.close();certIn.close();keyIn.close()}
    try assert(client.listFlights(Criteria.ALL).asScala.map(_.getDescriptor.getPath.get(1)).toSeq==Seq("public"))
    finally {client.close();allocator.close();server.close()}
    server.close();assert(count.get()==1)
    val socket=new ServerSocket(c.port);socket.close()
    intercept[IllegalArgumentException](server.start())
  }}
  test("startup failure and close before start release owned resources exactly once") {fixture{(dir,cert,key)=>
    val count=new AtomicInteger();val bad=config(dir,cert,key).copy(cert=dir.resolve("missing.pem"))
    val server=DacpServer.builder().config(bad).catalog(catalog(count)).build()
    intercept[Exception](server.start());server.close();assert(count.get()==1)
    val before=new AtomicInteger()
    val never=DacpServer.builder().config(config(dir,cert,key)).catalog(catalog(before)).build()
    never.close();never.close();assert(before.get()==1)
  }}
  test("catalog shutdown failure still releases the execution history lock and listening port") {fixture{(dir,cert,key)=>
    val c=config(dir,cert,key)
    val cat=new DatasetCatalog {
      def list():Seq[DatasetInfo]=Seq.empty
      def open(name:String):PublishedDataset=throw new java.io.FileNotFoundException()
      override def close():Unit=throw new IllegalStateException("catalog close failed")
    }
    val server=DacpServer.builder().config(c).catalog(cat).build().start()
    intercept[IllegalStateException](server.close());server.close()
    val history=new ExecutionHistory(new FileExecutionStore(dir.resolve("history"),false));history.close()
    val socket=new ServerSocket(c.port);socket.close()
  }}
  test("configFile embedding overrides configured catalog without instantiating its provider") {fixture{(dir,cert,key)=>
    val count=new AtomicInteger();val c=config(dir,cert,key);val yaml=dir.resolve("server.yaml")
    Files.writeString(yaml,s"""host: localhost
public_host: localhost
port: ${c.port}
tls_cert: $cert
tls_key: $key
tls_ca: $cert
execution_store: {provider: file, settings: {directory: '${dir.resolve("history")}'}}
dataset_catalog: {provider: does-not-exist}
s3: {host: localhost, port: 0}
web: {host: localhost, port: 0}
""")
    val builder=DacpServer.builder().configFile(yaml).catalog(catalog(count))
    val server=builder.build()
    try {server.start();assert(server.s3Port.isDefined)}finally server.close()
    assert(count.get()==1)
    intercept[IllegalArgumentException](builder.build())
  }}
  private def http(port:Int,path:String,method:String="GET")={
    val request=java.net.http.HttpRequest.newBuilder(java.net.URI.create(s"http://localhost:$port$path")).method(method,java.net.http.HttpRequest.BodyPublishers.noBody()).build()
    java.net.http.HttpClient.newHttpClient().send(request,java.net.http.HttpResponse.BodyHandlers.ofString())
  }
  test("node website is public and read-only, publishes metadata and renders local DAG history") {fixture{(dir,cert,key)=>
    val count=new AtomicInteger();val c=config(dir,cert,key).copy(web=NodeWebSettings(port=Some(0),publicHost=Some("node.example.org")))
    val server=DacpServer.builder().config(c).catalog(catalog(count)).build().start();val port=server.webPort.get
    try {
      assert(http(port,"/").body().contains("Node overview"))
      assert(http(port,"/dag.js").body().contains("DacpDag"))
      assert(http(port,"/vendor/g6-5.1.1.min.js").statusCode()==200)
      assert(http(port,"/assets/dacp-logo.png").statusCode()==200)
      val info=dacp.union.Json.mapper.readTree(http(port,"/api/overview").body())
      assert(info.path("web_url").asText==s"http://node.example.org:$port/" && c.runningWebUrl.contains(s"http://node.example.org:$port/"))
      assert(info.path("started_at_ms").asLong>0 && info.path("uptime_ms").asLong>=0)
      val datasets=dacp.union.Json.mapper.readTree(http(port,"/api/datasets").body())
      val entry=datasets.path("datasets").get(0)
      assert(entry.path("name").asText=="public" && entry.path("description").asText=="{}")
      assert(!entry.has("title") && !entry.has("tags")) // Catalog.open deliberately throws.
      assert(http(port,"/api/overview","POST").statusCode()==405)
      assert(http(port,"/api/history?limit=0").statusCode()==400)
      assert(http(port,"/api/history?limit=1&limit=2").statusCode()==400)
      val metadata=dacp.union.Json.obj().put("execution_id","web-example")
      val definitions=dacp.union.Json.mapper.readTree("""[{"id":"copy","title":"复制记录","operator":"identity@1.0","inputs":{"source":{"uri":"dacp://localhost/data/source","view":"bytes"}},"outputs":{"result":"client"}},{"id":"finish","operator":"identity@1.0","inputs":{"source":{"task":"copy","port":"result"}},"outputs":{"result":"client"}}]""")
      metadata.set[com.fasterxml.jackson.databind.JsonNode]("task_definitions",definitions)
      val record=c.executionHistory.begin("union_execution",metadata=metadata)
      val state=dacp.union.Json.obj();state.putObject("tasks").put("copy","succeeded").put("finish","running")
      state.putObject("ports").putObject("copy/outputs/result").put("payload_bytes",1024)
      record.update("running",state)
      val response=http(port,"/api/executions/"+record.id)
      assert(response.headers().firstValue("Cache-Control").get()=="no-store")
      val graph=dacp.union.Json.mapper.readTree(response.body())
      assert(graph.path("graph_tasks").get(0).path("title").asText=="复制记录")
      assert(graph.path("graph_tasks").get(1).path("state").asText=="running")
      assert(graph.path("graph_tasks").get(1).path("inputs").get(0).path("task").asText=="copy")
      assert(graph.path("graph_nodes").path("local").path("ports").path("copy/outputs/result").path("payload_bytes").asLong==1024)
      assert(!graph.path("graph_outputs").has("copy.result") && graph.path("graph_outputs").has("finish.result"))
    }finally server.close()
    val socket=new ServerSocket(port);socket.close()
  }}
  test("node HTTP can be disabled and a bind failure releases Flight and history") {fixture{(dir,cert,key)=>
    val c=config(dir,cert,key).copy(web=NodeWebSettings(enabled=false))
    val server=DacpServer.builder().config(c).build().start()
    assert(server.webPort.isEmpty && c.runningWebUrl.isEmpty);server.close()
    val blocked=new ServerSocket(0)
    val bad=config(dir,cert,key).copy(web=NodeWebSettings(port=Some(blocked.getLocalPort)))
    val failed=DacpServer.builder().config(bad).build()
    try intercept[java.net.BindException](failed.start())finally blocked.close()
    failed.close();val socket=new ServerSocket(bad.port);socket.close()
    val store=new FileExecutionStore(dir.resolve("history"),false);store.close()
  }}
  test("web configuration has strict fields and types") {
    assert(NodeWebSettings.read(None).enabled)
    for(text<-Seq("{enabled: 'false'}","{port: '9000'}","{unexpected: true}")) {
      val raw=new org.yaml.snakeyaml.Yaml().load[Object](text)
      intercept[IllegalArgumentException](NodeWebSettings.read(Some(raw)))
    }
    val raw=new org.yaml.snakeyaml.Yaml().load[Object]("{enabled: false, port: 9000}")
    assert(!NodeWebSettings.read(Some(raw)).enabled)
  }

  test("S3 starts with the node, can be disabled, releases its socket, and rolls back bind failures") {fixture{(dir,cert,key)=>
    val c=config(dir,cert,key)
    val server=DacpServer.builder().config(c).build().start()
    val s3Port=server.s3Port.get
    val http=SecurityTestSupport.client(cert)
    val anonymous=http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(s"https://localhost:$s3Port/")).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString())
    assert(anonymous.statusCode()==403)
    SecurityTestSupport.register(s3Port,c)
    val response=http.send(SecurityTestSupport.signed(s3Port,"/"),java.net.http.HttpResponse.BodyHandlers.ofString())
    assert(response.statusCode()==200 && response.body.contains("ListAllMyBucketsResult"))
    server.close();val freed=new ServerSocket(s3Port);freed.close()
    val disabled=DacpServer.builder().config(config(dir,cert,key).copy(s3=S3Settings(enabled=false))).build().start()
    assert(disabled.s3Port.isEmpty);disabled.close()
    val blocked=new ServerSocket(0)
    val bad=config(dir,cert,key).copy(s3=S3Settings(port=Some(blocked.getLocalPort)))
    val failed=DacpServer.builder().config(bad).build()
    try intercept[java.net.BindException](failed.start())finally blocked.close()
    failed.close();val socket=new ServerSocket(bad.port);socket.close()
  }}

}
