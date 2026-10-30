package dacp

import _root_.xstream.data._

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
  private def catalog(counter:AtomicInteger)=new DataCatalog {
    def list():Seq[DatasetInfo]=Seq(DatasetInfo("public"))
    def open(name:String):PublishedDataset=throw new java.io.FileNotFoundException("no items")
    override def close():Unit={counter.incrementAndGet();()}
  }
  test("scientific sequence view travels through the authenticated server with its descriptor") {fixture{(dir,cert,key)=>
    val c=config(dir,cert,key)
    val path=dir.resolve("sample.fa");Files.writeString(path,">chr1\nACGTACGT\n")
    val cat=new DataCatalog {
      def list():Seq[DatasetInfo]=Seq(DatasetInfo("science"))
      def open(name:String):PublishedDataset=new PublishedDataset {
        def open(check:()=>Unit):DatasetAccess=new _root_.xstream.data.fasta.FastaFormat().openStructure(ByteSource.local(path),FileOptions(),c,check)
      }
    }
    val server=DacpServer.builder().config(c).catalog(cat).build().start()
    val allocator=new RootAllocator()
    val token=c.credentials.create("scientific-reader")._2
    val client=dacp.union.ClientTransport.open(allocator,c.uri(""),Some(cert),None,None,()=>Some(token))
    try {
      val request=dacp.protocol.Metadata.Ticket.newBuilder().setDataset("science").setItem("/sequences").setView("sequence")
        .setSelectionJson(com.google.protobuf.ByteString.copyFromUtf8("{\"offset\":2,\"length\":4}")).build()
      val reader=client.getStream(new Ticket(request.toByteArray))
      val chunks=scala.collection.mutable.ArrayBuffer.empty[SequenceChunk]
      val codec=new _root_.xstream.data.scientific.SequenceCodec()
      try dacp.union.ScientificWire.consume(reader,"sequence",new EncodedDataConsumer {
        var descriptor:DataDescriptor[SequenceChunk]=null
        def start(d:com.fasterxml.jackson.databind.JsonNode):Unit={descriptor=codec.descriptor(d)}
        def accept(b:EncodedDataBlock):Unit=chunks+=codec.decode(b,descriptor)
      },()=>())finally reader.close()
      assert(chunks.map(_.symbols.toString).mkString=="GTAC" && chunks.head.offset==2 && chunks.last.last)
      val yaml="name: Scientific Job\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'context:/science/sequences', view: sequence, selection: {offset: 1, length: 3}}\noutputs: {result: read}\n"
      val tls=dacp.client.TlsConfig(ca=Some(cert),token=Some(token))
      val inference=dacp.client.Job.infer(c.uri(""),yaml,tls)
      assert(inference.path("outputs").path("result").path("structure").path("view").asText=="sequence")
      val job=dacp.client.Job.run(c.uri(""),yaml,tls)
      try {
        val events=job.toVector
        val parts=events.filter(_.kind=="data").map(_.scientific(codec))
        assert(parts.map(_.symbols.toString).mkString=="CGT" && parts.head.offset==1)
        assert(events.exists(_.kind=="complete"))
      }finally job.close()

    }finally {client.close();allocator.close();server.close()}
  }}
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
  test("public port changes advertised URIs and location without changing the listener") {fixture{(dir,cert,key)=>
    val count=new AtomicInteger();val c=config(dir,cert,key).copy(publicHost="data.example.org",publicPort=Some(28815))
    val server=DacpServer.builder().config(c).catalog(catalog(count)).build().start()
    try {
      assert(server.boundPort==c.port)
      assert(server.location.getUri.getHost=="data.example.org")
      assert(server.location.getUri.getPort==28815)
      assert(c.uri("public","/observations")=="dacp://data.example.org:28815/public/observations")
      val allocator=new RootAllocator()
      val client=dacp.union.ClientTransport.open(allocator,s"dacp://localhost:${c.port}",Some(cert),None,None,()=>Some(c.credentials.create("proxy-reader")._2))
      try {
        val result=client.doAction(new Action("dacp.catalog",Array.emptyByteArray)).next()
        val directory=dacp.protocol.Metadata.Directory.parseFrom(result.getBody)
        assert(directory.getDatasets(0).getUri=="dacp://data.example.org:28815/public")
      }finally {client.close();allocator.close()}
    }finally server.close()
    intercept[IllegalArgumentException](c.copy(publicPort=Some(0)))
    intercept[IllegalArgumentException](c.copy(publicPort=Some(65536)))
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
    val cat=new DataCatalog {
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
public_port: 28815
port: ${c.port}
tls_cert: $cert
tls_key: $key
tls_ca: $cert
execution_store: {provider: file, settings: {directory: '${dir.resolve("history")}'}}
data_catalog: {provider: does-not-exist}
s3: {host: localhost, port: 0}
web: {host: localhost, port: 0}
""")
    val builder=DacpServer.builder().configFile(yaml).catalog(catalog(count))
    val server=builder.build()
    try {server.start();assert(server.s3Port.isDefined);assert(server.boundPort==c.port);assert(server.location.getUri.getPort==28815)}finally server.close()
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
      val metadata=dacp.union.Json.obj().put("execution_id","web-example").put("union_id","union-example")
      val definitions=dacp.union.Json.mapper.readTree("""[{"id":"copy","title":"复制记录","operator":"identity@1.0","inputs":{"source":{"uri":"dacp://localhost/data/source","view":"bytes"}},"materialize":[]},{"id":"finish","operator":"identity@1.0","inputs":{"source":"copy:result","remote":{"origin":"dacp://upstream:8815","producer_identity":"peer","task":"parse","port":"result"}},"output_routes":{"result":{"consumer_identity":"client"}}}]""")
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
      val external=graph.path("graph_tasks").get(1).path("inputs").get(1)
      assert(external.path("remote").asBoolean && external.path("upstream_origin").asText=="dacp://upstream:8815")
      assert(external.path("upstream_task").asText=="parse" && !external.has("task"))
      assert(graph.path("graph_nodes").path("local").path("ports").path("copy/outputs/result").path("payload_bytes").asLong==1024)
      assert(!graph.path("graph_outputs").has("copy.result") && graph.path("graph_outputs").has("finish.result"))
      assert(graph.path("execution_source").asText=="union" && graph.path("union_id").asText=="union-example")
      assert(!graph.path("task_definitions").get(0).has("output_routes") && graph.path("task_definitions").get(1).has("output_routes"))
      // Union step snapshots contain globally qualified metrics, not just this step's ports.
      val parseStep=c.executionHistory.begin("union_task",metadata=dacp.union.Json.obj().put("parent_record_id",record.id).put("task_id","finish"))
      val metrics=dacp.union.Json.obj();val allPorts=metrics.putObject("ports")
      allPorts.putObject("copy/outputs/result").put("payload_bytes",1024)
      allPorts.putObject("finish/outputs/result").put("payload_bytes",512)
      parseStep.update("succeeded",metrics)
      val outputGraph=dacp.union.Json.mapper.readTree(http(port,"/api/executions/"+record.id).body())
      assert(outputGraph.path("graph_outputs").size()==1 && outputGraph.path("graph_outputs").has("finish.result"))
      assert(outputGraph.path("graph_nodes").path("local").path("ports").fieldNames().asScala.forall(_.split("/",-1).length==3))
      val direct=c.executionHistory.begin("dacp_exchange");direct.update("succeeded")
      val step=c.executionHistory.begin("job_task",metadata=dacp.union.Json.obj().put("parent_execution_id",direct.id));step.update("succeeded")
      val unionPage=dacp.union.Json.mapper.readTree(http(port,"/api/history?source=union&limit=1").body()).path("records")
      assert(unionPage.size()==1 && unionPage.get(0).path("record_id").asText==record.id && unionPage.get(0).path("execution_source").asText=="union")
      val clientPage=dacp.union.Json.mapper.readTree(http(port,"/api/history?source=client").body()).path("records")
      assert(clientPage.size()==1 && clientPage.elements().asScala.forall(_.path("execution_source").asText=="client"))
      val unionStep=c.executionHistory.begin("union_task",metadata=dacp.union.Json.obj().put("parent_record_id",record.id).put("task_id","copy"));unionStep.update("succeeded")
      val nestedExchange=c.executionHistory.begin("dacp_exchange",metadata=dacp.union.Json.obj().put("parent_execution_id",direct.id));nestedExchange.update("succeeded")
      val jobs=dacp.union.Json.mapper.readTree(http(port,"/api/history").body()).path("records")
      assert(jobs.size()==2 && jobs.elements().asScala.forall(r=>Set(record.id,direct.id)(r.path("record_id").asText)))
      val directDetails=dacp.union.Json.mapper.readTree(http(port,"/api/executions/"+direct.id).body())
      assert(directDetails.path("step_records").size()==2)
      val unionDetails=dacp.union.Json.mapper.readTree(http(port,"/api/executions/"+record.id).body())
      assert(unionDetails.path("step_records").size()==2 && unionDetails.path("step_records").elements().asScala.exists(_.path("record_id").asText==unionStep.id))
      assert(http(port,"/api/history?source=untrusted").statusCode()==400)
      assert(http(port,"/api/history?source=union&source=client").statusCode()==400)

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
