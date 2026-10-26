package dacp

import java.nio.file.{Files,Path}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import dacp.union.Json
import scala.jdk.CollectionConverters._

class LocalJobExecutionSpec extends AnyFunSuite {
  private val yaml="""job: local
    |tasks:
    |- id: read_a_source
    |  operator: read_item@1.0
    |  inputs: {}
    |  params:
    |    uri: dacp://localhost:8815/data/item
    |    view: bytes
    |- id: a
    |  operator: copy@1
    |  inputs:
    |    source: read_a_source
    |- id: b
    |  operator: copy@1
    |  inputs:
    |    source: a
    |outputs:
    |  result: b
    |""".stripMargin
  private val plan=Jobs.parse(yaml)
  private def fixture(f:(Config,RootAllocator)=>Unit):Unit=fixtureWithHistory(false)(f)
  private def fixtureWithHistory(memory:Boolean)(f:(Config,RootAllocator)=>Unit):Unit={
    val dir=Files.createTempDirectory("local-policy-");val allocator=new RootAllocator()
    val provider=new ExecutionStoreProvider {
      val id="memory"
      def open(settings:Map[String,Object],readOnly:Boolean):ExecutionStore=new ExecutionStore {
        private val data=scala.collection.mutable.Map.empty[String,com.fasterxml.jackson.databind.JsonNode]
        def get(id:String)=synchronized{data.get(id).map(_.deepCopy[com.fasterxml.jackson.databind.JsonNode]())}
        def records=synchronized{data.values.map(_.deepCopy[com.fasterxml.jackson.databind.JsonNode]()).toVector}
        def create(n:com.fasterxml.jackson.databind.JsonNode):Unit=synchronized{require(!data.contains(n.path("record_id").asText));put(n)}
        def put(n:com.fasterxml.jackson.databind.JsonNode):Unit=synchronized{data(n.path("record_id").asText)=n.deepCopy[com.fasterxml.jackson.databind.JsonNode]()}
        def close():Unit=()
      }
    }
    val plugins=if(memory)Some(new PluginEnvironment(registrations=Seq(PluginRegistration(provider))))else None
    val c=Config("localhost",8815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"),
      executionStore=Some(if(memory)ExecutionStoreDefinition("memory",Map.empty)else ExecutionStoreDefinition("file",Map("directory"->dir.resolve("history").toString))),pluginEnvironment=plugins,ownsPluginEnvironment=true,
      materializationStore=Some(MaterializationStoreDefinition("file",Map("directory"->dir.resolve("outputs").toString))))
    try f(c,allocator)finally {c.close();allocator.close();val paths=Files.walk(dir);try paths.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally paths.close()}
  }
  private def input(b:InputBinding,c:StreamCancel):FrameSource=new FrameSource {
    private val frames=Iterator(StreamProtocol.start("","bytes","txt"),StreamProtocol.frame("data",data="weather\n".getBytes),StreamProtocol.frame("end"))
    def next():Option[OperatorFrame]={c.active();if(frames.hasNext)Some(frames.next())else None}
    def close():Unit=()
  }
  private def record(c:Config)=c.executionHistory.begin("dacp_exchange",metadata=Json.obj().put("owner","alice").put("request_type","job").put("job_yaml",yaml))
  private def policy(text:String)=ExecutionPolicy.parse(Json.mapper.readTree(text),plan)
  test("zero-output sink consumes its input stream") {fixtureWithHistory(true){(c,a)=>
    val definition="""job: sink
      |tasks:
      | - {id: read, operator: read_item@1.0, inputs: {}, params: {uri: 'dacp://localhost:8815/data/item', view: bytes}}
      | - {id: sink, operator: sink@1, inputs: {source: read}}
      |outputs: {}
      |""".stripMargin
    val p=Jobs.parse(definition);val consumed=new AtomicInteger
    val executor=new TaskExecutor(c,a){override def execute(t:Task,in:Map[String,FrameSource],emit:OperatorFrame=>Unit,cancel:StreamCancel,resolved:Option[ResolvedOperator],onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]):Unit={
      var f=in("source").next();while(f.nonEmpty){if(StreamProtocol.header(f.get).path("type").asText=="data")consumed.addAndGet(f.get.data.length);f=if(StreamProtocol.header(f.get).path("type").asText=="end")None else in("source").next()}
    }}
    new LocalJobExecution(c,executor,input,a).run(p,ExecutionPolicy.parse(Json.obj(),p),"alice",record(c),None,_=>fail("sink must not emit data"),new StreamCancel())
    assert(consumed.get()>0)
  }}
  test("standalone policy configuration is independent of Union and validates timeout types") {
    val file=Files.createTempFile("local-policy-config-",".yaml")
    val base="host: localhost\npublic_host: localhost\ntls_cert: unused\ntls_key: unused\ntls_ca: unused\n"
    try {
      Files.writeString(file,base+"execution_timeout_seconds: 120\nmaterialization_store: {provider: file, settings: {directory: .unused-materialized}}\n")
      val c=Config.load(file)
      try {assert(c.union.isEmpty);assert(c.executionTimeoutSeconds==120);assert(c.materializationStore.get.provider=="file")}finally c.close()
      for(value<-Seq("0","86401","1.5","'120'","true")){
        Files.writeString(file,base+s"execution_timeout_seconds: $value\n")
        intercept[IllegalArgumentException](Config.load(file))
      }
    }finally Files.delete(file)
  }
  test("failed downstream retries from committed upstream and resets partial result; explicit replay skips both steps") {fixture{(c,a)=>
    val upstream=new AtomicInteger;val downstream=new AtomicInteger
    val executor=new TaskExecutor(c,a){override def execute(t:Task,in:Map[String,FrameSource],emit:OperatorFrame=>Unit,cancel:StreamCancel,resolved:Option[ResolvedOperator],onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]):Unit={
      val count=if(t.id=="a")upstream.incrementAndGet()else downstream.incrementAndGet()
      var f=in("source").next();while(f.nonEmpty){emit(StreamProtocol.rename(f.get,"result"));f=if(StreamProtocol.header(f.get).path("type").asText=="end")None else in("source").next()}
      if(t.id=="b" && count==1){Thread.sleep(100);throw new RuntimeException("transient failure")}
    }}
    val engine=new LocalJobExecution(c,executor,input,a)
    val p=policy("""{"max_retries":1,"materialization":{"default":"materialize"}}""")
    val r=record(c);val bytes=new java.io.ByteArrayOutputStream;var notices=0
    def receive(f:OperatorFrame):Unit=StreamProtocol.header(f).path("type").asText match {
      case "attempt_failed"=>notices+=1;bytes.reset()
      case "data"=>bytes.write(f.data)
      case _=>()
    }
    engine.run(plan,p,"alice",r,None,receive,new StreamCancel())
    r.update("succeeded")
    assert(notices==1);assert(bytes.toString=="weather\n");assert(upstream.get()==1);assert(downstream.get()==2)
    bytes.reset();val replay=record(c)
    engine.run(plan,p,"alice",replay,Some(r.id),receive,new StreamCancel())
    assert(bytes.toString=="weather\n");assert(upstream.get()==1 && downstream.get()==2)
    assert(c.executionHistory.get(replay.id).get.path("skipped_tasks").elements.asScala.exists(_.asText=="a"))
    val denied=record(c)
    intercept[IllegalArgumentException](engine.run(plan,p,"mallory",denied,Some(r.id),_=>(),new StreamCancel()))
    c.close();val restarted=c.copy()
    try {
      bytes.reset()
      val restored=new LocalJobExecution(restarted,new TaskExecutor(restarted,a),(_,_)=>fail("replay must not reopen raw data"),a)
      restored.run(plan,p,"alice",record(restarted),Some(r.id),receive,new StreamCancel())
      assert(bytes.toString=="weather\n");assert(upstream.get()==1 && downstream.get()==2)
      val execution=restarted.executionHistory.get(r.id).get.path("materialized_sources").path("b").asText
      val file=Path.of(restarted.materializationStore.get.settings("directory").toString).resolve(ExecutionPolicy.key(execution,"b")).resolve("result.bin")
      val corrupted=Files.readAllBytes(file);val index=new String(corrupted,java.nio.charset.StandardCharsets.ISO_8859_1).indexOf("weather")
      assert(index>=0);corrupted(index)=(corrupted(index)^1).toByte;Files.write(file,corrupted)
      var ends=0
      intercept[IllegalArgumentException](restored.run(plan,p,"alice",record(restarted),Some(r.id),f=>{if(StreamProtocol.header(f).path("type").asText=="end")ends+=1},new StreamCancel()))
      assert(ends==0,"corrupt replay must not publish end before validating the artifact checksum")
    }finally restarted.close()
  }}
  test("all-streaming retry obeys retry bound without materialization") {for(_<-1 to 5)fixtureWithHistory(true){(c,a)=>
    val calls=new AtomicInteger
    val executor=new TaskExecutor(c,a){override def execute(t:Task,in:Map[String,FrameSource],emit:OperatorFrame=>Unit,cancel:StreamCancel,resolved:Option[ResolvedOperator],onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]):Unit={calls.incrementAndGet();throw new RuntimeException("unavailable")}}
    val p=policy("""{"max_retries":2,"materialization":{"allowed":false}}""")
    val r=record(c);var notices=0
    val failure=intercept[RuntimeException](new LocalJobExecution(c,executor,input,a).run(plan,p,"alice",r,None,f=>{if(StreamProtocol.header(f).path("type").asText=="attempt_failed")notices+=1},new StreamCancel()))
    assert(notices==2,s"notices=$notices failure=$failure");assert(calls.get()>=3 && calls.get()<=6);assert(c.taskSlots.availablePermits()==c.maxConcurrentTasks,s"leaked permits after $failure")
  }}
  test("whole-job timeout and explicit cancellation stop steps without retry") {fixtureWithHistory(true){(c,a)=>
    val started=new java.util.concurrent.CountDownLatch(1);val stopped=new AtomicInteger
    val executor=new TaskExecutor(c,a){override def execute(t:Task,in:Map[String,FrameSource],emit:OperatorFrame=>Unit,cancel:StreamCancel,resolved:Option[ResolvedOperator],onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]):Unit={started.countDown();try {while(true){cancel.active();Thread.sleep(10)}}finally stopped.incrementAndGet()}}
    val engine=new LocalJobExecution(c,executor,input,a);val p=policy("""{"max_retries":2,"timeout_seconds":1}""")
    var notices=0;val before=System.nanoTime()
    intercept[java.util.concurrent.TimeoutException](engine.run(plan,p,"alice",record(c),None,_=>notices+=1,new StreamCancel(timeoutSeconds=1)))
    assert((System.nanoTime()-before)/1e9<5);assert(notices==0);assert(stopped.get()>=1);assert(c.taskSlots.availablePermits()==2)
    val cancel=new StreamCancel();val error=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val thread=new Thread(()=>try engine.run(plan,p,"alice",record(c),None,_=>(),cancel)catch{case t:Throwable=>error.set(t)})
    thread.start();Thread.sleep(100);cancel.close();thread.join(5000)
    assert(!thread.isAlive);assert(error.get().isInstanceOf[java.util.concurrent.CancellationException]);assert(c.taskSlots.availablePermits()==2)
  }}
  test("Flight Job exchange skips operator inference, validates policy, and replays original definition") {fixture{(c,a)=>
    val calls=new AtomicInteger
    val executor=new TaskExecutor(c,a){override def execute(t:Task,in:Map[String,FrameSource],emit:OperatorFrame=>Unit,cancel:StreamCancel,resolved:Option[ResolvedOperator],onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]):Unit={
      calls.incrementAndGet();var f=in("source").next()
      while(f.nonEmpty){emit(StreamProtocol.rename(f.get,"result"));f=if(StreamProtocol.header(f.get).path("type").asText=="end")None else in("source").next()}
    }}
    val service=new TaskService(c,a,executor){override protected def source(b:InputBinding,cancel:StreamCancel):FrameSource=input(b,cancel)}
    val producer=new org.apache.arrow.flight.NoOpFlightProducer {
      override def doExchange(ctx:org.apache.arrow.flight.FlightProducer.CallContext,r:org.apache.arrow.flight.FlightStream,w:org.apache.arrow.flight.FlightProducer.ServerStreamListener):Unit=service.exchange(ctx,r,w)
    }
    val server=org.apache.arrow.flight.FlightServer.builder(a,org.apache.arrow.flight.Location.forGrpcInsecure("localhost",0),producer).build().start()
    val client=org.apache.arrow.flight.FlightClient.builder(a,server.getLocation).build()
    def exchange(yaml:String,policy:String,replay:String="",aware:Boolean=false):Vector[OperatorFrame]={
      val req=dacp.protocol.Metadata.OperatorRequest.newBuilder().setJobYaml(yaml).setExecutionPolicyJson(com.google.protobuf.ByteString.copyFromUtf8(policy)).setReplayFrom(replay).setRetryAware(aware).build()
      val x=client.doExchange(org.apache.arrow.flight.FlightDescriptor.command(req.toByteArray));val root=org.apache.arrow.vector.VectorSchemaRoot.create(PluginSchemas.bytes,a)
      try {x.getWriter.start(root);x.getWriter.completed();val out=Vector.newBuilder[OperatorFrame];while(x.getReader.next())out+=StreamProtocol.receive(x.getReader.getRoot);out.result()}finally {x.close();root.close()}
    }
    try {
      intercept[org.apache.arrow.flight.FlightRuntimeException](exchange(yaml,"""{"max_retries":1}"""));assert(calls.get()==0)
      intercept[org.apache.arrow.flight.FlightRuntimeException](exchange(yaml,"""{"timeout_seconds":1801}"""));assert(calls.get()==0)
      val output=exchange(yaml,"""{"materialization":{"default":"materialize"}}""")
      val id=StreamProtocol.header(output.head).path("execution_id").asText;assert(id.nonEmpty);assert(calls.get()==2)
      val deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(5)
      while(c.executionHistory.get(id).get.path("state").asText!="succeeded" && System.nanoTime()<deadline)Thread.sleep(10)
      assert(!c.executionHistory.get(id).get.has("structure_inference"))
      val replayed=exchange("","",id)
      assert(replayed.filter(f=>StreamProtocol.header(f).path("type").asText=="data").flatMap(_.data).toArray.sameElements("weather\n".getBytes));assert(calls.get()==2)
      assert(c.executionHistory.list().exists(_.path("replay_of").asText==id))
    }finally {client.close();server.close();service.close()}
  }}

}
