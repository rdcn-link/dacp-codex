package dacp.unionserver

import dacp._
import dacp.union._
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightStatusCode._
import java.nio.file.{Files,Path}
import scala.sys.process._
import scala.jdk.CollectionConverters._

class UnionSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val root=Files.createTempDirectory("dacp-union-contract-")
  private def run(args:String*):Unit=assert(Process(args,root.toFile).!(ProcessLogger(_=>(),_=>()))==0)
  override def beforeAll():Unit={
    run("openssl","req","-x509","-newkey","rsa:2048","-nodes","-keyout","ca.key","-out","ca.crt","-days","1","-subj","/CN=union-test-ca")
    Files.writeString(root.resolve("leaf.ext"),"subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n")
    Seq("a","b","x","c","stranger").foreach { name=>
      run("openssl","req","-newkey","rsa:2048","-nodes","-keyout",s"$name.key","-out",s"$name.csr","-subj",s"/CN=$name")
      run("openssl","x509","-req","-in",s"$name.csr","-CA","ca.crt","-CAkey","ca.key","-CAcreateserial","-out",s"$name.crt","-days","1","-extfile","leaf.ext")
    }
  }
  override def afterAll():Unit={val files=Files.walk(root);try files.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally files.close()}
  private def tls(name:String)=new Tls(root.resolve(s"$name.crt"),root.resolve(s"$name.key"),root.resolve("ca.crt"))
  private def port():Int={val s=new java.net.ServerSocket(0);try s.getLocalPort finally s.close()}
  private def eventually(check: => Boolean,clue: => String="condition"):Unit={val end=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(15);while(!check && System.nanoTime()<end)Thread.sleep(50);assert(check,clue)}
  private final class Fixture(allowStranger:Boolean=false,timeoutSeconds:Long=60,web:Option[WebSettings]=None,dynamicCatalog:Boolean=false,materialize:Boolean=false,withRecords:Boolean=false) {
    @volatile var catalogVisible=true
    @volatile var catalogFailure=false
    val allocator=new RootAllocator()
    val dir=Files.createTempDirectory(root,"fixture-")
    val inputA=Array.fill[Byte](300000)(65.toByte);val inputB="local-B".getBytes
    Files.write(dir.resolve("a.bin"),inputA);Files.write(dir.resolve("b.bin"),inputB)
    Files.writeString(dir.resolve("observations.csv"),"station,temperature\nA,18\nB,21\nC,24\n")
    def store(name:String)=if(materialize)Some(MaterializationStoreDefinition("file",Map("directory"->dir.resolve(name+"-outputs").toString)))else None
    val as=NodeUnionSettings(dir.resolve("a-state"),30,30,store("a"))
    val bs=NodeUnionSettings(dir.resolve("b-state"),30,30,store("b"))
    val invocations=new java.util.concurrent.ConcurrentHashMap[String,java.util.concurrent.atomic.AtomicInteger]()
    @volatile var failDownstreamOnce=false
    @volatile var controlFinished=false
    def count(task:String):Int=Option(invocations.get(task)).map(_.get).getOrElse(0)
    def config(name:String):Config={
      val definition=FileDatasets.mixed(Seq(PublishedItem("/input",file=Some(dir.resolve(s"$name.bin"))))++(if(withRecords)Seq(PublishedItem("/observations",file=Some(dir.resolve("observations.csv"))))else Seq.empty)).copy(description="""{"summary":"公开数据说明","html":"<script>alert(1)</script>"}""")
      val c=Config("localhost",port(),"localhost",if(dynamicCatalog)Map.empty else Map("data"->definition),root.resolve(s"$name.crt"),root.resolve(s"$name.key"),root.resolve("ca.crt"),maxConcurrentTasks=4,accessLogDirectory=Some(dir.resolve(name+"-access")),executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->(dir.resolve(s"$name-executions")).toString))))
      if(dynamicCatalog)c.installCatalog(new DatasetCatalog {
        def list():Seq[DatasetInfo]={if(catalogFailure)throw new IllegalStateException("catalog unavailable");if(catalogVisible)Seq(DatasetInfo("data",definition.description))else Seq.empty}
        def open(name:String):PublishedDataset={require(name=="data");c.createDataset(definition)}
      })
      c
    }
    val ca=config("a");val cb=config("b")
    val runner:(Task,Map[String,FrameSource],OperatorFrame=>Unit,StreamCancel)=>Unit=(task,inputs,emit,cancel)=>{
      invocations.computeIfAbsent(task.id,_=>new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet()
      if(task.params.containsKey("fail"))throw new IllegalArgumentException("test failure")
      if(task.params.containsKey("wait")){while(true){cancel.active();Thread.sleep(20)}}
      if(task.params.containsKey("control")){Thread.sleep(100);controlFinished=true}
      else if(task.params.containsKey("sink")){
        assert(controlFinished,"control predecessor has not succeeded")
        inputs.values.foreach { source=>var done=false;while(!done){val frame=source.next().get;done=StreamProtocol.header(frame).path("type").asText=="end"} }
      }
      else if(task.params.containsKey("multi")){
        val data=new java.io.ByteArrayOutputStream()
        inputs.values.foreach { input=>var done=false;while(!done){val f=input.next().get;StreamProtocol.header(f).path("type").asText match {case "data"=>data.write(f.data);case "end"=>done=true;case _=>()}}}
        Seq("z","a").foreach { port=>emit(StreamProtocol.start(port,"bytes","binary"));data.toByteArray.grouped(65536).foreach(chunk=>emit(StreamProtocol.frame("data",port,chunk)));emit(StreamProtocol.frame("end",port))}
      }else {
      emit(StreamProtocol.start("result","bytes","application/octet-stream"))
      inputs.toVector.sortBy(_._1).foreach {case(_,source)=>
        var end=false
        while(!end){val frame=source.next().get;StreamProtocol.header(frame).path("type").asText match {
          case "start"=>()
          case "data"=>emit(StreamProtocol.rename(frame,"result"))
          case "end"=>end=true
        }}
      };if(task.params.containsKey("flood")){while(true){cancel.active();emit(StreamProtocol.frame("data","result",Array.fill[Byte](65536)(42)))}}
      if(task.id=="tb" && failDownstreamOnce){failDownstreamOnce=false;throw new IllegalStateException("downstream failed after reading upstream")}
      emit(StreamProtocol.frame("end","result"))
      }
    }
    val a=new UnionWorker(ca,as,allocator,Some(runner));val b=new UnionWorker(cb,bs,allocator,Some(runner))
    val xp=port()
    val unionId="test-union"
    Files.createDirectories(dir.resolve("x-state"));Json.write(dir.resolve("x-state/union.json"),Json.obj().put("union_id",unionId))
    def issue(c:Config,name:String):MemberSettings={
      val issued=c.credentials.create("union-test","union",unionId=unionId,fingerprint=tls("x").identity)
      val path=dir.resolve(name+"-credential");Files.writeString(path,issued._2)
      MemberSettings(DacpUri.origin(c.uri("")),path)
    }
    val settings=Settings("localhost",xp,s"dacp://localhost:$xp",root.resolve("x.crt"),root.resolve("x.key"),root.resolve("ca.crt"),dir.resolve("x-state"),100,timeoutSeconds,30,web,memberSettings=Vector(issue(ca,"a"),issue(cb,"b")),s3=S3Settings(port=Some(0)))
    val clientToken=settings.credentials.create("alice")._2
    val strangerToken=if(allowStranger)settings.credentials.create("bob")._2 else "invalid"
    val x=new Coordinator(settings)
    val client=new Remote(None,credential=_=>Some(clientToken),ca=Some(tls("c").ca))
    val stranger=new Remote(None,credential=_=>Some(strangerToken),ca=Some(tls("c").ca))
    def producer(c:Config,worker:UnionWorker)=new Producer(c,allocator,union=Some(worker)) {
      override protected def inferTask(t:Task,in:Map[String,StreamDescriptor],cancel:StreamCancel):InferenceResult={
        throw new AssertionError("job execution must not request structure inference")
      }
    }
    val af=new Endpoint("localhost",ca.port,tls("a"),allocator,producer(ca,a),Some(ca.credentials))
    val bf=new Endpoint("localhost",cb.port,tls("b"),allocator,producer(cb,b),Some(cb.credentials))
    af.start();bf.start();a.start();b.start();x.start()
    def approved():Unit={x.members.all.foreach(x.members.inspect);eventually(x.members.all.forall(_.path("state").asText=="approved"))}
    def revokeA():Unit=ca.credentials.list.filter(_.path("kind").asText=="union").foreach(n=>ca.credentials.revoke(n.path("id").asText))
    def publicYaml(text:String):String={
      approved()
      val entries=catalog(client.dataClient(settings.publicUrl,None)).getDatasetsList.asScala
      Vector(ca,cb).foldLeft(text){(out,c)=>
        val e=entries.find(e=>x.routes.resolve(e.getName).get.path("member").asText==new Tls(c.cert,c.key,c.ca).identity).get
        out.replace(c.uri("data"),e.getUri).replace(DacpUri.origin(c.uri("")),"nd-"+x.routes.assign(new Tls(c.cert,c.key,c.ca).identity,"").stripPrefix("ds-"))
      }
    }
    val yaml=s"""job: union-example
    |tasks:
    |- id: read_ta_source
    |  operator: read_item@1.0
    |  inputs: {}
    |  params:
    |    uri: ${ca.uri("data","/input")}
    |    view: bytes
    |  node: ${DacpUri.origin(ca.uri(""))}
    |- id: ta
    |  title: 汇总本地数据
    |  node: ${DacpUri.origin(ca.uri(""))}
    |  operator: identity@1.3
    |  inputs:
    |    source: read_ta_source
    |- id: read_tb_right
    |  operator: read_item@1.0
    |  inputs: {}
    |  params:
    |    uri: ${cb.uri("data","/input")}
    |    view: bytes
    |  node: ${DacpUri.origin(cb.uri(""))}
    |- id: tb
    |  title: 融合跨节点数据
    |  node: ${DacpUri.origin(cb.uri(""))}
    |  operator: identity@1.3
    |  inputs:
    |    left: ta
    |    right: read_tb_right
    |outputs:
    |  result: tb
    |""".stripMargin
    def submit(text:String=yaml):String=client.action(settings.publicUrl,"submit",Json.obj().put("job_yaml",publicYaml(text))).path("job_id").asText
    def submitPolicy(policy:JsonNode):String=client.action(settings.publicUrl,"submit",Json.obj().put("job_yaml",publicYaml(yaml)).set[JsonNode]("execution_policy",policy)).path("job_id").asText
    def replay(id:String,tasks:Option[Seq[String]]=None):String={
      val n=Json.obj().put("job_id",id);tasks.foreach(ts=>{val a=n.putArray("tasks");ts.foreach(a.add)})
      client.action(settings.publicUrl,"replay_job",n).path("job_id").asText
    }
    var retryNotices=0
    def bytes(id:String,retryAware:Boolean=true):Array[Byte]={
      val ticket=Json.obj().put("job_id",id);if(retryAware && status(id).path("max_retries").asInt>0)ticket.put("retry_aware",true)
      val r=client.stream(settings.publicUrl,"results",ticket);val out=new java.io.ByteArrayOutputStream()
      try {var next=r.next();var complete=false;while(next.nonEmpty){val f=OperatorFrames.decode(next.get);StreamProtocol.header(f).path("type").asText match {case "data"=>out.write(f.data);case "attempt_failed"=>retryNotices+=1;out.reset();case "complete"=>complete=true;case _=>()};next=r.next()};assert(complete);out.toByteArray}finally r.close()
    }
    def status(id:String)=client.action(settings.publicUrl,"job_status",Json.obj().put("job_id",id))
    def close():Unit={x.close();a.close();b.close();af.close();bf.close();client.close();stranger.close();Thread.sleep(100);ca.close();cb.close();allocator.close()}
  }
  private def policy(all:Boolean=false):JsonNode=if(all)Json.parse("""{"materialization":{"default":"materialize"}}""")else Json.parse("""{"materialization":{"tasks":{"ta":{"outputs":{"result":"materialize"}}}}}""")
  test("subjobs separate output routes from stream types and reject superseded route payloads") {
    val f=new Fixture()
    try {
      f.approved();val id=f.submit();assert(f.bytes(id).sameElements(f.inputA++f.inputB))
      eventually(f.status(id).path("state").asText=="succeeded")
      val a=f.ca.executionHistory.store.records.find(_.path("kind").asText=="union_execution").get
      val b=f.cb.executionHistory.store.records.find(_.path("kind").asText=="union_execution").get
      def task(record:JsonNode,id:String)=record.path("task_definitions").elements.asScala.find(_.path("id").asText==id).get
      val upstream=task(a,"ta");val downstream=task(b,"tb")
      assert(!upstream.has("outputs") && !downstream.has("outputs"))
      assert(upstream.path("output_routes").path("result").path("consumer_identity").asText==tls("b").identity)
      assert(downstream.path("output_routes").path("result").path("consumer_identity").asText==tls("x").identity)
      assert(downstream.path("inputs").path("left").path("origin").asText==DacpUri.origin(f.ca.uri("")))
      val remoteInput=downstream.path("inputs").path("left")
      assert(remoteInput.path("producer_identity").asText==tls("a").identity && !remoteInput.has("identity"))
      val invalidInput=Json.obj().put("union_id",f.unionId).put("execution_id","superseded-producer")
      invalidInput.set[JsonNode]("tasks",b.path("task_definitions").deepCopy[JsonNode]())
      val oldBinding=invalidInput.path("tasks").elements.asScala.find(_.path("id").asText=="tb").get.path("inputs").path("left").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      oldBinding.set[JsonNode]("identity",oldBinding.remove("producer_identity"))
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.cb.uri("")),"prepare",invalidInput,Some(tls("b").identity))).code==INVALID_ARGUMENT)
      val reader=task(a,"read_ta_source")
      assert(!reader.has("output_routes"))
      assert(upstream.path("inputs").path("source").asText=="read_ta_source:result")
      assert(task(b,"read_tb_right").path("output_routes").isMissingNode)
      assert(downstream.path("inputs").path("right").asText=="read_tb_right:result")
      val body=Json.obj().put("union_id",f.unionId).put("execution_id","invalid-routes")
      body.set[JsonNode]("tasks",a.path("task_definitions").deepCopy[JsonNode]())
      val first=body.path("tasks").elements.asScala.find(_.path("id").asText=="ta").get.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      val localReader=body.path("tasks").elements.asScala.find(_.path("id").asText=="read_ta_source").get.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      localReader.putObject("output_routes").putObject("result").put("consumer_identity",tls("a").identity)
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.ca.uri("")),"prepare",body,Some(tls("a").identity))).code==INVALID_ARGUMENT)
      localReader.remove("output_routes")
      first.withObject("inputs").putObject("source").put("task","read_ta_source").put("port","result")
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.ca.uri("")),"prepare",body,Some(tls("a").identity))).code==INVALID_ARGUMENT)
      first.withObject("inputs").put("source","read_ta_source:result")
      first.set[JsonNode]("outputs",first.remove("output_routes"))
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.ca.uri("")),"prepare",body,Some(tls("a").identity))).code==INVALID_ARGUMENT)
      first.set[JsonNode]("output_routes",first.remove("outputs"))
      first.withObject("output_routes").put("result",tls("a").identity)
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.ca.uri("")),"prepare",body,Some(tls("a").identity))).code==INVALID_ARGUMENT)
      first.withObject("output_routes").putObject("result").put("consumer_identity",tls("a").identity).put("representation","bytes")
      assert(intercept[UnionFailure](f.x.members.remote.action(DacpUri.origin(f.ca.uri("")),"prepare",body,Some(tls("a").identity))).code==INVALID_ARGUMENT)
    }finally f.close()
  }
  test("control dependencies are rejected; zero-output sinks remain valid") {
    val f=new Fixture()
    try {
      f.approved()
      assert(intercept[IllegalArgumentException](Jobs.parse(f.yaml.replace("- id: ta\n","- id: ta\n  depends_on: [read_ta_source]\n"))).getMessage.nonEmpty)
      val script=f.yaml.replace("- id: tb\n","- id: tb\n  params: {sink: true}\n").replace("outputs:\n  result: tb\n","outputs: {}\n")
      f.controlFinished=true
      val id=f.submit(script);assert(f.bytes(id).isEmpty);eventually(f.status(id).path("state").asText=="succeeded")
    }finally f.close()
  }
  test("execution policy is strict and forbid wins before any job starts") {
    val f=new Fixture(materialize=true)
    try {
      f.approved()
      Seq("""{"unexpected":true}""","""{"materialization":{"allowed":false,"default":"materialize"}}""","""{"materialization":{"allowed":false,"tasks":{"ta":{"outputs":{"result":"materialize"}}}}}""","""{"materialization":{"tasks":{"absent":{"outputs":{"result":"materialize"}}}}}""","""{"materialization":{"tasks":{"ta":{"outputs":{"absent":"stream"}}}}}""","""{"materialization":{"default":"auto"}}""","""{"materialization":{"allowed":"false"}}""").foreach(p=>assert(intercept[UnionFailure](f.submitPolicy(Json.parse(p))).code==INVALID_ARGUMENT))
      assert(f.count("ta")==0 && f.x.history.list(0,100).isEmpty)
      val id=f.submitPolicy(Json.parse("""{"materialization":{"allowed":false,"default":"stream"}}"""));assert(f.bytes(id).sameElements(f.inputA++f.inputB));eventually(f.status(id).path("state").asText=="succeeded")
      assert(intercept[UnionFailure](f.replay(id)).code==INVALID_ARGUMENT)
    }finally f.close()
  }
  test("unsupported storage rejects every subjob before any operator starts") {
    val f=new Fixture()
    try {f.approved();val id=f.submitPolicy(policy());eventually(f.status(id).path("state").asText=="failed");assert(f.count("ta")==0 && f.count("tb")==0)}finally f.close()
  }
  test("downstream failure replays committed upstream without rerunning it") {
    val f=new Fixture(materialize=true,allowStranger=true)
    try {
      f.approved();f.failDownstreamOnce=true;val original=f.submitPolicy(policy())
      intercept[FlightRuntimeException](f.bytes(original));eventually(f.status(original).path("state").asText=="failed")
      assert(f.count("ta")==1 && f.count("tb")==1)
      assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,"replay_job",Json.obj().put("job_id",original))).code==UNAUTHORIZED)
      val replay=f.replay(original,Some(Seq("ta")));assert(replay!=original)
      assert(f.bytes(replay).sameElements(f.inputA++f.inputB));eventually(f.status(replay).path("state").asText=="succeeded")
      assert(f.count("ta")==1 && f.count("tb")==2)
      val record=f.x.jobSnapshot(replay).get
      assert(record.path("replay_of").asText==original && record.path("execution_policy")==policy())
      assert(record.path("effective_execution_policy").path("materialization").path("tasks").path("tb").path("outputs").path("result").asText=="stream")
      val again=f.replay(replay);assert(f.bytes(again).sameElements(f.inputA++f.inputB));assert(f.count("ta")==1 && f.count("tb")==3)
    }finally f.close()
  }
  test("automatic retry reuses upstream and callback discards partial result") {
    val f=new Fixture(materialize=true)
    try {
      f.approved();f.failDownstreamOnce=true
      val p=policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",2)
      val id=f.submitPolicy(p);assert(f.bytes(id).sameElements(f.inputA++f.inputB));eventually(f.status(id).path("state").asText=="succeeded")
      assert(f.count("ta")==1 && f.count("tb")==2 && f.retryNotices==1)
      val status=f.status(id);assert(status.path("retry_count").asInt==1 && status.path("max_retries").asInt==2 && status.path("attempts").size()==2)
      assert(status.path("attempts").get(0).path("state").asText=="failed" && status.path("attempts").get(1).path("state").asText=="succeeded")
      val replay=f.replay(id);assert(f.bytes(replay).sameElements(f.inputA++f.inputB));assert(f.count("ta")==1 && f.count("tb")==2)
    }finally f.close()
  }
  test("retry count and timeout fields validate types and server limits") {
    val f=new Fixture(materialize=true,timeoutSeconds=20)
    try {
      f.approved()
      Seq(-1,11).foreach(n=>assert(intercept[UnionFailure](f.submitPolicy(policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",n))).code==INVALID_ARGUMENT))
      Seq("""{"max_retries":"2"}""","""{"max_retries":1.5}""","""{"timeout_seconds":0}""","""{"timeout_seconds":21}""","""{"timeout_seconds":"2"}""","""{"timeout_seconds":1.5}""").foreach(p=>assert(intercept[UnionFailure](f.submitPolicy(Json.parse(p))).code==INVALID_ARGUMENT))
      assert(f.count("ta")==0)
    }finally f.close()
  }
  test("automatic retry stops at the configured bound") {
    val f=new Fixture(materialize=true)
    try {
      f.approved()
      val script=f.yaml.replace(" - id: tb"," - id: tb").replace("  title: 融合跨节点数据","  params: {fail: true}\n  title: 融合跨节点数据")
      val id=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",f.publicYaml(script)).set[JsonNode]("execution_policy",policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",1))).path("job_id").asText
      val r=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id).put("retry_aware",true));try intercept[FlightRuntimeException]{while(r.next().nonEmpty){}}finally r.close()
      eventually(f.status(id).path("state").asText=="failed");assert(f.status(id).path("retry_count").asInt==1 && f.count("tb")==2)
    }finally f.close()
  }
  test("automatic retry never overrides explicit cancellation or revoked authorization") {
    val f=new Fixture(materialize=true)
    try {
      f.approved()
      val script=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}")
      def submit()=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",f.publicYaml(script)).set[JsonNode]("execution_policy",policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",2))).path("job_id").asText
      val cancelled=submit();eventually(f.status(cancelled).path("state").asText=="running")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",cancelled));assert(f.status(cancelled).path("retry_count").asInt==0)
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0)
      val revoked=submit();eventually(f.status(revoked).path("state").asText=="running");f.revokeA()
      eventually(Set("failed","cancelled")(f.status(revoked).path("state").asText));assert(f.status(revoked).path("retry_count").asInt==0)
    }finally f.close()
  }
  test("retry works with all-streaming forbid policy and rejects unaware consumers") {
    val f=new Fixture()
    try {
      f.approved();f.failDownstreamOnce=true
      val id=f.submitPolicy(Json.parse("""{"max_retries":1,"materialization":{"allowed":false}}"""))
      intercept[FlightRuntimeException](f.bytes(id,retryAware=false))
      assert(f.bytes(id).sameElements(f.inputA++f.inputB));eventually(f.status(id).path("state").asText=="succeeded")
      assert(f.retryNotices==1 && f.count("ta")==2 && f.count("tb")==2)
    }finally f.close()
  }
  test("policy timeout limits resource waiting and all attempts, without retrying expiry") {
    val f=new Fixture()
    try {
      f.approved()
      val script=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}")
      val id=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",f.publicYaml(script)).set[JsonNode]("execution_policy",Json.parse("""{"max_retries":2,"timeout_seconds":1}"""))).path("job_id").asText
      val start=System.nanoTime();eventually(f.status(id).path("state").asText=="expired")
      assert((System.nanoTime()-start)/1e9<5 && f.status(id).path("retry_count").asInt==0)
      assert(f.status(id).path("effective_execution_policy").path("timeout_seconds").asInt==1)
      eventually(f.ca.taskSlots.availablePermits()==4 && f.cb.taskSlots.availablePermits()==4)
    }finally f.close()
  }
  test("multi-output replay serves ports concurrently despite opposite input consumption order") {
    val f=new Fixture(materialize=true)
    try {
      f.approved()
      val script=f.yaml.replace("  title: 汇总本地数据","  params: {multi: true}\n  title: 汇总本地数据").replace("left: ta","left: ta:z").replace("right: read_tb_right","right: ta:a")
      val original=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",f.publicYaml(script)).set[JsonNode]("execution_policy",policy(all=true))).path("job_id").asText
      assert(f.bytes(original).sameElements(f.inputA++f.inputA));eventually(f.status(original).path("state").asText=="succeeded")
      val replay=f.replay(original,Some(Seq("ta")));assert(f.bytes(replay).sameElements(f.inputA++f.inputA));eventually(f.status(replay).path("state").asText=="succeeded")
      assert(f.count("ta")==1 && f.count("tb")==2)
    }finally f.close()
  }
  test("materialized final results replay after Union and worker restart without operators or raw inputs") {
    val f=new Fixture(materialize=true)
    var restored:Coordinator=null
    var nextA:UnionWorker=null;var nextB:UnionWorker=null;var nextAF:Endpoint=null;var nextBF:Endpoint=null
    try {
      f.approved();val original=f.submitPolicy(policy(all=true));assert(f.bytes(original).sameElements(f.inputA++f.inputB));eventually(f.status(original).path("state").asText=="succeeded")
      Files.delete(f.dir.resolve("a.bin"));Files.delete(f.dir.resolve("b.bin"))
      f.x.close();f.a.close();f.b.close();f.af.close();f.bf.close()
      nextA=new UnionWorker(f.ca,f.as,f.allocator,Some(f.runner));nextB=new UnionWorker(f.cb,f.bs,f.allocator,Some(f.runner))
      nextAF=new Endpoint("localhost",f.ca.port,tls("a"),f.allocator,f.producer(f.ca,nextA),Some(f.ca.credentials))
      nextBF=new Endpoint("localhost",f.cb.port,tls("b"),f.allocator,f.producer(f.cb,nextB),Some(f.cb.credentials))
      nextAF.start();nextBF.start();nextA.start();nextB.start()
      restored=new Coordinator(f.settings);restored.start()
      val replay=f.replay(original);assert(f.bytes(replay).sameElements(f.inputA++f.inputB));eventually(f.status(replay).path("state").asText=="succeeded")
      assert(f.count("ta")==1 && f.count("tb")==1)
      val record=restored.jobSnapshot(replay).get
      assert(record.path("tasks").elements.asScala.find(_.path("id").asText=="ta").get.path("state").asText=="skipped")
    }finally {if(restored!=null)restored.close();if(nextA!=null)nextA.close();if(nextB!=null)nextB.close();if(nextAF!=null)nextAF.close();if(nextBF!=null)nextBF.close();f.close()}
  }
  private def http(port:Int,path:String,method:String="GET"):java.net.http.HttpResponse[String]={
    val builder=java.net.http.HttpRequest.newBuilder(java.net.URI.create(s"http://127.0.0.1:$port$path")).timeout(java.time.Duration.ofSeconds(5))
    java.net.http.HttpClient.newHttpClient().send(builder.method(method,java.net.http.HttpRequest.BodyPublishers.noBody()).build(),java.net.http.HttpResponse.BodyHandlers.ofString())
  }
  test("node status publishes only a running website URL and Union observes its changes") {
    val f=new Fixture()
    try {
      f.approved()
      def status=f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))
      assert(!status.path("web_enabled").asBoolean && !status.has("web_url"))
      f.ca.runningWebUrl=Some("http://node-a.example.org:19861/")
      assert(status.path("web_url").asText=="http://node-a.example.org:19861/" && status.path("web_enabled").asBoolean)
      eventually(f.x.memberSnapshots.exists(m=>m.path("identity").asText==tls("a").identity && m.path("status").path("web_url").asText=="http://node-a.example.org:19861/"))
      f.ca.runningWebUrl=None
      eventually(f.x.memberSnapshots.exists(m=>m.path("identity").asText==tls("a").identity && m.has("status") && !m.path("status").has("web_url") && !m.path("status").path("web_enabled").asBoolean))
    }finally f.close()
  }
  test("website reads selected members and the public Flight namespace uses opaque dataset names") {
    val webPort=port();val f=new Fixture(allowStranger=true,dynamicCatalog=true,web=Some(WebSettings(port=webPort)))
    try {
      assert(intercept[FlightRuntimeException](f.stranger.catalog(DacpUri.origin(f.ca.uri("")))) .status.code==UNAUTHENTICATED)
      f.approved()
      val publicEntries=f.stranger.catalog(f.settings.publicUrl)
      assert(publicEntries.size==2 && publicEntries.forall(i=>dacp.protocol.Metadata.DataSet.parseFrom(i.getAppMetadata).getName.startsWith("ds-")))
      val id=f.x.members.all.head.path("identity").asText
      val path=s"/members/$id/datasets"
      val first=http(webPort,path)
      assert(first.statusCode()==200 && first.body().contains("公开数据说明"))
      assert(Json.parse(first.body()).path("datasets").get(0).path("description").asText.contains("<script>alert(1)</script>"))
      assert(http(webPort,"/nodes").body().contains("directory-nodes"))
      assert(first.headers().firstValue("Cache-Control").get()=="no-store")
      val sources=Json.parse(http(webPort,"/datasets/sources").body()).path("sources")
      assert(sources.size()==2)
      val unionDirectory=Json.parse(http(webPort,s"/datasets/sources/$id").body())
      assert(unionDirectory.path("datasets").size()==1)
      assert(unionDirectory.path("datasets").elements.asScala.forall(d=>d.path("name").asText.startsWith("ds-") && d.path("uri").asText.startsWith(f.settings.publicUrl+"/ds-")))
      assert(http(webPort,"/datasets").body().contains("catalog-rows"))
      f.catalogVisible=false
      assert(Json.parse(http(webPort,path).body()).path("datasets").size()==0)
      f.catalogVisible=true;f.catalogFailure=true
      assert(http(webPort,path).statusCode()==503)
      assert(Json.parse(http(webPort,"/datasets/sources").body()).path("sources").size()==2)
      assert(http(webPort,s"/datasets/sources/$id").statusCode()==503)
      f.catalogFailure=false
      assert(http(webPort,path).body().contains("公开数据说明"))
      assert(http(webPort,"/members/"+("0"*64)+"/datasets").statusCode()==404)
      assert(http(webPort,"/api/datasets").statusCode()==404)
    }finally f.close()
  }
  test("HTTP dashboard observes Flight executions without claiming outputs and closes its listener") {
    val webPort=port();val f=new Fixture(web=Some(WebSettings(port=webPort)))
    try {
      val page=http(webPort,"/");assert(page.statusCode()==200 && page.body().contains("Collaboration overview"))
      assert(page.headers().firstValue("Content-Security-Policy").get().contains("script-src 'self'"))
      assert(http(webPort,"/app.js").body().contains("/api/overview"))
      assert(http(webPort,"/style.css").statusCode()==200)
      assert(page.body().contains("/vendor/g6-5.1.1.min.js"))
      assert(http(webPort,"/dag.js").body().contains("G6.Graph"))
      val vendor=http(webPort,"/vendor/g6-5.1.1.min.js")
      assert(vendor.statusCode()==200 && vendor.headers().firstValue("Content-Type").get().startsWith("text/javascript"))
      assert(http(webPort,"/vendor/G6-LICENSE.txt").body().contains("MIT"))
      assert(http(webPort,"/vendor/absent.js").statusCode()==404)
      assert(http(webPort,"/api/overview","POST").statusCode()==405)
      assert(http(webPort,"/api/jobs/absent").statusCode()==404)
      assert(http(webPort,"/api/jobs/invalid%2Fid").statusCode()==400)
      f.approved()
      eventually(Json.parse(http(webPort,"/api/overview").body()).path("members").elements.asScala.count(_.path("state").asText=="approved")==2)
      val id=f.submit()
      val details=Json.parse(http(webPort,s"/api/jobs/$id").body())
      assert(details.path("tasks").size()==4)
      assert(details.path("tasks").elements.asScala.find(_.path("id").asText=="ta").get.path("title").asText=="汇总本地数据")
      assert(details.path("tasks").elements.asScala.find(_.path("id").asText=="tb").get.path("inputs").elements.asScala.exists(_.path("task").asText=="ta"))
      assert(details.path("job_yaml").asText.contains("\"tasks\""))
      val statuses=Json.parse(http(webPort,"/api/overview").body()).path("members").elements.asScala.map(_.path("status")).toVector
      assert(statuses.forall(s=>s.path("started_at_ms").asLong>0 && s.path("uptime_ms").asLong>=0))
      val stream=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id))
      try assert(Iterator.continually(stream.next()).takeWhile(_.nonEmpty).map(f=>StreamProtocol.header(OperatorFrames.decode(f.get)).path("type").asText).toVector.last=="complete")finally stream.close()
      eventually(Json.parse(http(webPort,s"/api/jobs/$id").body()).path("state").asText=="succeeded")
      val overview=Json.parse(http(webPort,"/api/overview").body())
      assert(overview.path("jobs").size()==1)
      val detail=Json.parse(http(webPort,s"/api/jobs/$id").body())
      assert(detail.path("nodes").path(tls("a").identity).path("ports").path("ta/outputs/result").path("remote_payload_bytes").asLong==f.inputA.length)
      assert(detail.path("elapsed_ms").asLong>=0)
      val topology=f.x.nodeTopology
      assert(topology.path("participation").path(tls("a").identity).asInt==1)
      assert(!topology.has("union_traffic"))
      val resources=topology.path("members").get(0).path("status").path("resources")
      assert(resources.path("processors").asInt>0 && resources.path("heap_used_bytes").asLong>0)

      val attempt=detail.path("attempts").get(0)
      assert(attempt.path("started_at_ms").asLong>0 && attempt.path("finished_at_ms").asLong>=attempt.path("started_at_ms").asLong)
    }finally f.close()
    intercept[java.net.ConnectException](http(webPort,"/"))
  }
  test("persistent Union and node records survive session shutdown and are browsable after restart") {
    val webPort=port();val f=new Fixture(web=Some(WebSettings(port=webPort)))
    var id=""
    try {
      f.approved();id=f.submit()
      val stream=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id))
      try Iterator.continually(stream.next()).takeWhile(_.nonEmpty).foreach(_=>())finally stream.close()
      eventually(f.status(id).path("state").asText=="succeeded")
      val aRecords=f.ca.executionHistory.list();assert(aRecords.exists(n=>n.path("kind").asText=="union_task" && n.path("task_id").asText=="ta" && n.path("title").asText=="汇总本地数据" && n.path("state").asText=="succeeded"))
      assert(f.cb.executionHistory.list().exists(n=>n.path("task_id").asText=="tb" && n.path("state").asText=="succeeded"))
    }finally f.close()
    val restored=new Coordinator(f.settings)
    try {
      restored.start()
      val page=Json.parse(http(webPort,"/api/history?offset=0&limit=10").body())
      assert(page.path("jobs").get(0).path("job_id").asText==id)
      assert(Json.parse(http(webPort,s"/api/jobs/$id").body()).path("state").asText=="succeeded")
      assert(Json.parse(http(webPort,s"/api/jobs/$id").body()).path("job_yaml").asText.contains("\"tasks\""))
      assert(http(webPort,"/api/history?limit=1001").statusCode()==400)
      assert(http(webPort,"/api/history?limit=1&limit=2").statusCode()==400)
      assert(http(webPort,"/api/history?unknown=bad").statusCode()==400)
    }finally restored.close()
  }
  test("public dashboard permits anonymous non-loopback reads and has no login endpoint") {
    val webPort=port()
    val f=new Fixture(web=Some(WebSettings(host="0.0.0.0",port=webPort)))
    try {
      f.approved()
      assert(http(webPort,"/").statusCode()==200)
      assert(http(webPort,"/api/overview").statusCode()==200)
      assert(http(webPort,"/api/history").statusCode()==200)
      assert(http(webPort,s"/members/${tls("a").identity}/datasets").statusCode()==200)
      assert(http(webPort,"/login").statusCode()==404)
      val client=java.net.http.HttpClient.newHttpClient()
      val response=client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(s"http://127.0.0.1:$webPort/api/overview")).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build(),java.net.http.HttpResponse.BodyHandlers.ofString())
      assert(response.statusCode()==405)
    }finally f.close()
  }
  test("two-node computation resolves dynamic catalogs and uses direct A-to-B stream while X relays final result") {
    val f=new Fixture(dynamicCatalog=true)
    try {
      f.approved()
      val data=f.ca.openDataset("data");try assert(data.list().nonEmpty)finally data.close()
      val id=f.submit()
      val r=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id))
      val output=new java.io.ByteArrayOutputStream();var complete=false
      try while(!complete){val frame=OperatorFrames.decode(r.next().get);StreamProtocol.header(frame).path("type").asText match {
        case "data"=>output.write(frame.data)
        case "complete"=>complete=true
        case "error"=>fail(f.status(id).toPrettyString)
        case _=>()
      }}finally r.close()
      assert(output.toByteArray.sameElements(f.inputA++f.inputB))
      eventually(f.status(id).path("state").asText=="succeeded")
      val states=f.x.jobSnapshot(id).get.path("nodes")
      val a=states.path(tls("a").identity);val b=states.path(tls("b").identity)
      assert(a.path("ports").path("ta/outputs/result").path("remote_payload_bytes").asLong==f.inputA.length)
      assert(b.path("ports").path("tb/inputs/left").path("remote_payload_bytes").asLong==f.inputA.length)
      assert(b.path("ports").path("tb/inputs/right").path("remote_payload_bytes").asLong==0)
      assert(intercept[FlightRuntimeException]{val r=f.client.stream(f.ca.uri(""),"output",Json.obj().put("execution_id",id).put("task","ta").put("port","result"));try r.next()finally r.close()}.status.code()==UNAUTHENTICATED)
      val peer=new Remote(Some(tls("stranger")))
      try assert(intercept[FlightRuntimeException]{val r=peer.stream(f.ca.uri(""),"output",Json.obj().put("execution_id",id).put("task","ta").put("port","result"));try r.next()finally r.close()}.status.code()==UNAUTHORIZED)finally peer.close()
      assert(intercept[FlightRuntimeException]{val r=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id));try r.next()finally r.close()}.status.code()==ALREADY_EXISTS)
      val events=f.client.stream(f.settings.publicUrl,"events",Json.obj().put("job_id",id).put("after",0))
      try {val text=Iterator.continually(events.next()).takeWhile(_.nonEmpty).map(x=>new String(x.get)).mkString;assert(text.contains("progress"));assert(text.contains("succeeded"))}finally events.close()
      val restored=new Members(f.settings);assert(restored.unionId==f.x.members.unionId);assert(restored.all.size==2);restored.close()
    }finally f.close()
  }
  test("CA certificate alone grants neither Union access nor node approval; revoke and cancel stop executions") {
    val f=new Fixture
    try {
      assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,"members")).code== (if(f.strangerToken=="invalid")UNAUTHENTICATED else UNIMPLEMENTED))
      assert(intercept[UnionFailure](f.stranger.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))).code==UNAUTHENTICATED)
      f.approved()
      val waiting=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}"))
      eventually(f.status(waiting).path("state").asText=="running")
      f.revokeA()
      eventually(Set("failed","cancelled")(f.status(waiting).path("state").asText))
      assert(intercept[UnionFailure](f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))).code==UNAUTHENTICATED)
    }finally f.close()
  }
  test("planner rejects fan-out, unknown source nodes and inline operators before scheduling") {
    val f=new Fixture
    try {
      f.approved()
      assert(intercept[UnionFailure](f.submit(f.yaml.replace("right: read_tb_right","right: ta"))).code==INVALID_ARGUMENT)
      assert(intercept[UnionFailure](f.submit(f.yaml.replace(f.cb.uri("data","/input"),"dacp://localhost:1/data/input"))).code==INVALID_ARGUMENT)
      assert(intercept[UnionFailure](f.submit(f.yaml.replace("identity@1.3","{runtime: python@1.0, source: pass}"))).code==INVALID_ARGUMENT)
    }finally f.close()
  }
  test("resource waiting, explicit cancellation, result disconnection and worker failure release reservations") {
    val f=new Fixture
    try {
      f.approved()
      val text=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}")
      assert(f.ca.taskSlots.tryAcquire(4)) // The same slots are used by ordinary Flight tasks.
      val first=try {val id=f.submit(text);eventually(f.status(id).path("state").asText=="waiting_resources");id}finally f.ca.taskSlots.release(4)
      eventually(f.status(first).path("state").asText=="running")
      val second=f.submit(text);eventually(f.status(second).path("state").asText=="waiting_resources")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",first))
      eventually(f.status(second).path("state").asText=="running")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",second))
      val broken=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {fail: true}"))
      eventually(f.status(broken).path("state").asText=="failed")
      // Closing a Flight result stream before completion cancels the graph.
      val disconnect=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {flood: true}"))
      val response=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",disconnect))
      assert(response.next().nonEmpty);response.close()
      eventually(f.status(disconnect).path("state").asText=="cancelled")
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0)
    }finally f.close()
  }
  test("service credential revocation denies new collaboration calls") {
    val f=new Fixture
    try {f.approved();f.revokeA();assert(intercept[UnionFailure](f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))).code==UNAUTHENTICATED)}finally f.close()
  }

  test("token rotation preserves job ownership and revoked tokens reject subsequent calls") {
    val f=new Fixture()
    try {
      val id=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}"))
      val original=f.settings.credentials.list.find(_.path("subject").asText=="alice").get.path("id").asText
      val replacement=f.settings.credentials.rotate(original)
      val rotated=new Remote(None,credential=_=>Some(replacement._2),ca=Some(tls("c").ca))
      try {
        assert(rotated.action(f.settings.publicUrl,"job_status",Json.obj().put("job_id",id)).path("job_id").asText==id)
        assert(intercept[UnionFailure](f.status(id)).code==UNAUTHENTICATED)
        assert(rotated.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",id)).path("state").asText=="cancelled")
      }finally rotated.close()
    }finally f.close()
  }
  test("allowed clients cannot inspect or cancel each other's jobs and wrong peer pins fail TLS") {
    val f=new Fixture(allowStranger=true)
    try {
      f.approved()
      val id=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}"))
      eventually(f.status(id).path("state").asText=="running")
      Seq("job_status","cancel_job").foreach { op=>
        assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,op,Json.obj().put("job_id",id))).code==UNAUTHORIZED)
      }
      Seq("results","events").foreach { kind=>
        val r=f.stranger.stream(f.settings.publicUrl,kind,if(kind=="events")Json.obj().put("job_id",id).put("after",0)else Json.obj().put("job_id",id))
        try assert(intercept[FlightRuntimeException](r.next()).status.code()==UNAUTHORIZED)finally r.close()
      }
      val pinned=new Remote(Some(tls("c")),credential=_=>Some(f.clientToken))
      try intercept[FlightRuntimeException](pinned.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId),pin=Some(tls("b").identity)))finally pinned.close()
      val future=f.client.stream(f.settings.publicUrl,"events",Json.obj().put("job_id",id).put("after",999999))
      try assert(intercept[FlightRuntimeException](future.next()).status.code()==INVALID_ARGUMENT)finally future.close()
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",id))
      assert(f.status(id).path("state").asText=="cancelled")
    }finally f.close()
  }

  test("Flight validates client certificates, server hostnames, strict controls and ticket schemas") {
    val f=new Fixture
    try {
      val anonymous=FlightClient.builder(f.allocator,Location.forGrpcTls("localhost",f.settings.port))
        .trustedCertificates(new java.io.ByteArrayInputStream(Files.readAllBytes(root.resolve("ca.crt")))).build()
      try intercept[FlightRuntimeException] {
        anonymous.doAction(new Action(Rpc.Prefix+"members",Rpc.bytes(Json.obj())),CallOptions.timeout(3,java.util.concurrent.TimeUnit.SECONDS)).hasNext
      } finally anonymous.close()
      val wrongHost=FlightClient.builder(f.allocator,Location.forGrpcTls("localhost",f.settings.port))
        .trustedCertificates(new java.io.ByteArrayInputStream(Files.readAllBytes(root.resolve("ca.crt")))).clientCertificate(new java.io.ByteArrayInputStream(Files.readAllBytes(root.resolve("c.crt"))),new java.io.ByteArrayInputStream(Files.readAllBytes(root.resolve("c.key"))))
        .overrideHostname("invalid.example").build()
      try intercept[FlightRuntimeException] {
        wrongHost.doAction(new Action(Rpc.Prefix+"members",Rpc.bytes(Json.obj())),CallOptions.timeout(3,java.util.concurrent.TimeUnit.SECONDS)).hasNext
      } finally wrongHost.close()
      assert(intercept[UnionFailure](f.client.action(f.settings.publicUrl,"job_status",Json.obj().put("identity",tls("x").identity))).code==INVALID_ARGUMENT)
      val ticket=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id","missing").put("identity",tls("c").identity))
      try assert(intercept[FlightRuntimeException](ticket.next()).status.code()==INVALID_ARGUMENT)finally ticket.close()
      intercept[IllegalArgumentException](NodeUnionSettings.read(Json.obj().put("state_directory","unused").put("port",19861)))
      intercept[IllegalArgumentException](Json.origin("https://localhost:19860"))
      assert(Json.origin("dacp://LOCALHOST")=="dacp://localhost:8815")
    }finally f.close()
  }

  test("an execution deadline cancels the whole graph and releases worker capacity") {
    val f=new Fixture(timeoutSeconds=2)
    try {
      f.approved()
      val id=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n  params: {wait: true}"))
      eventually(Set("expired","failed")(f.status(id).path("state").asText),f.status(id).toPrettyString)
      assert(f.status(id).path("state").asText=="expired",f.status(id).toPrettyString)
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0,"reservations remain: "+f.status(id).toPrettyString)
      eventually(f.x.members.remote.action(f.cb.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0,"reservations remain: "+f.status(id).toPrettyString)
    }finally f.close()
  }

  private def publicClient(f:Fixture,name:String="c"):FlightClient={
    ClientTransport.open(f.allocator,f.settings.publicUrl,Some(tls(name).ca),None,None,()=>Some(if(name=="stranger")f.strangerToken else f.clientToken))
  }
  private def catalog(client:FlightClient):dacp.protocol.Metadata.Directory={
    val values=client.doAction(new Action("dacp.catalog",Array.emptyByteArray)).asScala.toVector
    assert(values.size==1);dacp.protocol.Metadata.Directory.parseFrom(values.head.getBody)
  }
  private def publicJob(client:FlightClient,yaml:String):Array[Byte]={
    val request=dacp.protocol.Metadata.OperatorRequest.newBuilder().setJobYaml(yaml).build()
    val exchange=client.doExchange(FlightDescriptor.command(request.toByteArray));val root=org.apache.arrow.vector.VectorSchemaRoot.create(PluginSchemas.bytes,new org.apache.arrow.memory.RootAllocator())
    // No input frames are needed for a Job; descriptor carries the definition.
    try {
      exchange.getWriter.start(root);exchange.getWriter.completed()
      val data=new java.io.ByteArrayOutputStream();var complete=false
      while(exchange.getReader.next()){
        val frame=OperatorFrames.decode(exchange.getReader.getRoot.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector].get(0));val h=StreamProtocol.header(frame)
        if(h.path("type").asText=="data")data.write(frame.data)
        if(h.path("type").asText=="complete")complete=true
      }
      assert(complete);data.toByteArray
    }finally {val allocator=root.getVector(0).getAllocator;exchange.close();root.close();allocator.close()}
  }
  test("standard DACP discovery, Item bytes, S3 and jobs use only public URIs") {
    val f=new Fixture(allowStranger=true);val client=publicClient(f,"stranger")
    try {
      f.approved();val d=catalog(client);assert(d.getComplete && d.getDatasetsCount==2)
      val entries=d.getDatasetsList.asScala.toVector
      assert(entries.forall(e=>e.getName.matches("ds-[a-z2-9]{4}-[a-z2-9]{4}") && e.getNodeIdentity.isEmpty))
      val entry=entries.find { e=>
        val data=client.getInfo(FlightDescriptor.path("datasets",e.getName,"/input"));dacp.protocol.Metadata.Item.parseFrom(data.getAppMetadata).getTotalSizeBytes==f.inputA.length
      }.get
      val info=client.getInfo(FlightDescriptor.path("datasets",entry.getName,"/input"))
      val ticket=dacp.protocol.Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes)
      assert(ticket.getDataset==entry.getName)
      val bytes=client.getStream(info.getEndpoints.get(0).getTicket);val out=new java.io.ByteArrayOutputStream()
      try while(bytes.next()){val r=bytes.getRoot;(0 until r.getRowCount).foreach(i=>out.write(r.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector].get(i)))}finally bytes.close()
      assert(out.toByteArray.sameElements(f.inputA))
      val key=f.settings.credentials.create("download","s3")
      val uri=SigV4.presignGet(java.net.URI.create(s"https://localhost:${f.x.s3Port.get}/${entry.getName}/input"),key._1.path("id").asText,key._2)
      val trust=java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType);trust.load(null,null)
      val in=Files.newInputStream(root.resolve("ca.crt"));try trust.setCertificateEntry("ca",java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in))finally in.close()
      val tm=javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm);tm.init(trust)
      val ssl=javax.net.ssl.SSLContext.getInstance("TLS");ssl.init(null,tm.getTrustManagers,null)
      val httpBytes=java.net.http.HttpClient.newBuilder().sslContext(ssl).build().send(java.net.http.HttpRequest.newBuilder(uri).build(),java.net.http.HttpResponse.BodyHandlers.ofByteArray())
      assert(httpBytes.statusCode()==200 && httpBytes.body.sameElements(f.inputA))
      val job=s"""job: public-example
    |tasks:
    |- id: read_copied_source
    |  operator: read_item@1.0
    |  inputs: {}
    |  params:
    |    uri: ${entry.getUri}/input
    |    view: bytes
    |- id: copied
    |  operator: identity@1.3
    |  inputs:
    |    source: read_copied_source
    |outputs:
    |  result: copied
    |""".stripMargin
      assert(publicJob(client,job).sameElements(f.inputA))
      assert(catalog(client).getDatasetsList.asScala.map(_.getName).toSet==entries.map(_.getName).toSet)
      assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,"members")).code== (if(f.strangerToken=="invalid")UNAUTHENTICATED else UNIMPLEMENTED))
    }finally {client.close();f.close()}
  }
  test("partial directories preserve mappings across failure and remove them only after confirmed deletion") {
    val f=new Fixture(dynamicCatalog=true);val client=publicClient(f)
    try {
      f.approved();val first=catalog(client);val ids=first.getDatasetsList.asScala.map(_.getName).toSet
      assert(first.getComplete && ids.size==2)
      f.catalogFailure=true
      val failed=catalog(client);assert(!failed.getComplete && failed.getUnavailableSources==2 && failed.getDatasetsCount==0)
      f.catalogFailure=false;assert(catalog(client).getDatasetsList.asScala.map(_.getName).toSet==ids)
      f.catalogVisible=false;assert(catalog(client).getDatasetsCount==0)
      f.catalogVisible=true;assert(catalog(client).getDatasetsList.asScala.map(_.getName).toSet.intersect(ids).isEmpty)
    }finally {client.close();f.close()}
  }

  test("public multi-source operator transfers foreign input to the data-local execution node") {
    val f=new Fixture(allowStranger=true);val client=publicClient(f,"stranger")
    try {
      f.approved();val entries=catalog(client).getDatasetsList.asScala.toVector
      val inputs=entries.zipWithIndex.map{case(e,i)=>s"p$i: read$i"}.mkString(", ")
      val readers=entries.zipWithIndex.map{case(e,i)=>s" - {id: read$i, operator: read_item@1.0, inputs: {}, params: {uri: '${e.getUri}/input', view: bytes}}"}.mkString("\n")
      val job=s"job: joined\ntasks:\n$readers\n - id: join\n   operator: identity@1.3\n   inputs: {$inputs}\noutputs: {result: join}\n"
      val expected=entries.flatMap(e=>{val info=client.getInfo(FlightDescriptor.path("datasets",e.getName,"/input"));if(dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata).getTotalSizeBytes==f.inputA.length)f.inputA.toVector else f.inputB.toVector}).toArray
      assert(publicJob(client,job).sameElements(expected))
      val records=f.x.history.list(0,100);assert(records.head.path("tasks").elements.asScala.find(_.path("id").asText=="join").get.path("node").asText==DacpUri.origin(f.ca.uri("")))
      val status=f.stranger.action(f.settings.publicUrl,"job_status",Json.obj().put("job_id",records.head.path("job_id").asText))
      assert(!status.toString.contains(f.ca.port.toString) && !status.has("nodes") && status.has("attempts"))
    }finally {client.close();f.close()}
  }
  test("configured membership is the sole list and unavailable members do not block startup") {
    val f=new Fixture();try {f.approved();assert(f.x.members.all.size==2);f.af.close();eventually(f.x.members.inspect(f.x.members.all.find(_.path("url").asText==DacpUri.origin(f.ca.uri(""))).get).path("state").asText=="unavailable");assert(f.x.members.all.size==2)}finally f.close()
  }

  test("public records proxy preserves schema and pushes projection and limits to member access") {
    val f=new Fixture(withRecords=true);val client=publicClient(f)
    try {
      f.approved();val e=catalog(client).getDatasets(0)
      val info=client.getInfo(FlightDescriptor.path("datasets",e.getName,"/observations"))
      val metadata=dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata)
      assert(metadata.getViewsList.contains("records") && !metadata.getArrowSchema.isEmpty)
      val scan=dacp.protocol.Metadata.Scan.newBuilder().setProjection(dacp.protocol.Metadata.Scan.Projection.newBuilder().addColumns("temperature")).setLimit(2).build()
      val ticket=dacp.protocol.Metadata.Ticket.newBuilder().setDataset(e.getName).setItem("/observations").setView("records").setScan(scan).build()
      val reader=client.getStream(new Ticket(ticket.toByteArray))
      try {var rows=0;while(reader.next()){assert(reader.getRoot.getSchema.getFields.asScala.map(_.getName)==Seq("temperature"));rows+=reader.getRoot.getRowCount};assert(rows==2)}finally reader.close()
      val listing=dacp.protocol.Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets",e.getName)).getAppMetadata)
      assert(listing.getUri==e.getUri && listing.getItemsList.asScala.exists(_.getPath=="/observations"))
    }finally {client.close();f.close()}
  }

}
