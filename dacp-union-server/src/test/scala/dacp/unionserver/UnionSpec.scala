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
  private final class Fixture(allowStranger:Boolean=false,timeoutSeconds:Long=60,web:Option[WebSettings]=None,dynamicCatalog:Boolean=false,materialize:Boolean=false) {
    @volatile var catalogVisible=true
    @volatile var catalogFailure=false
    val allocator=new RootAllocator()
    val dir=Files.createTempDirectory(root,"fixture-")
    val inputA=Array.fill[Byte](300000)(65.toByte);val inputB="local-B".getBytes
    Files.write(dir.resolve("a.bin"),inputA);Files.write(dir.resolve("b.bin"),inputB)
    def store(name:String)=if(materialize)Some(MaterializationStoreDefinition("file",Map("directory"->dir.resolve(name+"-outputs").toString)))else None
    val as=NodeUnionSettings(dir.resolve("a-state"),30,30,store("a"))
    val bs=NodeUnionSettings(dir.resolve("b-state"),30,30,store("b"))
    val invocations=new java.util.concurrent.ConcurrentHashMap[String,java.util.concurrent.atomic.AtomicInteger]()
    @volatile var failDownstreamOnce=false
    def count(task:String):Int=Option(invocations.get(task)).map(_.get).getOrElse(0)
    def config(name:String):Config={
      val definition=FileDatasets.mixed(Seq(PublishedItem("/input",file=Some(dir.resolve(s"$name.bin"))))).copy(description="""{"summary":"公开数据说明","html":"<script>alert(1)</script>"}""")
      val c=Config("localhost",port(),"localhost",if(dynamicCatalog)Map.empty else Map("data"->definition),root.resolve(s"$name.crt"),root.resolve(s"$name.key"),root.resolve("ca.crt"),maxConcurrentTasks=4,executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->(dir.resolve(s"$name-executions")).toString))))
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
      if(task.params.containsKey("multi")){
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
    val xp=port();val settings=Settings("localhost",xp,s"dacp://localhost:$xp",root.resolve("x.crt"),root.resolve("x.key"),root.resolve("ca.crt"),dir.resolve("x-state"),(if(allowStranger)Set(tls("c").identity,tls("stranger").identity)else Set(tls("c").identity)),100,timeoutSeconds,30,web)
    val x=new Coordinator(settings);val client=new Remote(tls("c"));val stranger=new Remote(tls("stranger"))
    val af=new Endpoint("localhost",ca.port,tls("a"),allocator,new Producer(ca,allocator,union=Some(a)))
    val bf=new Endpoint("localhost",cb.port,tls("b"),allocator,new Producer(cb,allocator,union=Some(b)))
    af.start();bf.start();a.start();b.start();x.start()
    x.members.invite(ca.uri(""));x.members.invite(cb.uri(""))
    def approve(state:Path,origin:String):Unit={
      val p=Json.obj().put("scheduler_identity",tls("x").identity).put("max_concurrent_tasks",4).put("expires_at_ms",Json.now+180000)
      p.putArray("input_prefixes").add(origin+"/data");p.putArray("operators").add("identity@1.3")
      new UnionMembership(state).approve(x.members.unionId,p)
    }
    def approved():Unit={approve(as.state,DacpUri.origin(ca.uri("")));approve(bs.state,DacpUri.origin(cb.uri("")))}
    val yaml=s"""job: union-example
      |tasks:
      | - id: ta
      |   title: 汇总本地数据
      |   node: ${DacpUri.origin(ca.uri(""))}
      |   operator: identity@1.3
      |   inputs: {source: {uri: '${ca.uri("data","/input")}'}}
      | - id: tb
      |   title: 融合跨节点数据
      |   node: ${DacpUri.origin(cb.uri(""))}
      |   operator: identity@1.3
      |   inputs:
      |     left: {task: ta, port: result}
      |     right: {uri: '${cb.uri("data","/input")}'}
      |outputs: {result: {task: tb, port: result}}
      |""".stripMargin
    def submit(text:String=yaml):String=client.action(settings.publicUrl,"submit",Json.obj().put("job_yaml",text)).path("job_id").asText
    def submitPolicy(policy:JsonNode):String=client.action(settings.publicUrl,"submit",Json.obj().put("job_yaml",yaml).set[JsonNode]("execution_policy",policy)).path("job_id").asText
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
      val script=f.yaml.replace(" - id: tb"," - id: tb").replace("   title: 融合跨节点数据","   params: {fail: true}\n   title: 融合跨节点数据")
      val id=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",script).set[JsonNode]("execution_policy",policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",1))).path("job_id").asText
      val r=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id).put("retry_aware",true));try intercept[FlightRuntimeException]{while(r.next().nonEmpty){}}finally r.close()
      eventually(f.status(id).path("state").asText=="failed");assert(f.status(id).path("retry_count").asInt==1 && f.count("tb")==2)
    }finally f.close()
  }
  test("automatic retry never overrides explicit cancellation or revoked authorization") {
    val f=new Fixture(materialize=true)
    try {
      f.approved()
      val script=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}")
      def submit()=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",script).set[JsonNode]("execution_policy",policy(all=true).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("max_retries",2))).path("job_id").asText
      val cancelled=submit();eventually(f.status(cancelled).path("state").asText=="running")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",cancelled));assert(f.status(cancelled).path("retry_count").asInt==0)
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0)
      val revoked=submit();eventually(f.status(revoked).path("state").asText=="running");new UnionMembership(f.as.state).revoke(f.x.members.unionId)
      eventually(f.status(revoked).path("state").asText=="failed");assert(f.status(revoked).path("retry_count").asInt==0)
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
      val script=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}")
      val id=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",script).set[JsonNode]("execution_policy",Json.parse("""{"max_retries":2,"timeout_seconds":1}"""))).path("job_id").asText
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
      val script=f.yaml.replace(" - id: tb","   params: {multi: true}\n - id: tb").replace("left: {task: ta, port: result}","left: {task: ta, port: z}").replace("right: {uri: '"+f.cb.uri("data","/input")+"'}","right: {task: ta, port: a}")
      val original=f.client.action(f.settings.publicUrl,"submit",Json.obj().put("job_yaml",script).set[JsonNode]("execution_policy",policy(all=true))).path("job_id").asText
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
      nextAF=new Endpoint("localhost",f.ca.port,tls("a"),f.allocator,new Producer(f.ca,f.allocator,union=Some(nextA)))
      nextBF=new Endpoint("localhost",f.cb.port,tls("b"),f.allocator,new Producer(f.cb,f.allocator,union=Some(nextB)))
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
  test("website reads only a selected member online; Union exposes no Flight directory") {
    val webPort=port();val f=new Fixture(dynamicCatalog=true,web=Some(WebSettings(port=webPort)))
    try {
      assert(f.stranger.catalog(DacpUri.origin(f.ca.uri(""))).size==1)
      intercept[FlightRuntimeException](f.stranger.catalog(f.settings.publicUrl))
      val id=f.x.members.all.head.path("identity").asText
      val path=s"/members/$id/datasets"
      val first=http(webPort,path)
      assert(first.statusCode()==200 && first.body().contains("公开数据说明"))
      assert(!first.body().contains("<script>alert(1)</script>") && first.body().contains("&lt;script&gt;"))
      assert(first.headers().firstValue("Cache-Control").get()=="no-store")
      f.catalogVisible=false
      assert(http(webPort,path).body().contains("该节点暂无数据集"))
      f.catalogVisible=true;f.catalogFailure=true
      assert(http(webPort,path).statusCode()==503)
      f.catalogFailure=false
      assert(http(webPort,path).body().contains("公开数据说明"))
      assert(http(webPort,"/members/"+("0"*64)+"/datasets").statusCode()==404)
      assert(http(webPort,"/api/datasets").statusCode()==404)
    }finally f.close()
  }
  test("HTTP dashboard observes Flight executions without claiming outputs and closes its listener") {
    val webPort=port();val f=new Fixture(web=Some(WebSettings(port=webPort)))
    try {
      val page=http(webPort,"/");assert(page.statusCode()==200 && page.body().contains("协作运行概览"))
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
      assert(details.path("tasks").size()==2)
      assert(details.path("tasks").get(0).path("title").asText=="汇总本地数据")
      assert(details.path("tasks").get(1).path("inputs").elements.asScala.exists(_.path("task").asText=="ta"))
      assert(details.path("job_yaml").asText.contains("tasks:"))
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
      assert(Json.parse(http(webPort,s"/api/jobs/$id").body()).path("job_yaml").asText.contains("tasks:"))
      assert(http(webPort,"/api/history?limit=1001").statusCode()==400)
      assert(http(webPort,"/api/history?limit=1&limit=2").statusCode()==400)
      assert(http(webPort,"/api/history?unknown=bad").statusCode()==400)
    }finally restored.close()
  }
  test("HTTP dashboard is public on a non-loopback listener and remains read-only") {
    val webPort=port();val f=new Fixture(web=Some(WebSettings(host="0.0.0.0",port=webPort)))
    try {
      assert(http(webPort,"/").statusCode()==200)
      assert(!http(webPort,"/").body().contains("login-form"))
      assert(http(webPort,"/").body().contains("/assets/dacp-logo.png"))
      val logo=java.net.http.HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(s"http://127.0.0.1:$webPort/assets/dacp-logo.png")).build(),java.net.http.HttpResponse.BodyHandlers.ofByteArray())
      assert(logo.statusCode()==200 && logo.headers().firstValue("Content-Type").get()=="image/png")
      assert(logo.body().sameElements(Files.readAllBytes(java.nio.file.Path.of("../assets/dacp-logo.png"))))
      assert(http(webPort,"/api/overview").statusCode()==200)
      assert(http(webPort,"/api/history").statusCode()==200)
      assert(http(webPort,"/api/overview","POST").statusCode()==405)
      assert(http(webPort,"/api/overview").headers().firstValue("Cache-Control").get()=="no-store")
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
      val states=f.status(id).path("nodes")
      val a=states.path(tls("a").identity);val b=states.path(tls("b").identity)
      assert(a.path("ports").path("ta/outputs/result").path("remote_payload_bytes").asLong==f.inputA.length)
      assert(b.path("ports").path("tb/inputs/left").path("remote_payload_bytes").asLong==f.inputA.length)
      assert(b.path("ports").path("tb/inputs/right").path("remote_payload_bytes").asLong==0)
      assert(intercept[FlightRuntimeException]{val r=f.client.stream(f.ca.uri(""),"output",Json.obj().put("execution_id",id).put("task","ta").put("port","result"));try r.next()finally r.close()}.status.code()==UNAUTHORIZED)
      assert(intercept[FlightRuntimeException]{val r=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",id));try r.next()finally r.close()}.status.code()==ALREADY_EXISTS)
      val events=f.client.stream(f.settings.publicUrl,"events",Json.obj().put("job_id",id).put("after",0))
      try {val text=Iterator.continually(events.next()).takeWhile(_.nonEmpty).map(x=>new String(x.get)).mkString;assert(text.contains("progress"));assert(text.contains("succeeded"))}finally events.close()
      val restored=new Members(f.settings);assert(restored.unionId==f.x.members.unionId);assert(restored.all.size==2);restored.close()
    }finally f.close()
  }
  test("CA certificate alone grants neither Union access nor node approval; revoke and cancel stop executions") {
    val f=new Fixture
    try {
      assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,"members")).code==UNAUTHORIZED)
      assert(intercept[UnionFailure](f.stranger.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))).code==UNAUTHORIZED)
      val pending=f.submit();eventually(f.status(pending).path("state").asText=="failed")
      f.approved()
      val waiting=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}"))
      eventually(f.status(waiting).path("state").asText=="running")
      new UnionMembership(f.as.state).revoke(f.x.members.unionId)
      eventually(f.status(waiting).path("state").asText=="failed")
      val failed=f.submit();eventually(f.status(failed).path("state").asText=="failed")
      val other=f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",failed));assert(other.path("state").asText=="failed")
    }finally f.close()
  }
  test("planner rejects fan-out and cross-node raw inputs before scheduling") {
    val f=new Fixture
    try {
      f.approved()
      assert(intercept[UnionFailure](f.submit(f.yaml.replace("right: {uri: '"+f.cb.uri("data","/input")+"'}","right: {task: ta, port: result}"))).code==INVALID_ARGUMENT)
      assert(intercept[UnionFailure](f.submit(f.yaml.replace(f.cb.uri("data","/input"),f.ca.uri("data","/input")))).code==INVALID_ARGUMENT)
      assert(intercept[UnionFailure](f.submit(f.yaml.replace("identity@1.3","{runtime: python@1.0, source: pass}"))).code==INVALID_ARGUMENT)
    }finally f.close()
  }
  test("resource waiting, explicit cancellation, result disconnection and worker failure release reservations") {
    val f=new Fixture
    try {
      def limited(state:Path,origin:String):Unit={
        val p=Json.obj().put("scheduler_identity",tls("x").identity).put("max_concurrent_tasks",1).put("expires_at_ms",Json.now+180000)
        p.putArray("input_prefixes").add(origin+"/data");p.putArray("operators").add("identity@1.3")
        new UnionMembership(state).approve(f.x.members.unionId,p)
      }
      limited(f.as.state,DacpUri.origin(f.ca.uri("")));limited(f.bs.state,DacpUri.origin(f.cb.uri("")))
      val text=f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}")
      assert(f.ca.taskSlots.tryAcquire(4)) // The same slots are used by ordinary Flight tasks.
      val first=try {val id=f.submit(text);eventually(f.status(id).path("state").asText=="waiting_resources");id}finally f.ca.taskSlots.release(4)
      eventually(f.status(first).path("state").asText=="running")
      val second=f.submit(text);eventually(f.status(second).path("state").asText=="waiting_resources")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",first))
      eventually(f.status(second).path("state").asText=="running")
      f.client.action(f.settings.publicUrl,"cancel_job",Json.obj().put("job_id",second))
      val broken=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {fail: true}"))
      eventually(f.status(broken).path("state").asText=="failed")
      // Closing a Flight result stream before completion cancels the graph.
      val disconnect=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {flood: true}"))
      val response=f.client.stream(f.settings.publicUrl,"results",Json.obj().put("job_id",disconnect))
      assert(response.next().nonEmpty);response.close()
      eventually(f.status(disconnect).path("state").asText=="cancelled")
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0)
    }finally f.close()
  }
  test("expired grants deny scheduling and removing a member withdraws the node authorization") {
    val f=new Fixture
    try {
      f.approved()
      f.x.members.remove(tls("a").identity)
      assert(new UnionMembership(f.as.state).get(f.x.members.unionId).path("state").asText=="revoked")
      assert(intercept[UnionFailure](f.submit()).code==INVALID_ARGUMENT)
      val policy=new UnionMembership(f.bs.state).get(f.x.members.unionId).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      policy.put("expires_at_ms",Json.now-1)
      Json.write(f.bs.state.resolve(f.x.members.unionId+".json"),policy)
      assert(intercept[UnionFailure](f.x.members.remote.action(f.cb.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId))).code==UNAUTHORIZED)
    }finally f.close()
  }

  test("allowed clients cannot inspect or cancel each other's jobs and wrong peer pins fail TLS") {
    val f=new Fixture(allowStranger=true)
    try {
      f.approved()
      val id=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}"))
      eventually(f.status(id).path("state").asText=="running")
      Seq("job_status","cancel_job").foreach { op=>
        assert(intercept[UnionFailure](f.stranger.action(f.settings.publicUrl,op,Json.obj().put("job_id",id))).code==UNAUTHORIZED)
      }
      Seq("results","events").foreach { kind=>
        val r=f.stranger.stream(f.settings.publicUrl,kind,if(kind=="events")Json.obj().put("job_id",id).put("after",0)else Json.obj().put("job_id",id))
        try assert(intercept[FlightRuntimeException](r.next()).status.code()==UNAUTHORIZED)finally r.close()
      }
      intercept[FlightRuntimeException](f.client.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId),pin=Some(tls("b").identity)))
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
      assert(intercept[UnionFailure](f.client.action(f.settings.publicUrl,"members",Json.obj().put("identity",tls("x").identity))).code==INVALID_ARGUMENT)
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
      val id=f.submit(f.yaml.replace("operator: identity@1.3","operator: identity@1.3\n   params: {wait: true}"))
      eventually(Set("expired","failed")(f.status(id).path("state").asText),f.status(id).toPrettyString)
      assert(f.status(id).path("state").asText=="expired",f.status(id).toPrettyString)
      eventually(f.x.members.remote.action(f.ca.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0,"reservations remain: "+f.status(id).toPrettyString)
      eventually(f.x.members.remote.action(f.cb.uri(""),"node_status",Json.obj().put("union_id",f.x.members.unionId)).path("reserved_tasks").asInt==0,"reservations remain: "+f.status(id).toPrettyString)
    }finally f.close()
  }

}
