package dacp

import _root_.xstream.connector.dacp.wire.{ClientTransport,Tls}

import _root_.xstream.engine.{Jobs}
import _root_.xstream.execution.{ExecutionStoreDefinition,StreamCancel,StreamProtocol}

import _root_.xstream.data._

import dacp.union._
import java.nio.file.Files
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class SecuritySpec extends AnyFunSuite {
  private def fixture(run:(Config,DacpServer,RootAllocator)=>Unit):Unit={
    val dir=Files.createTempDirectory("security-contract-");val socket=new java.net.ServerSocket(0);val port=socket.getLocalPort;socket.close()
    val catalog=new DataCatalog {
      def list():Seq[DatasetInfo]=Seq(DatasetInfo("live"))
      def open(name:String):PublishedDataset=new PublishedDataset {
        def open(check:()=>Unit):DatasetAccess=new BatchDatasetAccess {
          def list():Seq[Item]=Seq(describe("/ticks"))
          def list(p:String):Seq[Item]=list()
          def describe(p:String):Item=Item(p,"binary",None,views=Some(Seq("bytes")))
          def count(p:String):Option[Long]=None
          def read(p:String,v:String,a:org.apache.arrow.memory.BufferAllocator,l: BatchReceiver,ready:()=>Unit):Unit={
            val root=VectorSchemaRoot.create(PluginSchemas.bytes,a)
            try {root.allocateNew();l.start(root);var n=0
              while(true){check();ready();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,n.toString.getBytes("UTF-8"));root.setRowCount(1);l.putNext();n+=1;Thread.sleep(20)}
            }finally root.close()
          }
        }
      }
    }
    val c=Config("localhost",port,"localhost",Map.empty,SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,catalogOverride=Some(catalog),credentialDirectory=Some(dir.resolve("credentials")),web=NodeWebSettings(enabled=false),s3=S3Settings(enabled=false),accessLogDirectory=Some(dir.resolve("access")),executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->dir.resolve("history").toString))))
    c.credentials.setRoles("alice",Set("reader"))
    val server=DacpServer.builder().config(c).build().start();val allocator=new RootAllocator()
    try run(c,server,allocator)finally {server.close();allocator.close();Data.deleteTree(dir)}
  }
  private def client(c:Config,a:RootAllocator,token:()=>Option[String],cert:Boolean=false)=ClientTransport.open(a,c.uri(""),Some(c.ca),if(cert)Some(c.cert)else None,if(cert)Some(c.key)else None,token)
  private def stream(c:FlightClient):FlightStream={val info=c.getInfo(FlightDescriptor.path("datasets","live","/ticks"));c.getStream(info.getEndpoints.get(0).getTicket)}
  test("capabilities distinguish public metadata, reader access and runner submission") {fixture{(c,_,a)=>
    def capabilities(client:FlightClient)= {
      val results=client.doAction(new Action("dacp.capabilities",Array.emptyByteArray)).asScala.toVector
      assert(results.size==1);Data.mapper.readTree(results.head.getBody)
    }
    val anonymous=client(c,a,()=>None)
    val issued=c.credentials.create("alice");val reader=client(c,a,()=>Some(issued._2))
    try {
      assert(capabilities(anonymous).path("permissions").path("catalog_read").asBoolean)
      assert(!capabilities(anonymous).path("permissions").path("job_submit").asBoolean)
      assert(capabilities(reader).path("permissions").path("item_read").asBoolean)
      assert(!capabilities(reader).path("permissions").path("job_submit").asBoolean)
      c.credentials.setRoles("alice",Set("runner"))
      assert(capabilities(reader).path("permissions").path("job_submit").asBoolean)
    }finally {anonymous.close();reader.close()}
  }}
  test("DACP client reports caller rights, submits results and cancels remotely") {fixture{(c,_,a)=>
    val dir=Files.createTempDirectory("client-credential-")
    val issued=c.credentials.create("alice");val token=dir.resolve("token");Files.writeString(token,issued._2)
    val tls=dacp.client.TlsConfig(ca=Some(c.ca),tokenFile=Some(token))
    val origin=c.uri("").stripSuffix("/")
    try {
      assert(!dacp.client.Source.capabilities(origin,tls).path("permissions").path("job_submit").asBoolean)
      c.credentials.setRoles("alice",Set("runner"))
      assert(dacp.client.Source.capabilities(origin,tls).path("permissions").path("job_submit").asBoolean)
      val job="name: Remote source\nsteps:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'context:/live/ticks', view: bytes}\noutputs: {result: read}\n"
      val stream=dacp.client.Job.run(origin,job,tls,30)
      try {
        assert(stream.next().kind=="execution")
        assert(stream.next().kind=="start")
        assert(stream.next().kind=="data")
        assert(stream.executionId.nonEmpty)
      }finally stream.close()
    }finally Data.deleteTree(dir)
  }}
  test("remote Item provider reads with reader credentials without needing job submission") {fixture{(c,_,a)=>
    val dir=Files.createTempDirectory("item-credential-")
    val issued=c.credentials.create("alice");val token=dir.resolve("token");Files.writeString(token,issued._2)
    val settings=_root_.xstream.util.Json.obj()
    settings.putArray("nodes").addObject().put("url",c.uri("").stripSuffix("/")).put("ca",c.ca.toString).put("token_file",token.toString)
    val provider=new _root_.xstream.connector.dacp.DacpRemoteItemProvider()
    val environment=new PluginEnvironment(registrations=Seq(PluginRegistration(provider)))
    val cancel=new StreamCancel()
    try {
      environment.byId(classOf[_root_.xstream.execution.RemoteItemProvider],"dacp")
      val source=provider.open(settings,_root_.xstream.execution.RemoteItemRequest(c.uri("live/ticks"),"bytes",None,None),a,cancel)
      try {
        assert(StreamProtocol.header(source.next().get).path("type").asText=="start")
        assert(StreamProtocol.header(source.next().get).path("type").asText=="data")
      }finally source.close()
    }finally {cancel.close();environment.close();provider.close();Data.deleteTree(dir)}
  }}
  test("metadata is public; certificates alone cannot read data and S3 keys cannot authenticate Flight") {fixture{(c,_,a)=>
    val issued=c.credentials.create("alice");val good=client(c,a,()=>Some(issued._2))
    val peer=client(c,a,()=>None,true);val s3=c.credentials.create("s3","s3");val wrong=client(c,a,()=>Some(s3._2))
    try {
      assert(good.listFlights(Criteria.ALL).asScala.size==1)
      assert(peer.listFlights(Criteria.ALL).asScala.size==1)
      val info=peer.getInfo(FlightDescriptor.path("datasets","live","/ticks"))
      val denied=peer.getStream(info.getEndpoints.get(0).getTicket)
      try assert(intercept[FlightRuntimeException](denied.next()).status.code==FlightStatusCode.UNAUTHORIZED)finally denied.close()
      assert(intercept[FlightRuntimeException](wrong.listFlights(Criteria.ALL).asScala.toVector).status.code==FlightStatusCode.UNAUTHENTICATED)
      val audit=Files.readString(c.credentials.directory.resolve("authentication-audit.jsonl"))
      assert(audit.contains("UNAUTHENTICATED") && !audit.contains(issued._2) && !audit.contains(s3._2))
    }finally {good.close();peer.close();wrong.close()}
  }}
  test("anonymous metadata needs no CA while payload authorization remains enforced") {fixture{(c,_,a)=>
    val public=ClientTransport.open(a,c.uri(""),None,None,None,()=>None,allowAnonymousWithoutCa=true)
    try {
      assert(public.listFlights(Criteria.ALL).asScala.size==1)
      val info=public.getInfo(FlightDescriptor.path("datasets","live","/ticks"))
      val denied=public.getStream(info.getEndpoints.get(0).getTicket)
      try assert(intercept[FlightRuntimeException](denied.next()).status.code==FlightStatusCode.UNAUTHORIZED)finally denied.close()
    }finally public.close()
  }}
  test("remote-serving environments reject local operators even in DEBUG mode") {fixture{(c,_,_)=>
    val task=Jobs.parse("name: Local attempt\nsteps:\n- id: run\n  operator: {local: /tmp/private-operator}\n  inputs: {}\noutputs: {result: run}\n").steps.head
    assert(intercept[IllegalArgumentException](Jobs.authorizeSource(c,task)).getMessage.contains("local engine"))
    assert(intercept[IllegalArgumentException](Jobs.authorizeSource(c.copy(debug=true),task)).getMessage.contains("local engine"))
    intercept[IllegalArgumentException](WorkflowInference.taskJson(task,Map.empty))
  }}
  test("ordinary revocation rejects subsequent RPCs on the same connection and preserves an admitted stream") {fixture{(c,_,a)=>
    val issued=c.credentials.create("alice");val connection=client(c,a,()=>Some(issued._2));val reader=stream(connection)
    try {
      assert(reader.next());c.credentials.revoke(issued._1.path("id").asText)
      assert(intercept[FlightRuntimeException](connection.listFlights(Criteria.ALL).asScala.toVector).status.code==FlightStatusCode.UNAUTHENTICATED)
      assert(reader.next());assert(reader.next())
    }finally {reader.cancel("test complete",null);reader.close();connection.close()}
  }}
  test("Union service credentials require their certificate and revocation terminates admitted streams") {fixture{(c,_,a)=>
    val issued=c.credentials.create("union","union",unionId="u",fingerprint=new Tls(c.cert,c.key,c.ca).identity)
    val bad=client(c,a,()=>Some(issued._2));val good=client(c,a,()=>Some(issued._2),true)
    try {
      assert(intercept[FlightRuntimeException](bad.listFlights(Criteria.ALL).asScala.toVector).status.code==FlightStatusCode.UNAUTHENTICATED)
      val reader=stream(good)
      try {assert(reader.next());c.credentials.revoke(issued._1.path("id").asText);intercept[FlightRuntimeException]{while(reader.next())()}}finally reader.close()
    }finally {bad.close();good.close()}
  }}

  test("anonymous metadata includes nested listings and schemas, but not count, inference or payload") {fixture{(c,_,a)=>
    val connection=client(c,a,()=>None)
    try {
      assert(connection.listFlights(Criteria.ALL).asScala.size==1)
      assert(connection.getInfo(FlightDescriptor.path("datasets","live")).getAppMetadata.nonEmpty)
      assert(connection.getInfo(FlightDescriptor.path("datasets","live","list","/folder")).getAppMetadata.nonEmpty)
      val info=connection.getInfo(FlightDescriptor.path("datasets","live","/ticks"))
      assert(info.getSchema!=null)
      assert(connection.doAction(new Action("dacp.catalog",Array.emptyByteArray)).hasNext)
      assert(intercept[FlightRuntimeException](connection.getInfo(FlightDescriptor.path("datasets","live","count","/ticks"))).status.code==FlightStatusCode.UNAUTHORIZED)
      assert(intercept[FlightRuntimeException](connection.doAction(new Action("dacp.infer","{}".getBytes("UTF-8"))).next()).status.code==FlightStatusCode.UNAUTHORIZED)
      val denied=connection.getStream(info.getEndpoints.get(0).getTicket)
      try assert(intercept[FlightRuntimeException](denied.next()).status.code==FlightStatusCode.UNAUTHORIZED)finally denied.close()
    }finally connection.close()
  }}
  test("subject roles change subsequent RPC permissions without replacing tokens") {fixture{(c,_,a)=>
    val token=c.credentials.create("visitor")._2;val connection=client(c,a,()=>Some(token))
    def inferenceStatus=intercept[FlightRuntimeException](connection.doAction(new Action("dacp.infer","{}".getBytes("UTF-8"))).next()).status.code
    def deniedRead():Unit={val reader=stream(connection);try assert(intercept[FlightRuntimeException](reader.next()).status.code==FlightStatusCode.UNAUTHORIZED)finally reader.close()}
    try {
      deniedRead();assert(inferenceStatus==FlightStatusCode.UNAUTHORIZED)
      c.credentials.setRoles("visitor",Set("reader"))
      val first=stream(connection);try assert(first.next())finally{first.cancel("done",null);first.close()}
      assert(inferenceStatus==FlightStatusCode.UNAUTHORIZED)
      c.credentials.setRoles("visitor",Set("runner"))
      assert(inferenceStatus!=FlightStatusCode.UNAUTHORIZED)
      val inherited=stream(connection);try assert(inherited.next())finally{inherited.cancel("done",null);inherited.close()}
      val rotated=c.credentials.rotate(c.credentials.list.find(_.path("subject").asText=="visitor").get.path("id").asText)
      val next=client(c,a,()=>Some(rotated._2))
      try {val reader=stream(next);try assert(reader.next())finally{reader.cancel("done",null);reader.close()}}finally next.close()
      c.credentials.setRoles("visitor",Set.empty)
      val noRoles=client(c,a,()=>Some(rotated._2));try {val reader=stream(noRoles);try assert(intercept[FlightRuntimeException](reader.next()).status.code==FlightStatusCode.UNAUTHORIZED)finally reader.close()}finally noRoles.close()
    }finally connection.close()
  }}
  test("subject roles persist independently, reload and reject invalid roles") {
    val dir=Files.createTempDirectory("role-contract-");val store=new Credentials(dir);val other=new Credentials(dir)
    try {
      val token=store.create("alice");val credential=store.authenticate(token._2,None).get
      assert(store.permissions(credential).isEmpty)
      store.setRoles("alice",Set("runner"))
      val until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(3)
      while(other.permissions(credential).isEmpty && System.nanoTime()<until)Thread.sleep(20)
      assert(other.permissions(credential)==Set("data:read","job:execute","job:manage:self"))
      assert(!Files.readString(dir.resolve("credentials.json")).contains("runner"))
      intercept[IllegalArgumentException](store.setRoles("alice",Set("admin")))
      store.setRoles("alice",Set.empty)
      assert(store.permissions(credential).isEmpty)
      val s3=store.create("download","s3")
      val union=store.create("service","union",unionId="u",fingerprint="a"*64)
      Files.writeString(dir.resolve("subjects.json"),"invalid")
      Thread.sleep(600);assert(other.authenticate(token._2,None).nonEmpty && other.permissions(credential).isEmpty)
      assert(other.s3(s3._1.path("id").asText).nonEmpty)
      assert(other.permissions(other.s3(s3._1.path("id").asText).get).isEmpty)
      assert(other.authenticate(union._2,Some("a"*64)).nonEmpty)
    }finally{store.close();other.close();Data.deleteTree(dir)}
  }
  test("credential persistence uses digests, rotation preserves subject, reload fails closed") {
    val dir=Files.createTempDirectory("credential-contract-");val store=new Credentials(dir);val other=new Credentials(dir)
    try {
      val issued=store.create("alice");assert(!Files.readString(dir.resolve("credentials.json")).contains(issued._2))
      val end=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(3)
      while(other.authenticate(issued._2,None).isEmpty && System.nanoTime()<end)Thread.sleep(20)
      assert(other.authenticate(issued._2,None).get.subject=="alice")
      val rotated=store.rotate(issued._1.path("id").asText)
      assert(store.authenticate(issued._2,None).isEmpty && store.authenticate(rotated._2,None).get.subject=="alice")
      Files.writeString(dir.resolve("credentials.json"),"invalid")
      Thread.sleep(600);assert(other.authenticate(rotated._2,None).isEmpty)
    }finally {store.close();other.close();Data.deleteTree(dir)}
  }
}
