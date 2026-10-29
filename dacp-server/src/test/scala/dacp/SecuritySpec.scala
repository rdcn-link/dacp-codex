package dacp

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
        def open(check:()=>Unit):DatasetAccess=new DatasetAccess {
          def list():Seq[Item]=Seq(describe("/ticks"))
          def list(p:String):Seq[Item]=list()
          def describe(p:String):Item=Item(p,"binary",None,views=Some(Seq("bytes")))
          def count(p:String):Option[Long]=None
          def stream(p:String,v:String,a:org.apache.arrow.memory.BufferAllocator,l:FlightProducer.ServerStreamListener,ready:()=>Unit):Unit={
            val root=VectorSchemaRoot.create(PluginSchemas.bytes,a)
            try {root.allocateNew();l.start(root);var n=0
              while(true){check();ready();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,n.toString.getBytes("UTF-8"));root.setRowCount(1);l.putNext();n+=1;Thread.sleep(20)}
            }finally root.close()
          }
        }
      }
    }
    val c=Config("localhost",port,"localhost",Map.empty,SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,catalogOverride=Some(catalog),credentialDirectory=Some(dir.resolve("credentials")),web=NodeWebSettings(enabled=false),s3=S3Settings(enabled=false),accessLogDirectory=Some(dir.resolve("access")),executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->dir.resolve("history").toString))))
    val server=DacpServer.builder().config(c).build().start();val allocator=new RootAllocator()
    try run(c,server,allocator)finally {server.close();allocator.close();Data.deleteTree(dir)}
  }
  private def client(c:Config,a:RootAllocator,token:()=>Option[String],cert:Boolean=false)=ClientTransport.open(a,c.uri(""),Some(c.ca),if(cert)Some(c.cert)else None,if(cert)Some(c.key)else None,token)
  private def stream(c:FlightClient):FlightStream={val info=c.getInfo(FlightDescriptor.path("datasets","live","/ticks"));c.getStream(info.getEndpoints.get(0).getTicket)}
  test("bearer client needs no certificate; certificate alone and S3 keys cannot authenticate Flight") {fixture{(c,_,a)=>
    val issued=c.credentials.create("alice");val good=client(c,a,()=>Some(issued._2))
    val peer=client(c,a,()=>None,true);val s3=c.credentials.create("s3","s3");val wrong=client(c,a,()=>Some(s3._2))
    try {
      assert(good.listFlights(Criteria.ALL).asScala.size==1)
      assert(intercept[FlightRuntimeException](peer.listFlights(Criteria.ALL).asScala.toVector).status.code==FlightStatusCode.UNAUTHENTICATED)
      assert(intercept[FlightRuntimeException](wrong.listFlights(Criteria.ALL).asScala.toVector).status.code==FlightStatusCode.UNAUTHENTICATED)
      val audit=Files.readString(c.credentials.directory.resolve("authentication-audit.jsonl"))
      assert(audit.contains("UNAUTHENTICATED") && !audit.contains(issued._2) && !audit.contains(s3._2))
    }finally {good.close();peer.close();wrong.close()}
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
