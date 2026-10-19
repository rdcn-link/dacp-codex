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
    Config("localhost",port,"localhost",Map.empty,Seq(s"dacp://localhost:$port/public"),cert,key,cert,executionStore=Some(ExecutionStoreDefinition("file",Map("directory"->(dir.resolve("history")).toString))))
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
    val client=try FlightClient.builder(allocator,server.location).trustedCertificates(caIn).clientCertificate(certIn,keyIn).build()
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
allowed_uri_prefixes: [dacp://localhost:${c.port}/public]
execution_store: {provider: file, settings: {directory: '${dir.resolve("history")}'}}
dataset_catalog: {provider: does-not-exist}
""")
    val builder=DacpServer.builder().configFile(yaml).catalog(catalog(count))
    val server=builder.build()
    try server.start()finally server.close()
    assert(count.get()==1)
    intercept[IllegalArgumentException](builder.build())
  }}
}
