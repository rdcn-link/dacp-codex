package dacp

import _root_.xstream.data._

import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.nio.file.{Files,Path}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector,VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class S3ServiceSpec extends AnyFunSuite {
  private def fixture(run:(Config,S3Service,AtomicInteger,()=>Unit)=>Unit):Unit={
    val dir=Files.createTempDirectory("s3-contract-");val allocator=new RootAllocator()
    val closed=new AtomicInteger();var exists=true
    val objects=Seq("/folder/a +&.bin","/folder/b.bin","/z.bin")
    val catalog=new DataCatalog {
      def list():Seq[DatasetInfo]=if(exists)Seq(DatasetInfo("science"))else Seq.empty
      def open(name:String):PublishedDataset={
        if(!exists || name!="science")throw new java.io.FileNotFoundException()
        new PublishedDataset {
          def open(check:()=>Unit):DatasetAccess=new BatchDatasetAccess {
            def list():Seq[Item]=objects.map(describe) :+ describe("/records")
            def list(path:String):Seq[Item]=list().filter(i=>LogicalPath.parent(i.path)==path)
            def describe(path:String):Item={
              if(path=="/records")return Item(path,"custom",Some(new org.apache.arrow.vector.types.pojo.Schema(Seq(Records.field("station","utf8"),Records.field("temp","float64")).asJava)),totalSizeBytes=Some(999),views=Some(Seq("records")))
              if(!objects.contains(path))throw new java.io.FileNotFoundException()
              Item(path,"custom",None,totalSizeBytes=if(path=="/z.bin")None else Some(10L),views=Some(Seq("bytes")))
            }
            def count(path:String):Option[Long]=None
            def read(path:String,view:String,a:org.apache.arrow.memory.BufferAllocator,l: BatchReceiver,ready:()=>Unit):Unit={
              if(path=="/records") {
                assert(view=="records")
                val root=VectorSchemaRoot.create(describe(path).schema.get,a)
                try {Records.fill(root,Seq(Seq("北京",20.5),Seq("A",null)));l.start(root);ready();l.putNext();l.completed()}finally root.close()
                return
              }
              assert(view=="bytes")
              val root=VectorSchemaRoot.create(PluginSchemas.bytes,a)
              try {
                val v=root.getVector(0).asInstanceOf[VarBinaryVector]
                v.allocateNew();l.start(root)
                Seq("0123","4567","89").foreach { chunk=>ready();v.setSafe(0,chunk.getBytes("UTF-8"));root.setRowCount(1);l.putNext() }
                l.completed()
              }finally root.close()
            }
            override def close():Unit={closed.incrementAndGet();()}
          }
        }
      }
    }
    val config=Config("localhost",8815,"localhost",Map.empty,SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,catalogOverride=Some(catalog),s3=S3Settings(host="localhost",port=Some(0)),accessLogDirectory=Some(dir.resolve("access")))
    val service=new S3Service(config,8815,allocator);service.start();SecurityTestSupport.register(service.port,config)
    try run(config,service,closed,()=>exists=false)finally{service.close();config.close();allocator.close();Data.deleteTree(dir)}
  }
  private lazy val client=SecurityTestSupport.client()
  private def request(s:S3Service,path:String,method:String="GET",headers:Map[String,String]=Map.empty)={
    client.send(SecurityTestSupport.signed(s.port,path,method,headers),HttpResponse.BodyHandlers.ofString())
  }
  test("live buckets, escaped keys, delimiter and bound pagination tokens") {fixture{(_,s,_,remove)=>
    assert(request(s,"/").body.contains("<Name>science</Name>"))
    val listing=request(s,"/science?list-type=2&max-keys=1")
    assert(listing.statusCode()==200 && listing.body.contains("<IsTruncated>true</IsTruncated>"))
    assert(listing.body.contains("folder/a +&amp;.bin") && listing.body.contains("folder/"))
    val token="<NextContinuationToken>([^<]+)</NextContinuationToken>".r.findFirstMatchIn(listing.body).get.group(1)
    assert(request(s,"/science?list-type=2&continuation-token="+token).body.contains("folder/b.bin"))
    assert(request(s,"/science?list-type=2&prefix=x&continuation-token="+token).statusCode()==400)
    val grouped=request(s,"/science?list-type=2&delimiter=/").body
    assert(grouped.contains("<CommonPrefixes><Prefix>folder/</Prefix></CommonPrefixes>"))
    assert(request(s,"/science?list-type=2&encoding-type=url").body.contains("folder%2Fa%20%2B%26.bin"))
    assert(request(s,"/science?max-keys=0").body.contains("<IsTruncated>false</IsTruncated>"))
    assert(request(s,"/science?location").body.contains("LocationConstraint"))
    remove();assert(!request(s,"/").body.contains("<Name>science</Name>"))
    assert(request(s,"/science").statusCode()==404)
  }}
  test("streamed object and ranges preserve bytes; HEAD does not stream; leases close on errors") {fixture{(_,s,closed,_)=>
    val url="/science/folder/a%20%2B%26.bin"
    assert(request(s,url).body=="0123456789")
    val head=request(s,url,"HEAD")
    assert(head.statusCode()==200 && head.body.isEmpty && head.headers.firstValue("content-length").get=="10")
    val middle=request(s,url,headers=Map("Range"->"bytes=3-6"))
    assert(middle.statusCode()==206 && middle.body=="3456")
    assert(middle.headers.firstValue("Content-Range").get=="bytes 3-6/10")
    assert(request(s,url,headers=Map("Range"->"bytes=-3")).body=="789")
    assert(request(s,url,headers=Map("Range"->"bytes=8-")).body=="89")
    assert(request(s,url,headers=Map("Range"->"bytes=10-")).statusCode()==416)
    assert(request(s,url,headers=Map("Range"->"bytes=0-1,3-4")).statusCode()==416)
    assert(request(s,"/science/missing").statusCode()==404)
    assert(closed.get()==8)
    assert(request(s,"/science/z.bin").body=="0123456789")
    assert(request(s,"/science/z.bin",headers=Map("Range"->"bytes=0-2")).statusCode()==501)
  }}
  test("public read-only boundary rejects writes, signatures, traversal and unsupported operations") {fixture{(_,s,_,_)=>
    assert(request(s,"/science/z.bin","PUT").statusCode()==405)
    assert(request(s,"/science/z.bin",headers=Map("Authorization"->"AWS4-HMAC-SHA256 fake")).statusCode()==403)
    assert(request(s,"/science/z.bin?X-Amz-Signature=fake").statusCode()==403)
    assert(request(s,"/science/%2e%2e/secret").statusCode()==400)
    assert(request(s,"/science?versions").statusCode()==501)
    assert(request(s,"/science/z.bin?acl").statusCode()==501)
    assert(request(s,"/science?max-keys=-1").statusCode()==400)
  }}
  test("anonymous requests, tampering, expired presigns and revoked keys fail closed") {fixture{(c,s,_,_)=>
    val base=URI.create(s"https://localhost:${s.port}/science/z.bin")
    assert(client.send(HttpRequest.newBuilder(base).build(),HttpResponse.BodyHandlers.ofString()).statusCode()==403)
    val (id,secret)=SecurityTestSupport.credentials.get(s.port)
    val url=dacp.union.SigV4.presignGet(base,id,secret,60)
    def get(u:URI)=client.send(HttpRequest.newBuilder(u).build(),HttpResponse.BodyHandlers.ofString())
    assert(get(url).body=="0123456789")
    assert(get(URI.create(url.toString.replace("/z.bin","/missing"))).statusCode()==403)
    val expired=dacp.union.SigV4.presignGet(base,id,secret,1,now=java.time.Instant.now.minusSeconds(5))
    assert(get(expired).statusCode()==403)
    c.credentials.revoke(id)
    assert(get(url).statusCode()==403)
    assert(request(s,"/science/z.bin",headers=Map("Range"->"bytes=0-2")).statusCode()==403)
  }}
  test("settings validate types and port bounds") {
    assert(S3Settings().enabled && S3Settings().bindPort(8815)==10815)
    def read(json:String)=S3Settings.read(Some(Data.mapper.readValue(json,classOf[java.util.LinkedHashMap[String,Object]])))
    assert(!read("""{"enabled":false}""").enabled)
    intercept[IllegalArgumentException](read("""{"enabled":"false"}"""))
    intercept[IllegalArgumentException](read("""{"port":4294967296}"""))
    intercept[IllegalArgumentException](read("""{"credentials":"x"}"""))
  }
  test("built-in mixed provider exposes original file bytes through the same transport") {
    val dir=Files.createTempDirectory("s3-plugin-");val allocator=new RootAllocator()
    val path=Files.writeString(dir.resolve("sample.csv"),"station,temp\nA,20\n")
    val empty=Files.createFile(dir.resolve("empty.bin"))
    val c=Config("localhost",8815,"localhost",Map("climate"->DatasetDefinition("mixed",items=Map("/sample.csv"->Map("file"->path.toString),"/empty.bin"->Map("file"->empty.toString)))),SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,s3=S3Settings(host="localhost",port=Some(0)),accessLogDirectory=Some(dir.resolve("access")))
    val s=new S3Service(c,8815,allocator);s.start();SecurityTestSupport.register(s.port,c)
    try {assert(request(s,"/climate?list-type=2").body.contains("sample.csv"));assert(request(s,"/climate/sample.csv").body==Files.readString(path));val zero=request(s,"/climate/empty.bin");assert(zero.statusCode()==200 && zero.body.isEmpty && zero.headers.firstValue("Content-Length").get=="0")}
    finally {s.close();c.close();allocator.close();Data.deleteTree(dir)}
  }
  test("boto3 signed path-style client interoperates with the S3 transport") {
    val python=sys.env.getOrElse("DACP_S3_TEST_PYTHON",cancel("set DACP_S3_TEST_PYTHON to a Python with boto3 installed"))
    fixture {(c,s,_,_)=>
      import scala.sys.process._
      val (id,secret)=SecurityTestSupport.credentials.get(s.port)
      val result=Process(Seq(python,"../scripts/test-s3-service.py",s"https://localhost:${s.port}","science","folder/a +&.bin"),None,"AWS_ACCESS_KEY_ID"->id,"AWS_SECRET_ACCESS_KEY"->secret,"AWS_CA_BUNDLE"->c.ca.toString).!
      assert(result==0)
    }
  }

  test("closing the endpoint cancels an active download and releases its dataset lease") {
    val dir=Files.createTempDirectory("s3-cancel-");val allocator=new RootAllocator()
    val released=new java.util.concurrent.CountDownLatch(1)
    val cat=new DataCatalog {
      def list():Seq[DatasetInfo]=Seq(DatasetInfo("large"))
      def open(name:String):PublishedDataset=new PublishedDataset {
        def open(check:()=>Unit):DatasetAccess=new BatchDatasetAccess {
          def list():Seq[Item]=Seq(describe("/stream"))
          def list(path:String):Seq[Item]=list()
          def describe(path:String):Item=Item(path,"custom",None,views=Some(Seq("bytes")))
          def count(path:String):Option[Long]=None
          def read(path:String,view:String,a:org.apache.arrow.memory.BufferAllocator,l: BatchReceiver,ready:()=>Unit):Unit={
            val root=VectorSchemaRoot.create(PluginSchemas.bytes,a)
            try {
              val v=root.getVector(0).asInstanceOf[VarBinaryVector];v.allocateNew();v.setSafe(0,new Array[Byte](65536));root.setRowCount(1)
              l.start(root)
              while(true){ready();l.putNext()}
            }finally root.close()
          }
          override def close():Unit=released.countDown()
        }
      }
    }
    val c=Config("localhost",8815,"localhost",Map.empty,SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,catalogOverride=Some(cat),s3=S3Settings(host="localhost",port=Some(0)),accessLogDirectory=Some(dir.resolve("access")))
    val s=new S3Service(c,8815,allocator);s.start();SecurityTestSupport.register(s.port,c)
    try {
      val response=client.send(SecurityTestSupport.signed(s.port,"/large/stream"),HttpResponse.BodyHandlers.ofInputStream())
      try {assert(response.body.read()>=0);s.close();assert(released.await(5,java.util.concurrent.TimeUnit.SECONDS))}finally response.body.close()
    }finally {s.close();c.close();allocator.close();Data.deleteTree(dir)}
  }

  test("records-only plugin Items expose their shared JSONL bytes view through S3") {fixture{(_,s,_,_)=>
    val response=request(s,"/science/records")
    assert(response.statusCode()==200 && response.body=="{\"station\":\"北京\",\"temp\":20.5}\n{\"station\":\"A\",\"temp\":null}\n")
    assert(request(s,"/science?list-type=2").body.contains("<Key>records</Key>"))
    val head=request(s,"/science/records","HEAD")
    assert(head.statusCode()==200 && !head.headers.firstValue("Content-Length").isPresent)
    assert(request(s,"/science/records",headers=Map("Range"->"bytes=0-3")).statusCode()==501)
  }}

}
