package dacp.repository

import java.nio.file.{Files,Path}
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class PublishingSpec extends AnyFunSuite {
  private val token="publisher-test-token-0123456789abcdef"
  private def fixture(explicitToken:Boolean=true)(run:(Path,RepositoryServer)=>Unit):Unit={
    val root=Files.createTempDirectory("publish-api-")
    val server=new RepositoryServer(root,port=0,publishToken=if(explicitToken)Some(token)else None,publishMaxBytes=1024)
    server.start()
    try run(root,server) finally {server.close();val files=Files.walk(root);try files.iterator.asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally files.close()}
  }
  private def request(s:RepositoryServer,path:String,method:String="GET",body:Array[Byte]=Array.emptyByteArray,auth:Option[String]=Some(token),kind:String="application/json")={
    val b=HttpRequest.newBuilder(URI.create(s"http://localhost:${s.boundPort}$path")).header("Content-Type",kind)
    auth.foreach(t=>b.header("Authorization","Bearer "+t))
    HttpClient.newHttpClient().send(b.method(method,HttpRequest.BodyPublishers.ofByteArray(body)).build(),HttpResponse.BodyHandlers.ofByteArray())
  }
  private def manifest(root:Path):com.fasterxml.jackson.databind.node.ObjectNode={
    val ports=Repository.mapper.readTree("""{"inputs":{"source":{"representations":["bytes"],"formats":[]}},"outputs":{"result":{"representations":["bytes"],"formats":[]}}}""")
    val code=root.resolve("code.jar");Files.writeString(code,"operator")
    val source=root.resolve("sources.zip");Files.writeString(source,"source archive")
    val exporter=new Repository.ImageExporter {
      def inspect(s:String):String="sha256:"+"a"*64
      def save(s:String,p:Path):Unit=Files.writeString(p,"runtime") match {case _=>()}
    }
    Repository.mapper.readTree(Repository.publish(root,"example@1","runtime",ports,exporter,executor="jvm",entrypoint="Example",artifact=Some(code),provenance=Provenance(sourceArchive=Some(source)))).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
  }
  test("authenticated uploads publish immutable operators with code and source; public reads need no token") {fixture(){(root,s)=>
    val prepared=Files.createDirectory(root.resolve("prepared"));val n=manifest(prepared)
    val descriptors=Seq(n.path("runtime").path("archive"),n.path("artifact"))++Provenance.descriptors(n.path("provenance"))
    descriptors.foreach { a=>
      val path="/v1/blobs/sha256/"+a.path("sha256").asText
      val bytes=Files.readAllBytes(Repository.blob(prepared,a.path("sha256").asText))
      assert(request(s,path,"PUT",bytes,kind="application/octet-stream").statusCode()==201)
      assert(request(s,path,"PUT",bytes,kind="application/octet-stream").statusCode()==201)
    }
    val body=Repository.mapper.writeValueAsBytes(n)
    val result=request(s,"/v1/operators","POST",body)
    assert(result.statusCode()==201 && result.headers().firstValue("Location").get()=="/v1/operators/example/1")
    assert(request(s,"/v1/operators","POST",body).statusCode()==409)
    assert(request(s,"/operators/example/1",auth=None).statusCode()==200)
    assert(new String(request(s,"/operators/example/1",auth=None).body(),"UTF-8").contains("SHA-256: "+n.path("artifact").path("sha256").asText))
    assert(request(s,"/v1/operators/example/1",auth=None).body().sameElements(body))
    val files=Files.list(root);try assert(!files.iterator.asScala.exists(_.getFileName.toString.startsWith(".upload-")))finally files.close()
  }}
  test("upload helper streams a local catalog to the authenticated API") {fixture(){(root,s)=>
    val prepared=Files.createDirectory(root.resolve("prepared"));val n=manifest(prepared)
    val status=scala.sys.process.Process(Seq("python3","../scripts/upload-operator.py","example@1","--catalog",prepared.toString,"--url",s"http://localhost:${s.boundPort}"),None,"DACP_REPOSITORY_PUBLISH_TOKEN"->token).!
    assert(status==0)
    assert(request(s,"/v1/operators/example/1",auth=None).statusCode()==200)
  }}
  test("missing and wrong tokens cannot upload; default publication requires the generated token") {
    fixture(){(root,s)=>
      for(auth<-Seq(None,Some("wrong"))) {
        assert(request(s,"/v1/operators","POST","{}".getBytes,auth).statusCode()==401)
        assert(request(s,"/v1/blobs/sha256/"+"a"*64,"PUT","x".getBytes,auth,"application/octet-stream").statusCode()==401)
      }
      assert(Repository.catalog(root).size()==0)
    }
    fixture(false){(root,s)=>
      assert(request(s,"/v1/operators","POST","{}".getBytes,auth=None).statusCode()==401)
      val file=PublishToken.path(root)
      val generated=Files.readString(file).trim
      assert(generated.matches("[a-f0-9]{64}"))
      assert(java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(file))=="rw-------")
      assert(PublishToken.load(root)==generated)
      assert(request(s,"/v1/operators","POST","{}".getBytes,auth=Some(generated)).statusCode()==400)
      assert(request(s,"/.publish-token",auth=None).statusCode()==404)
      assert(!new String(request(s,"/",auth=None).body(),"UTF-8").contains(generated))
      val restarted=new RepositoryServer(root,port=0);restarted.start()
      try {
        assert(Files.readString(file).trim==generated)
        assert(request(restarted,"/v1/operators","POST","{}".getBytes,auth=Some(generated)).statusCode()==400)
      }finally restarted.close()
    }
  }
  test("reject malformed, oversized and incomplete publications without exposing partial operators") {fixture(){(root,s)=>
    val blob="/v1/blobs/sha256/"+"a"*64
    assert(request(s,blob,"PUT","wrong hash".getBytes,kind="application/octet-stream").statusCode()==400)
    assert(request(s,blob,"PUT",new Array[Byte](1025),kind="application/octet-stream").statusCode()==413)
    assert(request(s,blob,"PUT",Array.emptyByteArray,kind="application/octet-stream").statusCode()==400)
    assert(request(s,blob,"PUT","x".getBytes).statusCode()==415)
    assert(request(s,"/v1/operators","POST","{".getBytes).statusCode()==400)
    assert(request(s,"/v1/operators","POST","{} {}".getBytes).statusCode()==400)
    assert(request(s,"/v1/operators","POST",new Array[Byte](65537)).statusCode()==413)
    for(body<-Seq("null", "[]", "{\"name\":\"a\",\"name\":\"b\"}"))
      assert(request(s,"/v1/operators","POST",body.getBytes).statusCode()==400)
    val n=manifest(Files.createDirectory(root.resolve("prepared")))
    assert(request(s,"/v1/operators","POST",Repository.mapper.writeValueAsBytes(n)).statusCode()==400)
    n.put("name","../escape")
    assert(request(s,"/v1/operators","POST",Repository.mapper.writeValueAsBytes(n)).statusCode()==400)
    assert(Repository.catalog(root).size()==0)
    val files=Files.list(root);try assert(!files.iterator.asScala.exists(_.getFileName.toString.startsWith(".upload-")))finally files.close()
  }}
}
