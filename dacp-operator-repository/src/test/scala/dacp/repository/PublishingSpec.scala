package dacp.repository



import java.nio.file.{Files,Path}
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.util.zip.{ZipEntry,ZipOutputStream}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class PublishingSpec extends AnyFunSuite {
  private val password="test-administrator-password"
  private final class Client(val server:RepositoryServer){
    val http=HttpClient.newHttpClient();var cookie="";var csrf=""
    def request(path:String,method:String="GET",data:Array[Byte]=Array.emptyByteArray,token:Option[String]=None,kind:String="application/json",admin:Boolean=false,withCsrf:Boolean=true)={
      val b=HttpRequest.newBuilder(URI.create(s"http://localhost:${server.boundPort}$path")).header("Content-Type",kind)
      token.foreach(t=>b.header("Authorization","Bearer "+t))
      if(admin){b.header("Cookie",cookie);if(withCsrf)b.header("X-CSRF-Token",csrf)}
      http.send(b.method(method,HttpRequest.BodyPublishers.ofByteArray(data)).build(),HttpResponse.BodyHandlers.ofByteArray())
    }
    def json(path:String,method:String="GET",n:JsonNode=Repository.mapper.createObjectNode(),admin:Boolean=true)=request(path,method,if(method=="GET")Array.emptyByteArray else Repository.mapper.writeValueAsBytes(n),admin=admin)
    def login():Unit={val r=json("/api/admin/login","POST",Repository.mapper.createObjectNode().put("password",password),false);assert(r.statusCode()==200);cookie=r.headers().firstValue("Set-Cookie").get().takeWhile(_!=';');csrf=Repository.mapper.readTree(r.body()).path("csrf").asText}
    def issue(name:String):(String,String)={val r=json("/api/admin/tokens","POST",Repository.mapper.createObjectNode().put("submitter",name));assert(r.statusCode()==201);val n=Repository.mapper.readTree(r.body());(n.path("token").asText,n.path("id").asText)}
  }
  private def fixture(run:(Path,Client)=>Unit):Unit={
    val root=Files.createTempDirectory("source-submission-")
    val engine=new SourceBuildEngine {def build(r:Path,m:JsonNode,rt:JsonNode):JsonNode={val code=Files.createTempFile(r,"test-build-",".zip");try {Files.writeString(code,"built:"+m.path("name").asText);Repository.store(r,code,"operator.zip")}finally Files.delete(code)}}
    val server=new RepositoryServer(root,port=0,adminPassword=Some(password),publishMaxBytes=1024*1024,buildEngine=engine);server.start()
    try run(root,new Client(server))finally {server.close();val files=Files.walk(root);try files.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally files.close()}
  }
  private def archive(root:Path,code:String):Path={val source=root.resolve("sources.zip");val out=new ZipOutputStream(Files.newOutputStream(source));try {out.putNextEntry(new ZipEntry("src/example.py"));out.write(code.getBytes("UTF-8"));out.closeEntry()}finally out.close();source}
  private def setup(root:Path,c:Client,publicSource:Boolean=false,code:String="def process():\n return 1\n"):(ObjectNode,String,String)={
    c.login();val (token,id)=c.issue("alice")
    val runtimeFile=root.resolve("runtime.tar");Files.writeString(runtimeFile,"test runtime")
    val runtime=Repository.store(root,runtimeFile,"runtime.tar")
    val rt=Repository.mapper.createObjectNode().put("name","python").put("version","1").put("description","Python test runtime").put("image","sha256:"+"a"*64)
    rt.set[JsonNode]("archive",runtime);rt.putArray("executors").add("python");rt.putObject("dependencies").put("python","3.12")
    val build=rt.putObject("build").put("image","sha256:"+"a"*64);build.set[JsonNode]("archive",runtime);build.putArray("languages").add("python")
    assert(c.json("/api/admin/runtimes","POST",rt).statusCode()==201)
    val source=archive(root,code);val hash=Repository.checksum(source)
    assert(c.request("/v1/blobs/sha256/"+hash,"PUT",Files.readAllBytes(source),Some(token),"application/octet-stream").statusCode()==201)
    val n=Repository.mapper.readTree("""{"api_version":2,"name":"example","version":"1","description":"Example source operator","icon":{"path":"M3 5h18v14H3z","color":"#14866b"},"inputs":{},"parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false},"execution":{"executor":"python","entrypoint":"example","config":{}},"build":{"language":"python"}}""").asInstanceOf[ObjectNode]
    n.set[JsonNode]("runtime",Repository.mapper.createObjectNode().put("image","sha256:"+"a"*64).set[JsonNode]("archive",runtime))
    n.put("source_public",publicSource)
    n.putObject("provenance").put("license","MIT").set[JsonNode]("source_archive",Repository.mapper.createObjectNode().put("sha256",hash).put("size",Files.size(source)).put("filename","sources.zip"))
    (n,token,id)
  }
  private def submit(c:Client,n:JsonNode,token:String):String={val r=c.request("/v1/submissions","POST",Repository.mapper.writeValueAsBytes(n),Some(token));assert(r.statusCode()==202,new String(r.body()));Repository.mapper.readTree(r.body()).path("submission_id").asText}
  private def waitBuild(c:Client,id:String):JsonNode={
    val end=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
    var n=Repository.mapper.readTree(c.json("/api/admin/submissions/"+id).body())
    while(Set("queued","running")(n.path("build_result").path("status").asText) && System.nanoTime()<end){Thread.sleep(20);n=Repository.mapper.readTree(c.json("/api/admin/submissions/"+id).body())}
    assert(n.path("build_result").path("status").asText=="succeeded",n.toPrettyString);n
  }
  private def approve(c:Client,id:String,score:Option[Int]=None)={val n=Repository.mapper.createObjectNode().put("decision","approved").put("note",if(score.isDefined)"Administrator reviewed the findings"else "");n.putArray("categories").add("Climate").add("Conversion");score.foreach(n.put("trust_score",_));c.json("/api/admin/submissions/"+id+"/review","POST",n)}
  test("source-only submissions are built and remain private until review; final trust and multiple categories are public") {fixture{(root,c)=>
    val (n,token,_)=setup(root,c);val id=submit(c,n,token);val s=waitBuild(c,id)
    assert(s.path("analysis").path("score").asInt==5)
    val sourceHash=n.path("provenance").path("source_archive").path("sha256").asText
    val codeHash=s.path("manifest").path("artifact").path("sha256").asText
    for(path<-Seq("/v1/operators/example/1","/operators/example/1","/v1/blobs/sha256/"+sourceHash,"/v1/blobs/sha256/"+sourceHash+"/sources.zip","/v1/blobs/sha256/"+codeHash))assert(c.request(path).statusCode()==404)
    assert(Repository.mapper.readTree(c.request("/v1/operators").body()).size()==0)
    assert(c.request("/api/admin/submissions/"+id).statusCode()==401)
    assert(approve(c,id,Some(4)).statusCode()==200)
    val published=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(published.path("categories").size()==2 && !published.has("category"))
    assert(published.path("trust").path("score").asInt==4 && !published.path("provenance").has("source_archive"))
    assert(c.request("/v1/blobs/sha256/"+sourceHash).statusCode()==404)
    assert(c.request("/v1/blobs/sha256/"+codeHash+"/operator.zip").statusCode()==200)
    assert(new String(c.request("/operators/example/1").body()).contains("4/5"))
    assert(c.request("/api/admin/submissions/"+id+"/files/"+sourceHash+"/sources.zip",admin=true).statusCode()==200)
    assert(approve(c,id).statusCode()==409)
  }}
  test("administrators can reclassify a published operator without changing its approved code or trust") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c);val id=submit(c,n,token);waitBuild(c,id)
    val endpoint="/api/admin/submissions/"+id+"/categories"
    val categories=Repository.mapper.createObjectNode();categories.putArray("categories").add("Data processing").add("Merging")
    assert(c.json(endpoint,"POST",categories,false).statusCode()==401)
    assert(c.request(endpoint,"POST",categories.toString.getBytes,admin=true,withCsrf=false).statusCode()==403)
    assert(c.json(endpoint,"POST",categories).statusCode()==409)
    assert(approve(c,id,Some(5)).statusCode()==200)
    val before=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(c.json(endpoint,"POST",categories).statusCode()==200)
    val after=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(after.path("categories")==categories.path("categories"))
    assert(after.path("artifact")==before.path("artifact") && after.path("trust")==before.path("trust"))
    assert(c.json(endpoint,"POST",Repository.mapper.createObjectNode().putArray("categories")).statusCode()==400)
  }}
  test("administrator icon changes validate SVG metadata and preserve approved operator contents") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c);val id=submit(c,n,token);waitBuild(c,id)
    val endpoint="/api/admin/submissions/"+id+"/icon"
    val update=Repository.mapper.createObjectNode();update.putObject("icon").put("path","M3 4h8v16H3zM14 6h7v14h-7z").put("color","#2378cb")
    assert(c.json(endpoint,"POST",update,false).statusCode()==401)
    assert(c.request(endpoint,"POST",update.toString.getBytes,admin=true,withCsrf=false).statusCode()==403)
    assert(c.json(endpoint,"POST",update).statusCode()==409)
    assert(approve(c,id,Some(5)).statusCode()==200)
    val before=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(c.json(endpoint,"POST",update).statusCode()==200)
    val after=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(after.path("icon")==update.path("icon"))
    for(field<-Seq("artifact","runtime","trust","categories"))assert(after.path(field)==before.path(field))
    update.withObject("/icon").put("path","<script>alert(1)</script>")
    assert(c.json(endpoint,"POST",update).statusCode()==400)
    assert(Repository.mapper.readTree(c.request("/v1/operators/example/1").body()).path("icon")==after.path("icon"))
  }}
  test("public source is exposed only after approval; scan default remains separate from administrator trust") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c,true,"import requests\nrequests.post('https://example.org',data='data')\n")
    val id=submit(c,n,token);val s=waitBuild(c,id);assert(s.path("analysis").path("score").asInt<3)
    val review=Repository.mapper.createObjectNode().put("decision","approved").put("note","Reviewed network destination and behavior");review.putArray("categories").add("Network")
    assert(c.json("/api/admin/submissions/"+id+"/review","POST",review).statusCode()==200)
    val p=Repository.mapper.readTree(c.request("/v1/operators/example/1").body())
    assert(p.path("trust").path("score")==s.path("analysis").path("score"))
    assert(c.request("/v1/blobs/sha256/"+p.path("provenance").path("source_archive").path("sha256").asText).statusCode()==200)
  }}
  test("administrator authentication, CSRF, per-submitter ownership and revocation are enforced") {fixture{(root,c)=>
    assert(c.json("/api/admin/tokens","POST",Repository.mapper.createObjectNode().put("submitter","anonymous"),false).statusCode()==401)
    val(n,token,tokenId)=setup(root,c)
    assert(c.request("/api/admin/tokens","POST","{}".getBytes,admin=true,withCsrf=false).statusCode()==403)
    assert(c.request("/v1/submissions","POST",n.toString.getBytes).statusCode()==401)
    val(other,_)=c.issue("bob")
    assert(c.request("/v1/submissions","POST",n.toString.getBytes,Some(other)).statusCode()==400)
    val id=submit(c,n,token);waitBuild(c,id)
    assert(Repository.mapper.readTree(c.request("/v1/submissions",token=Some(other)).body()).size()==0)
    assert(c.request("/v1/submissions/"+id,token=Some(other)).statusCode()==404)
    assert(c.json("/api/admin/tokens/"+tokenId,"DELETE").statusCode()==200)
    assert(c.request("/v1/submissions",token=Some(token)).statusCode()==401)
    assert(!Files.readString(root.resolve(".admin/state.json")).contains(token))
    assert(!new String(c.request("/").body()).contains(password))
    assert(c.request("/.admin/state.json").statusCode()==404)
  }}
  test("administrator archive uploads stream with authentication, CSRF and computed descriptors") {fixture{(root,c)=>
    val content="runtime archive payload".getBytes("UTF-8")
    def upload(auth:Boolean,csrf:Boolean)= {
      val b=HttpRequest.newBuilder(URI.create(s"http://localhost:${c.server.boundPort}/api/admin/blobs"))
        .header("Content-Type","application/octet-stream").header("X-File-Name","runtime.tar")
      if(auth)b.header("Cookie",c.cookie)
      if(csrf)b.header("X-CSRF-Token",c.csrf)
      c.http.send(b.POST(HttpRequest.BodyPublishers.ofInputStream(()=>new java.io.ByteArrayInputStream(content))).build(),HttpResponse.BodyHandlers.ofByteArray())
    }
    assert(upload(false,false).statusCode()==401)
    c.login();assert(upload(true,false).statusCode()==403)
    val response=upload(true,true);assert(response.statusCode()==201)
    val descriptor=Repository.mapper.readTree(response.body())
    assert(descriptor.path("filename").asText=="runtime.tar" && descriptor.path("size").asLong==content.length)
    val hash=descriptor.path("sha256").asText
    assert(Files.readAllBytes(Repository.blob(root,hash)).sameElements(content))
    assert(c.request("/v1/blobs/sha256/"+hash).statusCode()==404)
  }}
  test("binary artifacts, developer classifications, unsupported runtime and invalid review scores are rejected") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c)
    for(field<-Seq("artifact","category","categories")){val bad=n.deepCopy[ObjectNode]();bad.put(field,"forbidden");assert(c.request("/v1/submissions","POST",bad.toString.getBytes,Some(token)).statusCode()==400)}
    val missing=n.deepCopy[ObjectNode]();missing.path("provenance").asInstanceOf[ObjectNode].remove("source_archive");assert(c.request("/v1/submissions","POST",missing.toString.getBytes,Some(token)).statusCode()==400)
    assert(c.request("/v1/operators","POST",n.toString.getBytes,Some(token)).statusCode()==405)
    val id=submit(c,n,token);waitBuild(c,id)
    assert(approve(c,id,Some(6)).statusCode()==400)
    assert(approve(c,id,Some(-1)).statusCode()==400)
    assert(approve(c,id).statusCode()==200)
    assert(c.request("/v1/submissions","POST",n.toString.getBytes,Some(token)).statusCode()==409)
  }}
  test("source submission CLI resolves the public runtime and sends no binary artifact") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c)
    val definition=n.deepCopy[ObjectNode]();definition.remove(java.util.Arrays.asList("api_version","execution","build"))
    definition.put("language","python").put("entrypoint","example").put("runtime","python@1")
    definition.path("provenance").asInstanceOf[ObjectNode].remove("source_archive")
    val yaml=root.resolve("submission.yaml");Files.writeString(yaml,definition.toString)
    val tokenFile=root.resolve("submitter.token");Files.writeString(tokenFile,token)
    val status=scala.sys.process.Process(Seq("python3","../scripts/submit-operator.py",yaml.toString,"--source",root.resolve("sources.zip").toString,"--url",s"http://localhost:${c.server.boundPort}","--token-file",tokenFile.toString)).!
    assert(status==0)
    val submissions=Repository.mapper.readTree(c.request("/v1/submissions",token=Some(token)).body())
    assert(submissions.size()==1 && submissions.get(0).path("manifest").path("build").path("language").asText=="python")
  }}
  test("rejected source never becomes public; runtime catalog is public and local package files do not bypass approval") {fixture{(root,c)=>
    val(n,token,_)=setup(root,c)
    assert(c.request("/runtimes").statusCode()==200)
    assert(Repository.mapper.readTree(c.request("/v1/runtimes").body()).size()==1)
    val id=submit(c,n,token);waitBuild(c,id)
    assert(c.json("/api/admin/submissions/"+id+"/review","POST",Repository.mapper.createObjectNode().put("decision","rejected").put("note","Not accepted")).statusCode()==200)
    assert(c.request("/v1/operators/example/1").statusCode()==404)
    val path=root.resolve("injected/1/manifest.json");Files.createDirectories(path.getParent);Files.writeString(path,n.toString)
    assert(c.request("/v1/operators/injected/1").statusCode()==404)
    assert(Repository.mapper.readTree(c.request("/v1/operators").body()).size()==0)
  }}
}
