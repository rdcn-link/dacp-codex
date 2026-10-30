package dacp.repository


import com.fasterxml.jackson.databind.JsonNode
import com.sun.net.httpserver.HttpExchange
import java.nio.file.{Files,Path,StandardCopyOption}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

private[repository] case class PublishFailure(status:Int, detail:String) extends RuntimeException(detail)

/** Publisher Tokens accept source materials only. Public visibility is granted solely by review. */
private[repository] final class Publishing(root:Path,admin:Administration,pipeline:SourcePipeline,api:AdminApi,maxBytes:Long) {
  require(maxBytes>0,"publish byte limit must be positive")
  private def copy(e:HttpExchange,file:Path,limit:Long):Unit={
    Option(e.getRequestHeaders.getFirst("Content-Length")).foreach(v=>if(v.toLong>limit)throw PublishFailure(413,"upload exceeds byte limit"))
    val out=Files.newOutputStream(file)
    try {val buffer=new Array[Byte](65536);var total=0L;var n=e.getRequestBody.read(buffer)
      while(n != -1){if(n>limit-total)throw PublishFailure(413,"upload exceeds byte limit");out.write(buffer,0,n);total+=n;n=e.getRequestBody.read(buffer)}
      require(total>0,"empty upload")
    }finally out.close()
  }
  private def publisher(e:HttpExchange):String={
    val headers=Option(e.getRequestHeaders.get("Authorization")).map(_.asScala.toVector).getOrElse(Vector.empty)
    if(headers.size!=1)throw PublishFailure(401,"submitter authentication required")
    admin.authenticate(headers.head)
  }
  private def json(e:HttpExchange,status:Int,n:JsonNode):Unit={val data=Repository.mapper.writeValueAsBytes(n);e.getResponseHeaders.set("Content-Type","application/json");e.getResponseHeaders.set("Cache-Control","no-store");e.sendResponseHeaders(status,data.length);e.getResponseBody.write(data)}
  def handle(e:HttpExchange):Boolean={
    val path=e.getRequestURI.getRawPath;val method=e.getRequestMethod
    val adminBlob=(path.matches("/api/admin/blobs/sha256/[a-f0-9]{64}") && method=="PUT") || (path=="/api/admin/blobs" && method=="POST")
    val blob=path.matches("/v1/blobs/sha256/[a-f0-9]{64}") && method=="PUT"
    val submit=path=="/v1/submissions" && method=="POST"
    val status=(path=="/v1/submissions" || path.matches("/v1/submissions/[a-f0-9-]{36}")) && method=="GET"
    if(!adminBlob && !blob && !submit && !status)return false
    val submitter=if(adminBlob){api.requireSession(e,mutating=true);"administrator"}else publisher(e)
    require(e.getRequestURI.getRawQuery==null,"unexpected query parameters")
    if(status){
      val result=admin.submissions(Some(submitter))
      if(path=="/v1/submissions")json(e,200,result)
      else json(e,200,result.elements.asScala.find(_.path("id").asText==path.split('/').last).getOrElse(throw PublishFailure(404,"submission not found")))
      return true
    }
    val kind=Option(e.getRequestHeaders.getFirst("Content-Type")).getOrElse("").takeWhile(_!=';').trim
    if(kind!=(if(blob || adminBlob)"application/octet-stream"else "application/json"))throw PublishFailure(415,"unsupported upload content type")
    val temporary=Files.createTempFile(root,".upload-",".tmp")
    try {
      copy(e,temporary,if(adminBlob)maxBytes else if(blob)math.min(maxBytes,64L*1024*1024)else 65536)
      if(blob || adminBlob){
        val hash=if(path=="/api/admin/blobs")Repository.checksum(temporary)else path.split('/').last;require(Repository.checksum(temporary)==hash,"uploaded blob checksum mismatch")
        val filename=if(path=="/api/admin/blobs")Option(e.getRequestHeaders.getFirst("X-File-Name")).getOrElse("")else "archive.tar"
        if(path=="/api/admin/blobs")require(Provenance.validFilename(filename),"invalid archive filename")
        val size=Files.size(temporary)
        Repository.synchronized {val target=Repository.blob(root,hash);Files.createDirectories(target.getParent)
          if(Files.exists(target))require(Repository.checksum(target)==hash,"stored blob checksum mismatch")
          else Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE)
        }
        admin.grantBlob(submitter,hash)
        json(e,201,if(path=="/api/admin/blobs")Repository.mapper.createObjectNode().put("sha256",hash).put("size",size).put("filename",filename)else Repository.mapper.createObjectNode().put("status","stored"))
      }else {
        val parser=Repository.mapper.getFactory.createParser(temporary.toFile);parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        val n=try {val node=Repository.mapper.readTree[JsonNode](parser);require(parser.nextToken()==null,"trailing manifest content");node}finally parser.close()
        require(n!=null && n.isObject && n.fieldNames.asScala.toSet.subsetOf(Set("api_version","name","version","description","icon","inputs","parameters","execution","runtime","provenance","build","source_public")),"source submission contains unsupported fields; artifacts and classifications cannot be submitted")
        require(n.path("api_version").isIntegralNumber && n.path("api_version").asInt==2,"unsupported manifest version")
        Repository.parts(n.path("name").asText+"@"+n.path("version").asText)
        require(n.path("description").isTextual && n.path("description").asText.length<=4000,"description required")
        val execution=n.path("execution")
        require(execution.isObject && execution.fieldNames.asScala.toSet==Set("executor","entrypoint","config") && execution.path("entrypoint").isTextual && execution.path("entrypoint").asText.matches("[A-Za-z_][A-Za-z0-9_.]*") && execution.path("config").isObject && execution.path("config").size()==0,"invalid source execution settings")
        require(n.path("runtime").isObject && n.path("runtime").fieldNames.asScala.toSet==Set("image","archive"),"choose a registered runtime")
        Repository.validate(root,n)
        // Recheck before committing, in case the Token was revoked during upload.
        require(publisher(e)==submitter,"submitter changed")
        val record=admin.submit(n,submitter);val id=record.path("id").asText
        pipeline.enqueue(id)
        e.getResponseHeaders.set("Location","/v1/submissions/"+id)
        json(e,202,Repository.mapper.createObjectNode().put("status","pending").put("submission_id",id))
      }
      true
    }finally Files.deleteIfExists(temporary)
  }
}
