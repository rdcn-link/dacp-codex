package dacp.repository

import com.fasterxml.jackson.databind.JsonNode
import com.sun.net.httpserver.HttpExchange
import java.nio.file.{Files,Path,StandardCopyOption,StandardOpenOption}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import scala.jdk.CollectionConverters._

private[repository] case class PublishFailure(status:Int, detail:String) extends RuntimeException(detail)

/** Authenticated uploads never execute code, contact Docker, or fetch publisher-supplied URLs. */
private[repository] final class Publishing(root:Path, token:String, maxBytes:Long) {
  require(token.length>=32 && token.length<=4096 && token.forall(c=>c>=33 && c<=126), "publish token must contain 32–4096 visible ASCII characters")
  require(maxBytes>0,"publish byte limit must be positive")
  private val expected=("Bearer "+token).getBytes(UTF_8)
  private def locked[A](f: => A):A=Repository.synchronized {
    val channel=FileChannel.open(root.resolve(".publish.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)
    val lock=channel.lock()
    try f finally {lock.release();channel.close()}
  }
  private def copy(e:HttpExchange,file:Path,limit:Long):Unit={
    val length=Option(e.getRequestHeaders.getFirst("Content-Length"))
    length.foreach(v=>if(v.toLong>limit)throw PublishFailure(413,"upload exceeds byte limit"))
    val out=Files.newOutputStream(file)
    try {
      val buffer=new Array[Byte](65536);var total=0L;var n=e.getRequestBody.read(buffer)
      while(n != -1){if(n>limit-total)throw PublishFailure(413,"upload exceeds byte limit");out.write(buffer,0,n);total+=n;n=e.getRequestBody.read(buffer)}
      require(total>0,"empty upload")
    }finally out.close()
  }
  private def descriptor(a:JsonNode):Unit={
    require(a.isObject && a.fieldNames.asScala.toSet==Set("sha256","size","filename"),"invalid artifact descriptor")
    require(a.path("sha256").isTextual && a.path("sha256").asText.matches("[a-f0-9]{64}"),"invalid artifact digest")
    require(a.path("size").isIntegralNumber && a.path("size").canConvertToLong && a.path("size").asLong>0,"invalid artifact size")
    require(a.path("filename").isTextual && Provenance.validFilename(a.path("filename").asText),"invalid artifact filename")
    if(!Files.isRegularFile(Repository.blob(root,a.path("sha256").asText)))throw PublishFailure(400,"upload all referenced blobs first")
  }
  private def publish(n:JsonNode):Unit=locked {
    require(n!=null && n.isObject && n.fieldNames.asScala.toSet.subsetOf(Set("api_version","name","version","description","inputs","outputs","execution","runtime","artifact","provenance")),"invalid manifest fields")
    require(n.path("api_version").isIntegralNumber && n.path("api_version").canConvertToInt && n.path("api_version").asInt==2,"unsupported manifest version")
    require(n.path("name").isTextual && n.path("version").isTextual,"operator name and version required")
    val (name,version)=Repository.parts(n.path("name").asText+"@"+n.path("version").asText)
    require(!n.has("description") || n.path("description").isTextual,"invalid description")
    val execution=n.path("execution")
    require(execution.isObject && execution.fieldNames.asScala.toSet==Set("executor","entrypoint","config"),"invalid execution fields")
    require(execution.path("executor").isTextual && execution.path("executor").asText.matches("[a-z][a-z0-9_-]*"),"invalid executor ID")
    require(execution.path("entrypoint").isTextual && execution.path("entrypoint").asText.length<=1024 && !execution.path("entrypoint").asText.contains("\u0000") && execution.path("config").isObject,"invalid execution settings")
    val runtime=n.path("runtime")
    require(runtime.isObject && runtime.fieldNames.asScala.toSet==Set("image","archive"),"invalid runtime fields")
    (Seq(runtime.path("archive"))++Option(n.get("artifact"))++Option(n.get("provenance")).toSeq.flatMap(Provenance.descriptors)).foreach(descriptor)
    Repository.validate(root,n)
    val parent=root.resolve(name);Files.createDirectories(parent)
    val target=parent.resolve(version)
    if(Files.exists(target))throw PublishFailure(409,"operator version already published")
    val stage=Files.createTempDirectory(parent,".publish-")
    try {Repository.mapper.writeValue(stage.resolve("manifest.json").toFile,n);Files.move(stage,target,StandardCopyOption.ATOMIC_MOVE)}
    finally {Files.deleteIfExists(stage.resolve("manifest.json"));Files.deleteIfExists(stage)}
  }
  def handle(e:HttpExchange):Boolean={
    val path=e.getRequestURI.getRawPath
    val blob=path.matches("/v1/blobs/sha256/[a-f0-9]{64}") && e.getRequestMethod=="PUT"
    val manifest=path=="/v1/operators" && e.getRequestMethod=="POST"
    if(!blob && !manifest)return false
    val auth=Option(e.getRequestHeaders.get("Authorization")).map(_.asScala.toSeq).getOrElse(Seq.empty)
    if(auth.size!=1 || !MessageDigest.isEqual(expected,auth.head.getBytes(UTF_8)))throw PublishFailure(401,"publisher authentication required")
    require(e.getRequestURI.getRawQuery==null,"unexpected query parameters")
    val contentType=Option(e.getRequestHeaders.getFirst("Content-Type")).getOrElse("").takeWhile(_!=';').trim
    if(contentType!=(if(blob)"application/octet-stream"else "application/json"))throw PublishFailure(415,"unsupported upload content type")
    val temporary=Files.createTempFile(root,".upload-",".tmp")
    try {
      copy(e,temporary,if(blob)maxBytes else 65536)
      if(blob) {
        val hash=path.split('/').last
        require(Repository.checksum(temporary)==hash,"uploaded blob checksum mismatch")
        locked {
          val target=Repository.blob(root,hash);Files.createDirectories(target.getParent)
          if(Files.exists(target))require(Repository.checksum(target)==hash,"stored blob checksum mismatch")
          else Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE)
        }
      }else {
        val parser=Repository.mapper.getFactory.createParser(temporary.toFile)
        parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        try {val n=Repository.mapper.readTree[JsonNode](parser);require(parser.nextToken()==null,"trailing manifest content");publish(n)}finally parser.close()
      }
      e.getResponseHeaders.set("Content-Type","application/json")
      if(manifest){val n=Repository.mapper.readTree(temporary.toFile);e.getResponseHeaders.set("Location",s"/v1/operators/${n.path("name").asText}/${n.path("version").asText}")}
      val result="{\"status\":\"published\"}\n".getBytes(UTF_8)
      e.sendResponseHeaders(201,result.length);e.getResponseBody.write(result)
      true
    }finally Files.deleteIfExists(temporary)
  }
}
