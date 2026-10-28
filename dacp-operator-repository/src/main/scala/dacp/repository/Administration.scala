package dacp.repository

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode,ObjectNode}
import java.nio.file.{Files,Path,StandardCopyOption,StandardOpenOption}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.security.{MessageDigest,SecureRandom}
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import scala.jdk.CollectionConverters._

/** Durable review decisions and hashed credentials. Public reads derive only from approved records. */
private[repository] final class Administration(root:Path, initialPassword:Option[String]) extends AutoCloseable {
  private val random=new SecureRandom()
  private val directory=root.resolve(".admin")
  Files.createDirectories(directory)
  Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"))
  private val channel=FileChannel.open(directory.resolve("store.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)
  private val lock=try Option(channel.tryLock()).getOrElse(throw new IllegalStateException("repository already open")) catch {case e:Throwable=>channel.close();throw e}
  private val file=directory.resolve("state.json")
  private def secret():String={val bytes=new Array[Byte](32);random.nextBytes(bytes);bytes.map(b=>f"${b & 255}%02x").mkString}
  private def hash(s:String):String=MessageDigest.getInstance("SHA-256").digest(s.getBytes(UTF_8)).map(b=>f"${b & 255}%02x").mkString
  private def passwordHash(password:String,salt:String):String={
    val spec=new PBEKeySpec(password.toCharArray,salt.getBytes(UTF_8),210000,256)
    try SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded.map(b=>f"${b & 255}%02x").mkString finally spec.clearPassword()
  }
  private def save(n:ObjectNode):Unit={
    val tmp=Files.createTempFile(directory,".state-",".json",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    try {Repository.mapper.writeValue(tmp.toFile,n);val out=FileChannel.open(tmp,StandardOpenOption.WRITE);try out.force(true)finally out.close();Files.move(tmp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)}finally Files.deleteIfExists(tmp)
  }
  private var state:ObjectNode=try {
    if(Files.exists(file))Repository.mapper.readTree(file.toFile).asInstanceOf[ObjectNode]
    else {
      val password=initialPassword.getOrElse(secret())
      require(password.length>=12 && password.length<=1024,"administrator password must be 12–1024 characters")
      if(initialPassword.isEmpty){val target=directory.resolve("bootstrap-password");Files.createFile(target,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.writeString(target,password+"\n",UTF_8)}
      val n=Repository.mapper.createObjectNode();val salt=secret()
      n.putObject("password").put("salt",salt).put("hash",passwordHash(password,salt))
      Seq("tokens","categories","runtimes","submissions","blob_grants").foreach(n.putArray)
      save(n);n
    }
  }catch{case e:Throwable=>lock.release();channel.close();throw e}
  def bootstrapPasswordPath:Path=directory.resolve("bootstrap-password")
  private def change[A](f:ObjectNode=>A):A=synchronized {val next=state.deepCopy[ObjectNode]();val result=f(next);save(next);state=next;result}
  private def array(n:JsonNode,key:String)=n.path(key).asInstanceOf[ArrayNode]
  private def all(n:JsonNode,key:String)=n.path(key).elements.asScala.toVector
  private def equal(a:String,b:String)=MessageDigest.isEqual(a.getBytes(UTF_8),b.getBytes(UTF_8))
  def login(password:String):Boolean=synchronized {password.length<=1024 && equal(passwordHash(password,state.path("password").path("salt").asText),state.path("password").path("hash").asText)}
  def issue(submitter:String):ObjectNode=change{n=>
    require(submitter.nonEmpty && submitter==submitter.trim && submitter.length<=100 && !submitter.exists(Character.isISOControl),"invalid submitter name")
    val token=secret();val entry=Repository.mapper.createObjectNode().put("id",java.util.UUID.randomUUID().toString).put("submitter",submitter).put("hash",hash(token)).put("created_at_ms",System.currentTimeMillis()).put("revoked",false)
    array(n,"tokens").add(entry)
    val result=entry.deepCopy[ObjectNode]();result.remove("hash");result.put("token",token)
  }
  def tokens:ArrayNode=synchronized {val result=Repository.mapper.createArrayNode();all(state,"tokens").foreach{t=>val copy=t.deepCopy[ObjectNode]();copy.remove("hash");result.add(copy)};result}
  def revoke(id:String):Unit=change {n=>all(n,"tokens").find(_.path("id").asText==id).getOrElse(throw PublishFailure(404,"token not found")).asInstanceOf[ObjectNode].put("revoked",true);()}
  def authenticate(header:String):String=synchronized {
    if(!header.startsWith("Bearer ") || header.length>4096)throw PublishFailure(401,"submitter authentication required")
    val digest=hash(header.substring(7))
    all(state,"tokens").find(t => !t.path("revoked").asBoolean && equal(t.path("hash").asText,digest)).map(_.path("submitter").asText).getOrElse(throw PublishFailure(401,"submitter authentication required"))
  }
  private val recipes=java.util.ServiceLoader.load(classOf[dacp.operator.SourceBuildRecipe]).iterator.asScala.toVector
  def buildLanguages:ArrayNode={val result=Repository.mapper.createArrayNode();recipes.foreach(r=>result.addObject().put("language",r.language).put("executor",r.executor).put("artifact_filename",r.artifactFilename));result}
  def runtimes:ArrayNode=synchronized {state.path("runtimes").deepCopy[ArrayNode]()}
  def categories:ArrayNode=synchronized {state.path("categories").deepCopy[ArrayNode]()}
  def addRuntime(runtime:JsonNode):Unit=change{n=>
    require(runtime.isObject && runtime.fieldNames.asScala.toSet==Set("name","version","description","image","archive","executors","dependencies","build"),"runtime requires name, version, description, image, archive, executors, dependencies and build")
    Repository.parts(runtime.path("name").asText+"@"+runtime.path("version").asText)
    require(runtime.path("description").isTextual && runtime.path("description").asText.length<=2000 && runtime.path("dependencies").isObject,"invalid runtime description/dependencies")
    require(runtime.path("executors").isArray && runtime.path("executors").size()>0 && runtime.path("executors").elements.asScala.forall(v=>v.isTextual && v.asText.matches("[a-z][a-z0-9_-]*")),"invalid runtime executors")
    require(runtime.path("image").asText.matches("sha256:[a-f0-9]{64}"),"runtime image must be pinned")
    verifyDescriptor(runtime.path("archive"))
    val build=runtime.path("build")
    require(build.isObject && build.fieldNames.asScala.toSet==Set("image","archive","languages") && build.path("image").asText.matches("sha256:[a-f0-9]{64}"),"invalid build environment")
    require(build.path("languages").isArray && build.path("languages").size()>0 && build.path("languages").elements.asScala.forall(_.isTextual),"build.languages must be a nonempty language list")
    require(build.path("languages").elements.asScala.forall(v=>recipes.exists(r=>r.language==v.asText && runtime.path("executors").elements.asScala.exists(_.asText==r.executor))),"build language plugin or compatible executor is unavailable")
    verifyDescriptor(build.path("archive"))
    if(all(n,"runtimes").exists(r=>r.path("name")==runtime.path("name") && r.path("version")==runtime.path("version")))throw PublishFailure(409,"runtime version already registered")
    array(n,"runtimes").add(runtime.deepCopy[JsonNode]());()
  }
  def verifyDescriptor(a:JsonNode):Unit={
    require(a.isObject && a.fieldNames.asScala.toSet==Set("sha256","size","filename") && a.path("sha256").isTextual && a.path("sha256").asText.matches("[a-f0-9]{64}") && a.path("filename").isTextual && Provenance.validFilename(a.path("filename").asText) && a.path("size").isIntegralNumber && a.path("size").canConvertToLong && a.path("size").asLong>0,"invalid artifact descriptor")
    val path=Repository.blob(root,a.path("sha256").asText)
    require(Files.isRegularFile(path) && Files.size(path)==a.path("size").asLong && Repository.checksum(path)==a.path("sha256").asText,"artifact checksum/size mismatch")
  }
  private def compatible(n:JsonNode,m:JsonNode):Boolean=all(n,"runtimes").exists(r=>r.path("image")==m.path("runtime").path("image") && r.path("archive")==m.path("runtime").path("archive") && r.path("executors").elements.asScala.exists(_.asText==m.path("execution").path("executor").asText))
  def submit(manifest:JsonNode,submitter:String):ObjectNode=change{n=>
    require(!manifest.has("category") && !manifest.has("categories"),"classification is assigned by the administrator during review")
    require(compatible(n,manifest),"choose a registered runtime from /v1/runtimes")
    require(!manifest.has("artifact"),"only source submissions are accepted")
    require(manifest.path("source_public").isBoolean,"source_public must be a boolean")
    require(manifest.path("build").isObject && manifest.path("build").fieldNames.asScala.toSet==Set("language"),"build requires language")
    val selected=runtimeFor(manifest)
    require(selected.path("build").path("languages").elements.asScala.exists(_.asText==manifest.path("build").path("language").asText),"runtime does not support this build language")
    require(manifest.path("provenance").has("source_archive"),"source archive is required")
    descriptors(manifest).filterNot(_==manifest.path("runtime").path("archive")).foreach(a=>require(ownsBlob(submitter,a.path("sha256").asText),"upload source materials using your own Token first"))
    val name=manifest.path("name").asText;val version=manifest.path("version").asText
    val previous=all(n,"submissions").filter(_.path("manifest").path("name").asText==name)
    if(previous.exists(s=>s.path("status").asText=="approved" && s.path("submitter").asText!=submitter))throw PublishFailure(403,"operator name belongs to another submitter")
    if(previous.exists(s=>s.path("manifest").path("version").asText==version && s.path("status").asText!="rejected"))throw PublishFailure(409,"operator version already submitted")
    val entry=Repository.mapper.createObjectNode().put("id",java.util.UUID.randomUUID().toString).put("submitter",submitter).put("status","pending").put("submitted_at_ms",System.currentTimeMillis())
    entry.putObject("analysis").put("status","queued")
    entry.putObject("build_result").put("status","queued")
    entry.set[JsonNode]("manifest",manifest.deepCopy[JsonNode]());array(n,"submissions").add(entry);entry.deepCopy[ObjectNode]()
  }
  def submissions(submitter:Option[String]=None):ArrayNode=synchronized {val result=Repository.mapper.createArrayNode();all(state,"submissions").filter(s=>submitter.forall(_==s.path("submitter").asText)).reverse.foreach(s=>result.add(s.deepCopy[JsonNode]()));result}
  def submission(id:String):JsonNode=synchronized {all(state,"submissions").find(_.path("id").asText==id).map(_.deepCopy[JsonNode]()).getOrElse(throw PublishFailure(404,"submission not found"))}
  def review(id:String,decision:String,labels:JsonNode,reason:String,trust:Option[Int]=None):Unit=change {n=>
    require(Set("approved","rejected")(decision),"decision must be approved or rejected")
    require(reason.length<=4000,"review note exceeds 4000 characters")
    val entry=all(n,"submissions").find(_.path("id").asText==id).getOrElse(throw PublishFailure(404,"submission not found")).asInstanceOf[ObjectNode]
    if(entry.path("status").asText!="pending")throw PublishFailure(409,"submission already reviewed")
    if(decision=="approved"){
      require(entry.path("analysis").path("status").asText=="complete" && entry.path("build_result").path("status").asText=="succeeded","successful source analysis and build are required before approval")
      val score=trust.getOrElse(entry.path("analysis").path("score").asInt)
      require(score>=0 && score<=5,"trust_score must be an integer from 0 to 5")
      if(score!=entry.path("analysis").path("score").asInt || score<3)require(reason.trim.nonEmpty,"explain a low score approval or a score adjustment")
      entry.put("trust_score",score)
      dacp.operator.OperatorPresentation.validateCategories(labels,required=true)
      require(compatible(n,entry.path("manifest")),"runtime is unavailable")
      Repository.validate(root,entry.path("manifest"))
      val name=entry.path("manifest").path("name").asText
      require(!all(n,"submissions").exists(s=>s.path("status").asText=="approved" && s.path("manifest").path("name").asText==name && s.path("submitter")!=entry.path("submitter")),"operator name belongs to another submitter")
      entry.path("manifest").asInstanceOf[ObjectNode].set[JsonNode]("categories",labels.deepCopy[JsonNode]())
      labels.elements.asScala.foreach(v=>if(!all(n,"categories").contains(v))array(n,"categories").add(v.asText))
    }
    entry.put("status",decision).put("reviewed_at_ms",System.currentTimeMillis()).put("review_note",reason);()
  }
  def runtimeFor(manifest:JsonNode):JsonNode=synchronized {all(state,"runtimes").find(r=>r.path("image")==manifest.path("runtime").path("image") && r.path("archive")==manifest.path("runtime").path("archive")).map(_.deepCopy[JsonNode]()).getOrElse(throw new IllegalArgumentException("runtime not registered"))}
  def grantBlob(submitter:String,hash:String):Unit=change{n=>if(!all(n,"blob_grants").exists(g=>g.path("submitter").asText==submitter && g.path("sha256").asText==hash))array(n,"blob_grants").addObject().put("submitter",submitter).put("sha256",hash);()}
  def ownsBlob(submitter:String,hash:String):Boolean=synchronized {all(state,"blob_grants").exists(g=>g.path("submitter").asText==submitter && g.path("sha256").asText==hash)}
  def processing(id:String,key:String,value:JsonNode,artifact:Option[JsonNode]=None):Unit=change{n=>
    val entry=all(n,"submissions").find(_.path("id").asText==id).get.asInstanceOf[ObjectNode]
    entry.set[JsonNode](key,value)
    artifact.foreach(a=>entry.path("manifest").asInstanceOf[ObjectNode].set[JsonNode]("artifact",a))
    ()
  }
  def publicCatalog:ArrayNode=synchronized {
    val result=Repository.mapper.createArrayNode()
    all(state,"submissions").filter(_.path("status").asText=="approved").foreach{s=>
      val m=s.path("manifest").deepCopy[ObjectNode]();m.remove("build")
      if(!m.path("source_public").asBoolean){
        val provenance=m.path("provenance").asInstanceOf[ObjectNode]
        Seq("source_archive","source_repository","source_revision","build_instructions").foreach(provenance.remove)
      }
      m.putObject("trust").put("score",s.path("trust_score").asInt).put("assessed_by","administrator").put("reviewed_at_ms",s.path("reviewed_at_ms").asLong)
      result.add(m)
    };result
  }
  def descriptors(m:JsonNode):Seq[JsonNode]=Seq(m.path("runtime").path("archive"))++Option(m.get("artifact"))++Option(m.get("provenance")).toSeq.flatMap(Provenance.descriptors)
  def publicBlob(hash:String):Boolean=synchronized {publicCatalog.elements.asScala.exists(m=>descriptors(m).exists(_.path("sha256").asText==hash)) || all(state,"runtimes").exists(r=>Seq(r.path("archive"),r.path("build").path("archive")).exists(_.path("sha256").asText==hash))}
  def close():Unit={lock.release();channel.close()}
}
