package dacp.union


import java.nio.file.{Files,Path,StandardCopyOption}
import java.nio.file.attribute.PosixFilePermissions
import java.security.{MessageDigest,SecureRandom}
import java.util.concurrent.{Executors,TimeUnit}
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.JsonNode

final case class Credential(id:String,subject:String,kind:String,digest:String,created:Long,expires:Long,
  revoked:Boolean=false,unionId:String="",fingerprint:String="",secret:Option[String]=None) {
  def active:Boolean = !revoked && (expires==0 || expires>System.currentTimeMillis())
}
/** File updates reload on a background thread. RPC validation only reads the immutable memory snapshot. */
final class Credentials(val directory:Path) extends AutoCloseable {
  import Credentials._
  protectDirectory(directory)
  private val file=directory.resolve("credentials.json")
  private val subjectsFile=directory.resolve("subjects.json")
  @volatile private var subjects=Map.empty[String,Set[String]]
  @volatile private var entries=Map.empty[String,Credential]
  @volatile private var temporary=Map.empty[String,Credential]
  private val timer=Executors.newSingleThreadScheduledExecutor(r=>{val t=new Thread(r,"dacp-credentials");t.setDaemon(true);t})
  reload()
  timer.scheduleWithFixedDelay(new Runnable{def run():Unit=try reload()catch{case scala.util.control.NonFatal(_)=>entries=Map.empty;subjects=Map.empty}},250,250,TimeUnit.MILLISECONDS)
  private def read():Map[String,Credential]={
    if(!Files.exists(file))return Map.empty
    val n=Json.read(file);require(n.isArray,"invalid credential store")
    val values=n.elements.asScala.map{v=>
      val c=Credential(v.path("id").asText,v.path("subject").asText,v.path("kind").asText,v.path("digest").asText,v.path("created_at_ms").asLong,v.path("expires_at_ms").asLong,v.path("revoked").asBoolean,v.path("union_id").asText,v.path("fingerprint").asText,Option(v.get("secret")).map(_.asText))
      require(Set("token","union","s3")(c.kind) && c.id.matches("[a-zA-Z0-9_-]+") && c.subject.nonEmpty && c.subject.length<=256 && !c.subject.exists(_.isControl) && c.digest.matches("[a-f0-9]{64}") && c.created>0 && c.expires>=0,"invalid credential entry")
      require(c.kind!="union" || (c.unionId.nonEmpty && c.fingerprint.matches("[a-f0-9]{64}")),"invalid union credential binding")
      require(c.kind!="s3" || c.secret.exists(_.nonEmpty),"missing S3 secret")
      c.id->c
    }.toVector
    require(values.map(_._1).distinct.size==values.size,"duplicate credential ID")
    values.toMap
  }
  private def readSubjects():Map[String,Set[String]]={
    if(!Files.exists(subjectsFile))return Map.empty
    val n=Json.read(subjectsFile);require(n.isObject,"subjects must be an object")
    n.fields.asScala.map { e=>
      validateSubject(e.getKey)
      Json.fields(e.getValue,Set("roles"))
      val roles=Json.strings(e.getValue,"roles");require(roles.distinct.size==roles.size && roles.forall(Set("reader","runner")),"invalid subject roles")
      e.getKey->roles.toSet
    }.toMap
  }
  private def reload():Unit={val nextSubjects=try readSubjects()catch{case scala.util.control.NonFatal(_)=>Map.empty[String,Set[String]]};val nextEntries=read();subjects=nextSubjects;entries=nextEntries}
  private def validateSubject(subject:String):Unit=require(subject.nonEmpty && subject.length<=256 && !subject.exists(_.isControl),"invalid credential subject")
  private def writeSubjects(values:Map[String,Set[String]]):Unit={
    val n=Json.obj();values.toVector.sortBy(_._1).foreach { case(name,rs)=>val a=n.putObject(name).putArray("roles");rs.toVector.sorted.foreach(a.add) }
    val tmp=Files.createTempFile(directory,".subjects-",".json");protectFile(tmp)
    try {Files.write(tmp,Json.mapper.writeValueAsBytes(n));Files.move(tmp,subjectsFile,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)}finally Files.deleteIfExists(tmp)
    subjects=values
  }
  def setRoles(subject:String,roles:Set[String]):Unit=synchronized {Json.locked(directory){
    validateSubject(subject);require(roles.subsetOf(Set("reader","runner")),"roles must be reader or runner")
    writeSubjects(readSubjects()+(subject->roles));event("set_roles","",subject)
  }}
  /** One account provisions both protocols; neither credential inherits authority from the other. */
  def createAccount(subject:String,role:String="reader",expires:Long=0):JsonNode=synchronized {Json.locked(directory){
    validateSubject(subject);require(Set("reader","runner")(role),"role must be reader or runner")
    require(expires==0 || expires>System.currentTimeMillis(),"invalid credential expiry")
    val previous=read();val previousSubjects=readSubjects()
    require(!previousSubjects.contains(subject) && !previous.values.exists(_.subject==subject),"account already exists")
    val now=System.currentTimeMillis();val token=random(32);val secret=random(32)
    val t=Credential(random(12),subject,"token",hash(token),now,expires)
    val key=Credential(random(12),subject,"s3",hash(secret),now,expires,secret=Some(secret))
    write(previous+(t.id->t)+(key.id->key))
    try writeSubjects(previousSubjects+(subject->Set(role)))catch{case scala.util.control.NonFatal(e)=>
      try write(previous)catch{case scala.util.control.NonFatal(cleanup)=>e.addSuppressed(cleanup)}
      throw e
    }
    event("create_account",t.id,subject)
    val result=Json.obj().put("subject",subject).put("token_id",t.id).put("token",token).put("access_key_id",key.id).put("secret_key",secret).put("expires_at_ms",expires)
    result.putArray("roles").add(role);result
  }}
  def createToken(subject:String,expires:Long=0):(JsonNode,String)={
    require(subjects.contains(subject) && entries.values.exists(c=>c.subject==subject && c.kind=="s3"),"create-account first")
    create(subject,"token",expires)
  }
  def listSubjects:JsonNode={val n=Json.obj();subjects.toVector.sortBy(_._1).foreach{case(name,rs)=>val a=n.putObject(name).putArray("roles");rs.toVector.sorted.foreach(a.add)};n}
  def permissions(c:Credential):Set[String]= {
    if(temporary.contains(c.id))Set("data:read","job:execute","job:manage:self")
    else if(!Set("token","s3")(c.kind))Set.empty
    else {
      val roles=subjects.getOrElse(c.subject,Set.empty)
      (if(roles("reader") || roles("runner"))Set("data:read")else Set.empty[String]) ++
        (if(c.kind=="token" && roles("runner"))Set("job:execute","job:manage:self")else Set.empty[String])
    }
  }
  private def json(c:Credential,privateFields:Boolean):JsonNode={
    val n=Json.obj().put("id",c.id).put("subject",c.subject).put("kind",c.kind).put("created_at_ms",c.created).put("expires_at_ms",c.expires).put("revoked",c.revoked)
    if(c.kind=="union")n.put("union_id",c.unionId).put("fingerprint",c.fingerprint)
    if(privateFields){n.put("digest",c.digest);c.secret.foreach(n.put("secret",_))};n
  }
  private def write(values:Map[String,Credential]):Unit={
    val n=Json.mapper.createArrayNode();values.values.toVector.sortBy(_.id).foreach(c=>n.add(json(c,true)))
    val tmp=Files.createTempFile(directory,".credentials-",".json");protectFile(tmp)
    try {Files.write(tmp,Json.mapper.writeValueAsBytes(n));Files.move(tmp,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)}finally Files.deleteIfExists(tmp)
    entries=values
  }
  private def event(operation:String,id:String,subject:String):Unit={
    val log=directory.resolve("credential-audit.jsonl")
    if(Files.exists(log) && Files.size(log)>1024*1024)Files.move(log,directory.resolve("credential-audit.previous.jsonl"),StandardCopyOption.REPLACE_EXISTING)
    val bytes=(Json.obj().put("at_ms",System.currentTimeMillis()).put("operation",operation).put("credential_id",id).put("subject",subject).toString+"\n").getBytes(java.nio.charset.StandardCharsets.UTF_8)
    Files.write(log,bytes,java.nio.file.StandardOpenOption.CREATE,java.nio.file.StandardOpenOption.APPEND);protectFile(log)
  }
  def auditAccess(operation:String,subject:String,status:String,elapsedMs:Long):Unit=synchronized {
    val log=directory.resolve("authentication-audit.jsonl")
    if(Files.exists(log) && Files.size(log)>1024*1024)Files.move(log,directory.resolve("authentication-audit.previous.jsonl"),StandardCopyOption.REPLACE_EXISTING)
    val n=Json.obj().put("at_ms",System.currentTimeMillis()).put("operation",operation).put("subject",subject).put("status",status).put("elapsed_ms",elapsedMs)
    Files.writeString(log,n.toString+"\n",java.nio.file.StandardOpenOption.CREATE,java.nio.file.StandardOpenOption.APPEND);protectFile(log)
  }
  def create(subject:String,kind:String="token",expires:Long=0,unionId:String="",fingerprint:String=""):(JsonNode,String)=synchronized {Json.locked(directory){
    require(subject.nonEmpty && subject.length<=256 && !subject.exists(_.isControl),"invalid credential subject")
    require(Set("token","union","s3")(kind) && (expires==0 || expires>System.currentTimeMillis()),"invalid credential kind or expiry")
    require(kind!="union" || (unionId.matches("[A-Za-z0-9_-]+") && fingerprint.matches("[a-f0-9]{64}")),"union ID and certificate fingerprint required")
    val id=random(12);val value=random(32)
    val c=Credential(id,subject,kind,hash(value),System.currentTimeMillis(),expires,unionId=unionId,fingerprint=fingerprint,secret=if(kind=="s3")Some(value)else None)
    write(read()+(id->c));event("create",id,subject);json(c,false)->value
  }}
  def list:Vector[JsonNode]=entries.values.toVector.sortBy(_.created).map(json(_,false))
  def revoke(id:String):Unit=synchronized {Json.locked(directory){val values=read();val c=values.getOrElse(id,throw new IllegalArgumentException("unknown credential"));write(values+(id->c.copy(revoked=true)));event("revoke",id,c.subject)}}
  def rotate(id:String):(JsonNode,String)={val c=entries.getOrElse(id,throw new IllegalArgumentException("unknown credential"));val replacement=create(c.subject,c.kind,c.expires,c.unionId,c.fingerprint);revoke(id);replacement}
  def authenticate(secret:String,peer:Option[String]):Option[Credential]={
    val digest=hash(secret)
    (entries.valuesIterator++temporary.valuesIterator).find(c=>c.kind!="s3" && c.active && constant(c.digest,digest) && (c.kind!="union" || peer.contains(c.fingerprint)))
  }
  def valid(id:String):Boolean=entries.get(id).orElse(temporary.get(id)).exists(_.active)
  def union(id:String,fingerprint:String):Option[Credential]=entries.values.find(c=>c.kind=="union" && c.unionId==id && c.fingerprint==fingerprint && c.active)
  def s3(id:String):Option[Credential]=entries.get(id).filter(c=>c.kind=="s3" && c.active)
  /** Private loopback client, never written to configuration, disk, logs or downstream nodes. */
  def internalToken():String=synchronized{val token=random(32);val id=random(12);temporary += id->Credential(id,"dacp-internal","token",hash(token),System.currentTimeMillis(),0);token}
  def close():Unit=timer.shutdownNow()
}
object Credentials {
  private val randomSource=new SecureRandom()
  def random(bytes:Int):String={val b=new Array[Byte](bytes);randomSource.nextBytes(b);java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(b)}
  def hash(s:String):String=MessageDigest.getInstance("SHA-256").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map(b=>f"${b&255}%02x").mkString
  def constant(a:String,b:String):Boolean=MessageDigest.isEqual(a.getBytes(java.nio.charset.StandardCharsets.UTF_8),b.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  def protectDirectory(p:Path):Unit={Files.createDirectories(p);Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"))}
  def protectFile(p:Path):Unit=Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-------"))
}
/** Local administration only. Secret values are returned once; list never reveals them. */
object CredentialAdmin {
  val commands=Set("create-account","create-token","grant-union","list-credentials","revoke-credential","rotate-credential","list-subjects","set-roles","presign-get")
  def main(args:Array[String]):Unit={
    require(args.length>=2 && commands(args(0)),"COMMAND DIRECTORY [OPTIONS]")
    val store=new Credentials(Path.of(args(1)))
    def options(allowed:Set[String]):Map[String,String]={
      require((args.length-2)%2==0,"options need values")
      val raw=args.drop(2).grouped(2).map(a=>a(0)->a(1)).toVector
      require(raw.map(_._1).distinct.size==raw.size && raw.forall(x=>allowed(x._1)),"unknown or duplicate option")
      raw.toMap
    }
    def expires(o:Map[String,String]):Long=o.get("--expires-in").map { s=>val seconds=s.toLong;require(seconds>0 && seconds<=315360000,"invalid credential lifetime");System.currentTimeMillis()+seconds*1000 }.getOrElse(0L)
    def subject(o:Map[String,String]):String=o.getOrElse("--subject",throw new IllegalArgumentException("--subject required"))
    try {
      val result=args(0) match {
        case "list-credentials"=>require(args.length==2);Json.mapper.valueToTree[JsonNode](store.list.asJava)
        case "list-subjects"=>require(args.length==2);store.listSubjects
        case "set-roles"=>
          require(args.length==4,"set-roles DIRECTORY SUBJECT reader|runner|reader,runner|none")
          val roles=if(args(3)=="none")Set.empty[String]else args(3).split(",",-1).toSet
          store.setRoles(args(2),roles);store.listSubjects
        case "revoke-credential"=>require(args.length==3);store.revoke(args(2));Json.obj().put("revoked",args(2))
        case "rotate-credential"=>require(args.length==3);issued(store.rotate(args(2)))
        case "presign-get"=>
          require(args.length==4 || args.length==5,"presign-get DIRECTORY ACCESS_KEY HTTPS_OBJECT_URL [SECONDS]")
          val c=store.s3(args(2)).getOrElse(throw new IllegalArgumentException("S3 credential unavailable"))
          require(store.permissions(c)("data:read"),"S3 subject requires reader or runner")
          Json.obj().put("url",SigV4.presignGet(java.net.URI.create(args(3)),c.id,c.secret.get,if(args.length==5)args(4).toLong else 900).toASCIIString)
        case "create-account"=>
          val o=options(Set("--subject","--role","--expires-in"));store.createAccount(subject(o),o.getOrElse("--role","reader"),expires(o))
        case "create-token"=>
          val o=options(Set("--subject","--expires-in"));issued(store.createToken(subject(o),expires(o)))
        case "grant-union"=>
          val o=options(Set("--subject","--expires-in","--union-id","--fingerprint"))
          issued(store.create(subject(o),"union",expires(o),o.getOrElse("--union-id",""),o.getOrElse("--fingerprint","")))
        case _=>throw new IllegalArgumentException("unknown credential operation")
      };println(result.toPrettyString)
    }finally store.close()
  }
  private def issued(pair:(JsonNode,String)):JsonNode={val n=pair._1.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();n.put(if(n.path("kind").asText=="s3")"secret_key"else "token",pair._2);n}
}
