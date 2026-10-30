package dacp.union


import java.net.{URI,URLDecoder,URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.time.{Instant,ZoneOffset,LocalDateTime}
import java.time.format.DateTimeFormatter

/** Read-only S3 SigV4. Authentication is completed once per HTTP request. */
object SigV4 {
  private val timestamp=DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
  private val emptyHash=sha(Array.emptyByteArray)
  private def hex(bytes:Array[Byte]):String=bytes.map(b=>f"${b&255}%02x").mkString
  private def sha(bytes:Array[Byte]):String=hex(MessageDigest.getInstance("SHA-256").digest(bytes))
  private def hmac(key:Array[Byte],text:String):Array[Byte]={val m=Mac.getInstance("HmacSHA256");m.init(new SecretKeySpec(key,"HmacSHA256"));m.doFinal(text.getBytes(UTF_8))}
  def encode(text:String):String=URLEncoder.encode(text,UTF_8).replace("+","%20").replace("%7E","~").replace("*","%2A")
  private def decode(text:String):String=URLDecoder.decode(text.replace("+","%2B"),UTF_8)
  def query(uri:URI):Vector[(String,String)]=Option(uri.getRawQuery).toVector.flatMap(_.split("&").filter(_.nonEmpty).map {p=>val a=p.split("=",2);decode(a(0))->decode(if(a.length==2)a(1)else "")})
  private def canonicalQuery(values:Vector[(String,String)]):String=values.map{case(k,v)=>encode(k)->encode(v)}.sortBy(identity).map{case(k,v)=>s"$k=$v"}.mkString("&")
  private def signingKey(secret:String,date:String,region:String):Array[Byte]=hmac(hmac(hmac(hmac(("AWS4"+secret).getBytes(UTF_8),date),region),"s3"),"aws4_request")
  private def signature(method:String,uri:URI,headers:Map[String,Vector[String]],signed:String,payload:String,date:String,scope:String,secret:String,queryValues:Vector[(String,String)]):String={
    val names=signed.split(";",-1).toVector
    require(names.nonEmpty && names==names.sorted && names.distinct==names && names.forall(_.matches("[a-z0-9-]+")) && names.contains("host"),"invalid signed headers")
    val normalized=names.map { name=>
      val values=headers.getOrElse(name,throw new IllegalArgumentException("missing signed header"))
      require(values.nonEmpty,"empty signed header")
      name+":"+values.map(_.trim.replaceAll("\\s+"," ")).mkString(",")+"\n"
    }.mkString
    val path=Option(uri.getRawPath).filter(_.nonEmpty).getOrElse("/")
    val canonical=method+"\n"+path+"\n"+canonicalQuery(queryValues)+"\n"+normalized+"\n"+signed+"\n"+payload
    val stringToSign="AWS4-HMAC-SHA256\n"+date+"\n"+scope+"\n"+sha(canonical.getBytes(UTF_8))
    val parts=scope.split("/",-1)
    hex(hmac(signingKey(secret,parts(0),parts(1)),stringToSign))
  }
  /** Returns the admitted subject. Revocation after this point does not interrupt this response. */
  def authenticate(method:String,uri:URI,rawHeaders:Map[String,Vector[String]],credentials:Credentials,now:Instant=Instant.now()):Credential={
    require(Set("GET","HEAD")(method),"unsupported S3 method")
    val headers=rawHeaders.toVector.groupMapReduce(_._1.toLowerCase(java.util.Locale.ROOT))(_._2)(_++_)
    require(!headers.contains("x-amz-security-token"),"session credentials are unsupported")
    require(headers.get("host").exists(_.size==1),"one Host header required")
    val qs=query(uri);val signing=qs.filter(_._1.startsWith("X-Amz-"))
    require(signing.map(_._1).distinct.size==signing.size,"duplicate signing parameter")
    val q=signing.toMap
    val authorization=headers.get("authorization")
    val presigned=signing.nonEmpty
    require(!(presigned && authorization.nonEmpty),"ambiguous authentication")
    def single(name:String):String={val values=headers.getOrElse(name,throw new IllegalArgumentException("missing header"));require(values.size==1,"duplicate authentication header");values.head}
    val (credential,signed,provided,date,payload)=if(presigned){
      require(method=="GET" && uri.getRawPath.stripPrefix("/").split("/",2).length==2 && !uri.getRawPath.endsWith("/"),"presigned URL requires one object GET")
      require(q.keySet==Set("X-Amz-Algorithm","X-Amz-Credential","X-Amz-Date","X-Amz-Expires","X-Amz-SignedHeaders","X-Amz-Signature") && q("X-Amz-Algorithm")=="AWS4-HMAC-SHA256","invalid presigned parameters")
      val expires=q("X-Amz-Expires").toLong;require(expires>=1 && expires<=604800,"invalid presigned expiry")
      val at=LocalDateTime.parse(q("X-Amz-Date"),timestamp).toInstant(ZoneOffset.UTC)
      require(!now.isAfter(at.plusSeconds(expires)) && !at.isAfter(now.plusSeconds(900)),"presigned URL expired or not yet valid")
      (q("X-Amz-Credential"),q("X-Amz-SignedHeaders"),q("X-Amz-Signature"),q("X-Amz-Date"),"UNSIGNED-PAYLOAD")
    }else{
      val raw=single("authorization");require(raw.startsWith("AWS4-HMAC-SHA256 "),"SigV4 Authorization required")
      val pairs=raw.stripPrefix("AWS4-HMAC-SHA256 ").split(",").map(_.trim.split("=",2));require(pairs.forall(_.length==2),"invalid Authorization")
      val fields=pairs.map(a=>a(0)->a(1)).toMap
      require(fields.size==pairs.length && fields.keySet==Set("Credential","SignedHeaders","Signature"),"invalid Authorization fields")
      val date=single("x-amz-date");val at=LocalDateTime.parse(date,timestamp).toInstant(ZoneOffset.UTC)
      require(math.abs(java.time.Duration.between(at,now).getSeconds)<=900,"request clock skew exceeds 15 minutes")
      require(fields("SignedHeaders").split(";").contains("x-amz-date"),"x-amz-date must be signed")
      val payload=single("x-amz-content-sha256");require(payload==emptyHash || payload=="UNSIGNED-PAYLOAD","read requests must have an empty payload")
      (fields("Credential"),fields("SignedHeaders"),fields("Signature"),date,payload)
    }
    require(provided.matches("[a-f0-9]{64}"),"invalid signature")
    val parts=credential.split("/",-1)
    require(parts.length==5 && parts(1)==date.take(8) && parts(2).matches("[a-z0-9-]+") && parts(3)=="s3" && parts(4)=="aws4_request","invalid signing scope")
    val key=credentials.s3(parts(0)).getOrElse(throw new IllegalArgumentException("S3 credential unavailable"))
    val scope=parts.drop(1).mkString("/")
    val expected=signature(method,uri,headers,signed,payload,date,scope,key.secret.get,qs.filterNot(_._1=="X-Amz-Signature"))
    require(Credentials.constant(expected,provided),"signature mismatch");key
  }
  /** Local signing utility. Only HTTPS object GETs are issued; the URL is itself a temporary credential. */
  def presignGet(uri:URI,accessKey:String,secret:String,expiresSeconds:Long=900,region:String="us-east-1",now:Instant=Instant.now()):URI={
    require(uri.getScheme=="https" && uri.getHost!=null && uri.getUserInfo==null && uri.getFragment==null && uri.getRawQuery==null,"expected HTTPS object URL")
    require(expiresSeconds>=1 && expiresSeconds<=604800 && region.matches("[a-z0-9-]+"),"invalid presign settings")
    require(uri.getRawPath.stripPrefix("/").split("/",2).length==2 && !uri.getRawPath.endsWith("/"),"object path required")
    val date=timestamp.format(LocalDateTime.ofInstant(now,ZoneOffset.UTC));val scope=date.take(8)+"/"+region+"/s3/aws4_request"
    val qs=Vector("X-Amz-Algorithm"->"AWS4-HMAC-SHA256","X-Amz-Credential"->(accessKey+"/"+scope),"X-Amz-Date"->date,
      "X-Amz-Expires"->expiresSeconds.toString,"X-Amz-SignedHeaders"->"host")
    val sig=signature("GET",uri,Map("host"->Vector(uri.getRawAuthority)),"host","UNSIGNED-PAYLOAD",date,scope,secret,qs)
    URI.create(uri.toASCIIString+"?"+canonicalQuery(qs:+("X-Amz-Signature"->sig)))
  }
}
