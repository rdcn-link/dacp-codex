package dacp

import java.nio.file.{Files,Path}
import java.net.URI
import java.net.http.{HttpClient,HttpRequest}
import javax.net.ssl.{SSLContext,TrustManagerFactory}
import java.security.KeyStore
import scala.sys.process._

private[dacp] object SecurityTestSupport {
  lazy val directory:Path={
    val dir=Files.createTempDirectory("dacp-security-tls-")
    require(Process(Seq("openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-subj","/CN=localhost","-addext","subjectAltName=DNS:localhost","-keyout",dir.resolve("key.pem").toString,"-out",dir.resolve("cert.pem").toString)).!(ProcessLogger(_=>(),_=>()))==0)
    dir
  }
  def certificate:Path=directory.resolve("cert.pem")
  def key:Path=directory.resolve("key.pem")
  def client(cert:Path=certificate):HttpClient={
    val store=KeyStore.getInstance(KeyStore.getDefaultType);store.load(null,null)
    val in=Files.newInputStream(cert)
    try store.setCertificateEntry("ca",java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in))finally in.close()
    val tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm);tm.init(store)
    val ssl=SSLContext.getInstance("TLS");ssl.init(null,tm.getTrustManagers,null)
    HttpClient.newBuilder().sslContext(ssl).build()
  }
  val credentials=new java.util.concurrent.ConcurrentHashMap[Int,(String,String)]()
  def register(port:Int,c:Config):Unit={val issued=c.credentials.create("s3-test","s3");credentials.put(port,issued._1.path("id").asText->issued._2);()}
  // Independent SigV4 header signer for request tests. Production validation does not call this code.
  def signed(port:Int,path:String,method:String="GET",headers:Map[String,String]=Map.empty):HttpRequest={
    val (id,secret)=credentials.get(port)
    val uri=URI.create(s"https://localhost:$port$path")
    def hex(b:Array[Byte])=b.map(v=>f"${v&255}%02x").mkString
    def sha(s:String)=hex(java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")))
    def mac(k:Array[Byte],s:String)={val m=javax.crypto.Mac.getInstance("HmacSHA256");m.init(new javax.crypto.spec.SecretKeySpec(k,"HmacSHA256"));m.doFinal(s.getBytes("UTF-8"))}
    val date=java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(java.time.ZoneOffset.UTC).format(java.time.Instant.now())
    val scope=date.take(8)+"/us-east-1/s3/aws4_request"
    val payload=sha("");val signed="host;x-amz-content-sha256;x-amz-date"
    val query=dacp.union.SigV4.query(uri).map{case(k,v)=>dacp.union.SigV4.encode(k)->dacp.union.SigV4.encode(v)}.sorted.map{case(k,v)=>k+"="+v}.mkString("&")
    val canonical=method+"\n"+uri.getRawPath+"\n"+query+"\nhost:"+uri.getRawAuthority+"\nx-amz-content-sha256:"+payload+"\nx-amz-date:"+date+"\n\n"+signed+"\n"+payload
    val key=Seq(date.take(8),"us-east-1","s3","aws4_request").foldLeft(("AWS4"+secret).getBytes("UTF-8"))((k,s)=>mac(k,s))
    val signature=hex(mac(key,"AWS4-HMAC-SHA256\n"+date+"\n"+scope+"\n"+sha(canonical)))
    val b=HttpRequest.newBuilder(uri).method(method,HttpRequest.BodyPublishers.noBody()).header("x-amz-date",date).header("x-amz-content-sha256",payload)
    if(!headers.keys.exists(_.equalsIgnoreCase("Authorization")))b.header("Authorization",s"AWS4-HMAC-SHA256 Credential=$id/$scope, SignedHeaders=$signed, Signature=$signature")
    headers.foreach{case(k,v)=>b.header(k,v)};b.build()
  }
}
