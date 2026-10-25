package dacp.union

import java.nio.file.{Files,Path}
import java.security.{KeyStore,KeyFactory,MessageDigest}
import java.security.cert.{CertificateFactory,X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import javax.net.ssl._
import scala.jdk.CollectionConverters._

/** TLS-verified leaf certificate fingerprints are identities, never request headers or body claims. */
final class Tls(val cert:Path,val key:Path,val ca:Path) {
  private val factory=CertificateFactory.getInstance("X.509")
  private def certificates(p:Path):Vector[X509Certificate]={val in=Files.newInputStream(p);try factory.generateCertificates(in).asScala.map(_.asInstanceOf[X509Certificate]).toVector finally in.close()}
  private val chain=certificates(cert)
  require(chain.nonEmpty,"missing certificate chain")
  val identity:String=Tls.fingerprint(chain.head)
  private val pem=Files.readString(key)
  require(pem.contains("-----BEGIN PRIVATE KEY-----"),"TLS key must be unencrypted PKCS#8 PEM")
  private val bytes=java.util.Base64.getMimeDecoder.decode(pem.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----",""))
  private val privateKey=KeyFactory.getInstance(chain.head.getPublicKey.getAlgorithm).generatePrivate(new PKCS8EncodedKeySpec(bytes))
  private val password=java.util.UUID.randomUUID().toString.toCharArray
  private val keys=KeyStore.getInstance("PKCS12");keys.load(null,null)
  keys.setKeyEntry("identity",privateKey,password,chain.toArray[java.security.cert.Certificate])
  private val km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm);km.init(keys,password)
  java.util.Arrays.fill(password,'\u0000')
  private val trust=KeyStore.getInstance("PKCS12");trust.load(null,null)
  certificates(ca).zipWithIndex.foreach {case(c,i)=>trust.setCertificateEntry(i.toString,c)}
  private val tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm);tm.init(trust)
  private val base=tm.getTrustManagers.collectFirst {case t:X509ExtendedTrustManager=>t}.get
  def sslContext:SSLContext={val c=SSLContext.getInstance("TLS");c.init(km.getKeyManagers,tm.getTrustManagers,null);c}
  def trustManager(pin:Option[String]=None,observed:String=>Unit=_=>()):X509ExtendedTrustManager={
    val check=new X509ExtendedTrustManager {
      private def pinned(cs:Array[X509Certificate]):Unit={
        val actual=Tls.fingerprint(cs.head)
        pin.foreach(p=>if(actual!=p)throw new java.security.cert.CertificateException("node identity mismatch"))
        observed(actual)
      }
      def getAcceptedIssuers():Array[X509Certificate]=base.getAcceptedIssuers
      def checkClientTrusted(c:Array[X509Certificate],a:String):Unit=base.checkClientTrusted(c,a)
      def checkClientTrusted(c:Array[X509Certificate],a:String,s:java.net.Socket):Unit=base.checkClientTrusted(c,a,s)
      def checkClientTrusted(c:Array[X509Certificate],a:String,e:SSLEngine):Unit=base.checkClientTrusted(c,a,e)
      def checkServerTrusted(c:Array[X509Certificate],a:String):Unit={base.checkServerTrusted(c,a);pinned(c)}
      def checkServerTrusted(c:Array[X509Certificate],a:String,s:java.net.Socket):Unit={base.checkServerTrusted(c,a,s);pinned(c)}
      def checkServerTrusted(c:Array[X509Certificate],a:String,e:SSLEngine):Unit={base.checkServerTrusted(c,a,e);pinned(c)}
    }
    check
  }
}
object Tls {
  def fingerprint(cert:java.security.cert.Certificate):String=MessageDigest.getInstance("SHA-256").digest(cert.getEncoded).map(b=>f"${b&255}%02x").mkString
}
