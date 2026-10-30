package dacp.proxy


import java.nio.file.{Files,Path}
import org.yaml.snakeyaml.{LoaderOptions,Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import scala.jdk.CollectionConverters._

/** A fixed, administrator-selected destination; clients cannot select an upstream. */
final case class ProxyConfig(host:String,port:Int,upstreamHost:String,upstreamPort:Int,
  maxConnections:Int=256,bufferBytes:Int=65536,connectTimeoutSeconds:Int=10,idleTimeoutSeconds:Int=0) {
  require(host.nonEmpty && upstreamHost.nonEmpty,"host and upstream.host must not be empty")
  require(port>=0 && port<=65535,"port must be 0..65535")
  require(upstreamPort>=1 && upstreamPort<=65535,"upstream.port must be 1..65535")
  require(maxConnections>=1 && maxConnections<=65536,"max_connections must be 1..65536")
  require(bufferBytes>=1024 && bufferBytes<=1048576,"buffer_bytes must be 1024..1048576")
  require(connectTimeoutSeconds>=1 && connectTimeoutSeconds<=120,"connect_timeout_seconds must be 1..120")
  require(idleTimeoutSeconds>=0 && idleTimeoutSeconds<=86400,"idle_timeout_seconds must be 0..86400")
  require(maxConnections.toLong*bufferBytes*2<=1024L*1024*1024,"configured relay buffers exceed 1 GiB")
}
object ProxyConfig {
  def load(path:Path):ProxyConfig={
    require(Files.size(path)<=65536,"proxy configuration exceeds 64 KiB")
    val options=new LoaderOptions();options.setAllowDuplicateKeys(false);options.setCodePointLimit(65536);options.setMaxAliasesForCollections(0)
    val in=Files.newInputStream(path)
    val raw=try new Yaml(new SafeConstructor(options)).load[Any](in)finally in.close()
    def mapping(value:Any,name:String):Map[String,Any]=value match {
      case m:java.util.Map[_,_]=>m.asScala.map{case(k:String,v)=>k->v;case _=>throw new IllegalArgumentException(name+" keys must be strings")}.toMap
      case _=>throw new IllegalArgumentException(name+" must be a mapping")
    }
    val n=mapping(raw,"configuration")
    require(n.keySet.subsetOf(Set("host","port","upstream","max_connections","buffer_bytes","connect_timeout_seconds","idle_timeout_seconds")),"unknown proxy setting")
    val upstream=mapping(n.getOrElse("upstream",null),"upstream")
    require(upstream.keySet==Set("host","port"),"upstream requires only host and port")
    def text(m:Map[String,Any],key:String,default:Option[String]=None):String=m.get(key).orElse(default) match {
      case Some(s:String) if s.nonEmpty && !s.exists(_.isControl)=>s
      case _=>throw new IllegalArgumentException(key+" must be nonempty text")
    }
    def number(m:Map[String,Any],key:String,default:Int):Int=m.get(key) match {
      case None=>default
      case Some(v:java.lang.Integer)=>v.intValue
      case Some(v:java.lang.Long) if v.longValue>=Int.MinValue && v.longValue<=Int.MaxValue=>v.intValue
      case _=>throw new IllegalArgumentException(key+" must be an integer")
    }
    ProxyConfig(text(n,"host",Some("127.0.0.1")),number(n,"port",8815),text(upstream,"host"),number(upstream,"port",8815),
      number(n,"max_connections",256),number(n,"buffer_bytes",65536),number(n,"connect_timeout_seconds",10),number(n,"idle_timeout_seconds",0))
  }
}
