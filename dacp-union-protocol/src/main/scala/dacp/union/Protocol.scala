package dacp.union


import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.{Files,Path,StandardCopyOption}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

final case class UnionFailure(code:org.apache.arrow.flight.FlightStatusCode, detail:String) extends RuntimeException(detail)
object Json {
  val mapper=new ObjectMapper()
  def obj():ObjectNode=mapper.createObjectNode()
  def parse(text:String):JsonNode=mapper.readTree(text)
  def text(n:JsonNode,k:String):String={
    val v=n.path(k);require(v.isTextual && v.asText.nonEmpty,s"$k must be nonempty text");v.asText
  }
  def fields(n:JsonNode,required:Set[String],optional:Set[String]=Set.empty):Unit=
    require(n!=null && n.isObject && required.subsetOf(n.fieldNames.asScala.toSet) && n.fieldNames.asScala.toSet.subsetOf(required++optional),"invalid request fields")
  def id(s:String):String={require(s.matches("[A-Za-z0-9_-]{1,128}"),"invalid identifier");s}
  def identity(s:String):String={require(s.matches("[a-f0-9]{64}"),"invalid certificate identity");s}
  def array(n:JsonNode,k:String):Vector[JsonNode]={require(n.path(k).isArray,s"$k must be an array");n.path(k).elements.asScala.toVector}
  def strings(n:JsonNode,k:String):Vector[String]=array(n,k).map {v=>require(v.isTextual,"array values must be text");v.asText}
  def number(n:JsonNode,k:String,min:Long,max:Long):Long={val v=n.path(k);require(v.isIntegralNumber && v.canConvertToLong && v.asLong>=min && v.asLong<=max,s"invalid $k");v.asLong}
  def uuid():String=java.util.UUID.randomUUID().toString
  def now:Long=System.currentTimeMillis()
  def write(path:Path,n:JsonNode):Unit={
    Files.createDirectories(path.toAbsolutePath.getParent)
    val tmp=Files.createTempFile(path.toAbsolutePath.getParent,".state-",".json")
    try {Files.write(tmp,mapper.writeValueAsBytes(n));Files.move(tmp,path.toAbsolutePath,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)}finally Files.deleteIfExists(tmp)
  }
  private val locks=new java.util.concurrent.ConcurrentHashMap[Path,Object]()
  def locked[T](directory:Path)(f: => T):T=locks.computeIfAbsent(directory.toAbsolutePath.normalize(),_=>new Object()).synchronized {
    Files.createDirectories(directory)
    val channel=java.nio.channels.FileChannel.open(directory.resolve(".lock"),java.nio.file.StandardOpenOption.CREATE,java.nio.file.StandardOpenOption.WRITE)
    try {val lock=channel.lock();try f finally lock.release()}finally channel.close()
  }
  def read(path:Path):JsonNode=mapper.readTree(path.toFile)
  def yaml(path:Path):JsonNode={
    val o=new org.yaml.snakeyaml.LoaderOptions();o.setAllowDuplicateKeys(false)
    mapper.valueToTree(new org.yaml.snakeyaml.Yaml(o).load[Object](Files.readString(path)))
  }
  def origin(s:String):String={
    val u=java.net.URI.create(s)
    require(u.getScheme=="dacp" && u.getHost!=null && u.getUserInfo==null && u.getQuery==null && u.getFragment==null &&
      Set("","/")(Option(u.getPath).getOrElse("")) && (u.getPort == -1 || u.getPort>0 && u.getPort<=65535),"expected dacp://host:port Flight origin")
    val host=u.getHost.toLowerCase(java.util.Locale.ROOT)
    val authority=if(host.contains(":") && !host.startsWith("["))s"[$host]"else host
    s"dacp://$authority:${if(u.getPort == -1)8815 else u.getPort}"
  }
}
