package dacp.unionserver

import java.nio.file.{Files,Path,StandardOpenOption}
import java.nio.channels.FileChannel
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import dacp.union.Json
import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Only routing identities are persisted, never a replica of member catalogs. Single serving owner. */
private[unionserver] final class DatasetRoutes(directory:Path) extends AutoCloseable {
  Files.createDirectories(directory)
  private val channel=FileChannel.open(directory.resolve(".owner"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)
  private val lock=try Option(channel.tryLock()).getOrElse(throw new IllegalStateException("dataset routes already in use"))catch{case t:Throwable=>channel.close();throw t}
  private val file=directory.resolve("routes.json")
  private val alphabet="23456789abcdefghjkmnpqrstuvwxyz"
  private val base=alphabet.length.toLong
  private val half=math.pow(base,4).toLong
  val capacity:Long=half*half
  private var state=try {
    if(Files.exists(file))Json.read(file).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    else {val key=new Array[Byte](32);new SecureRandom().nextBytes(key);val n=Json.obj().put("next",1L).put("key",java.util.Base64.getEncoder.encodeToString(key));n.putArray("routes");Json.write(file,n);n}
  }catch{case t:Throwable=>lock.release();channel.close();throw t}
  private val key=try {
  require(state.path("next").isIntegralNumber && state.path("next").asLong>0 && state.path("next").asLong<=capacity && state.path("routes").isArray,"invalid route registry")
  val decoded=java.util.Base64.getDecoder.decode(state.path("key").asText)
  require(decoded.length==32,"invalid route encoding key");decoded
  }catch{case t:Throwable=>lock.release();channel.close();throw t}
  private def round(r:Long,index:Int):Long={
    val mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"))
    val data=java.nio.ByteBuffer.allocate(12).putInt(index).putLong(r).array()
    (BigInt(1,mac.doFinal(data))%half).toLong
  }
  def encode(value:Long):String=synchronized {
    require(value>=0 && value<capacity,"dataset ID capacity exhausted")
    var l=value/half;var r=value%half
    (0 until 6).foreach { i=>val next=(l+round(r,i))%half;l=r;r=next }
    var n=l*half+r
    val text=new Array[Char](8)
    (7 to 0 by -1).foreach { i=>text(i)=alphabet((n%base).toInt);n/=base }
    "ds-"+text.take(4).mkString+"-"+text.drop(4).mkString
  }
  def decode(id:String):Long=synchronized {
    require(id.matches("ds-["+alphabet+"]{4}-["+alphabet+"]{4}"),"invalid dataset ID")
    val n=id.drop(3).replace("-","").foldLeft(0L)((n,c)=>n*base+alphabet.indexOf(c))
    var l=n/half;var r=n%half
    (5 to 0 by -1).foreach { i=>val previous=Math.floorMod(r-round(l,i),half);r=l;l=previous }
    l*half+r
  }
  private var failed=false
  private def commit(next:com.fasterxml.jackson.databind.node.ObjectNode):Unit={
    require(!failed,"dataset registry requires restart after persistence failure")
    try {
      Json.write(file,next)
      val data=FileChannel.open(file,StandardOpenOption.WRITE);try data.force(true)finally data.close()
      val parent=FileChannel.open(directory,StandardOpenOption.READ);try parent.force(true)finally parent.close()
      state=next
    }catch{case t:Throwable=>failed=true;throw t}
  }
  private def rows=state.path("routes").elements.asScala.toVector
  try {
    val loaded=rows
    require(loaded.map(_.path("id").asText).distinct.size==loaded.size,"duplicate dataset route IDs")
    require(loaded.map(r=>(r.path("member").asText,r.path("name").asText)).distinct.size==loaded.size,"duplicate dataset route targets")
    loaded.foreach(r=>require(decode(r.path("id").asText)<state.path("next").asLong,"dataset route counter moved backwards"))
  }catch{case t:Throwable=>lock.release();channel.close();throw t}
  def resolve(id:String):Option[JsonNode]=synchronized {require(!failed,"dataset registry unavailable");rows.find(_.path("id").asText==id).map(_.deepCopy[JsonNode]())}
  def assign(member:String,name:String):String=synchronized {
    require(!failed,"dataset registry unavailable")
    rows.find(r=>r.path("member").asText==member && r.path("name").asText==name).map(_.path("id").asText).getOrElse {
      require(!failed,"dataset registry unavailable");val next=state.path("next").asLong;require(next<capacity,"dataset ID capacity exhausted")
      val id=encode(next);val updated=state.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();updated.put("next",next+1)
      updated.withArray("routes").addObject().put("id",id).put("member",member).put("name",name)
      commit(updated);id
    }
  }
  def reconcile(member:String,names:Set[String]):Unit=synchronized {
    val keep=rows.filter(r=>r.path("member").asText!=member || r.path("name").asText.isEmpty || names(r.path("name").asText))
    if(keep.size!=rows.size){val updated=state.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();val a=updated.putArray("routes");keep.foreach(a.add);commit(updated)}
  }
  def retainMembers(ids:Set[String]):Unit=synchronized {
    val keep=rows.filter(r=>ids(r.path("member").asText))
    if(keep.size!=rows.size){val updated=state.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();val a=updated.putArray("routes");keep.foreach(a.add);commit(updated)}
  }
  def close():Unit={lock.release();channel.close()}
}
