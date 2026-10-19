package dacp

import dacp.union.Json
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.{Files,Path,StandardOpenOption,StandardCopyOption}
import java.nio.channels.FileChannel
import scala.jdk.CollectionConverters._

/** Persistent metadata, independent of result retention. Each record is atomically replaced. */
final class ExecutionHistory(val directory:Path,recoverInterrupted:Boolean=true) extends AutoCloseable {
  Files.createDirectories(directory)
  private val channel=if(recoverInterrupted)Some(FileChannel.open(directory.resolve(".writer.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE))else None
  private val ownerLock=channel.map { c=>
    try Option(c.tryLock()).getOrElse(throw new IllegalStateException("execution history directory is already in use"))
    catch{case t:Throwable=>c.close();throw t}
  }
  private val terminal=Set("succeeded","failed","cancelled","expired","interrupted","rejected")
  private def file(id:String)=directory.resolve(Json.id(id)+".json")
  private def paths:Vector[Path]={val stream=Files.list(directory);try stream.iterator.asScala.filter(_.getFileName.toString.matches("[A-Za-z0-9_-]+\\.json")).toVector finally stream.close()}
  private def write(n:JsonNode):Unit={
    // Cancelling an execution interrupts its thread. Its final audit write must
    // still complete, then restore that interruption for the caller.
    val interrupted=Thread.interrupted()
    try writeAtomic(n)finally if(interrupted)Thread.currentThread().interrupt()
  }
  private def writeAtomic(n:JsonNode):Unit={
    val path=file(Json.text(n,"record_id"));val temp=Files.createTempFile(directory,".record-",".tmp")
    try {
      val out=FileChannel.open(temp,StandardOpenOption.WRITE)
      try {val buffer=java.nio.ByteBuffer.wrap(Json.mapper.writeValueAsBytes(n));while(buffer.hasRemaining)out.write(buffer);out.force(true)}finally out.close()
      Files.move(temp,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)
      val parent=FileChannel.open(directory,StandardOpenOption.READ);try parent.force(true)finally parent.close()
    }finally Files.deleteIfExists(temp)
  }
  if(recoverInterrupted) {
    try paths.foreach { p=>
      val n=Json.read(p).asInstanceOf[ObjectNode]
      if(!terminal(n.path("state").asText)) {
        val now=Json.now;val last=n.path("updated_at_ms").asLong;n.put("state","interrupted").put("finished_at_ms",last).put("interruption_detected_at_ms",now).put("updated_at_ms",now)
          .put("elapsed_ms",math.max(0,last-n.path("created_at_ms").asLong)).put("message","server stopped before recording completion; execution was not resumed")
        if(n.path("tasks").isArray)n.path("tasks").elements.asScala.foreach { t=>
          if(!terminal(t.path("state").asText)){t.asInstanceOf[ObjectNode].put("last_observed_state",t.path("state").asText).put("state","interrupted")}
        }
        val events=n.withArray("events");events.addObject().put("sequence",events.size()+1).put("state","interrupted").put("at_ms",now);write(n)
      }
    } catch {case t:Throwable=>close();throw t}
  }
  def begin(kind:String,id:String=Json.uuid(),metadata:JsonNode=Json.obj(),state:String="accepted"):Record=synchronized {
    require(recoverInterrupted,"history opened read-only");require(!Files.exists(file(id)),"execution record already exists")
    val n=metadata.deepCopy[ObjectNode]();val now=Json.now
    n.put("record_id",Json.id(id)).put("kind",kind).put("state",state).put("created_at_ms",now).put("updated_at_ms",now).put("finished_at_ms",0).put("elapsed_ms",0)
    n.putArray("events").addObject().put("sequence",1).put("state",state).put("at_ms",now)
    write(n);new Record(n)
  }
  final class Record private[ExecutionHistory](private var n:ObjectNode) {
    def id:String=n.path("record_id").asText
    def update(state:String,details:JsonNode=Json.obj(),message:Option[String]=None):Unit=synchronized {
      if(terminal(n.path("state").asText) && n.path("state").asText!=state)return
      val next=n.deepCopy[ObjectNode]();val now=Json.now
      details.fields.asScala.filterNot(e=>Set("record_id","kind","created_at_ms","events","updated_at_ms","finished_at_ms","elapsed_ms","state")(e.getKey)).foreach(e=>next.set[JsonNode](e.getKey,e.getValue))
      if(state!=n.path("state").asText)next.withArray("events").addObject().put("sequence",next.path("events").size()+1).put("state",state).put("at_ms",now)
      message.foreach(m=>next.put("message",m.take(2048)))
      val end=if(terminal(state)){if(n.path("finished_at_ms").asLong>0)n.path("finished_at_ms").asLong else now}else 0L
      if(state=="running" && next.path("started_at_ms").asLong==0)next.put("started_at_ms",now)
      next.put("state",state).put("updated_at_ms",now).put("finished_at_ms",end).put("elapsed_ms",math.max(0,(if(end>0)end else now)-next.path("created_at_ms").asLong))
      write(next);n=next
    }
  }
  def get(id:String):Option[JsonNode]={val p=file(id);if(Files.exists(p))Some(Json.read(p))else None}
  def list(offset:Int=0,limit:Int=100,owner:Option[String]=None):Vector[JsonNode]={
    require(offset>=0 && limit>0 && limit<=1000,"invalid history page")
    paths.iterator.map(Json.read).filter(n=>owner.forall(_==n.path("owner").asText)).toVector.sortBy(n=>(-n.path("created_at_ms").asLong,n.path("record_id").asText)).slice(offset,offset+limit)
  }
  def close():Unit={ownerLock.foreach(l=>if(l.isValid)l.release());channel.foreach(c=>if(c.isOpen)c.close())}
}
object ExecutionHistoryAdmin {
  def main(args:Array[String]):Unit={
    require(args.length>=2,"history list DIRECTORY [OFFSET LIMIT] | history get DIRECTORY RECORD_ID")
    val history=new ExecutionHistory(Path.of(args(1)),recoverInterrupted=false)
    try args(0) match {
      case "list"=>require(args.length==2 || args.length==4);println(Json.mapper.valueToTree[JsonNode](history.list(if(args.length==4)args(2).toInt else 0,if(args.length==4)args(3).toInt else 100).asJava).toPrettyString)
      case "get"=>require(args.length==3);println(history.get(args(2)).getOrElse(throw new IllegalArgumentException("unknown execution record")).toPrettyString)
      case _=>throw new IllegalArgumentException("unknown history command")
    } finally history.close()
  }
}
