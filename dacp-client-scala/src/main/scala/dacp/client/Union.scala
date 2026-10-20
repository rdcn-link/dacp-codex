package dacp.client

import dacp.union.{BinarySource,Json,Remote,Tls}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Path
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ReadChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Results and events use independent Flight streams over mTLS. */
final class Union(url:String,cert:Path,key:Path,ca:Path,timeoutSeconds:Long=1800) extends AutoCloseable {
  private[client] val origin=Json.origin(url)
  private[client] val remote=new Remote(new Tls(cert,key,ca))
  private[client] val timeout=timeoutSeconds
  require(timeoutSeconds>0)
  def members():JsonNode=remote.action(origin,"members")
  def submit(yaml:String):UnionExecution={
    val n=remote.action(origin,"submit",Json.obj().put("job_yaml",yaml))
    new UnionExecution(this,Json.id(Json.text(n,"job_id")))
  }
  def execution(id:String):UnionExecution=new UnionExecution(this,Json.id(id))
  def close():Unit=remote.close()
}
final class UnionExecution private[client](union:Union,val id:String) extends AutoCloseable {
  private def body=Json.obj().put("job_id",id)
  @volatile private var complete=false
  private var closed=false
  private val streams=mutable.ArrayBuffer.empty[AutoCloseable]
  def status():JsonNode=union.remote.action(union.origin,"job_status",body)
  def cancel():JsonNode=union.remote.action(union.origin,"cancel_job",body)
  def events(after:Long=0):UnionEvents=synchronized {
    require(!closed && after>=0)
    val r=union.remote.stream(union.origin,"events",body.put("after",after),timeout=union.timeout)
    val stream=new UnionEvents(r)
    streams+=stream;stream
  }
  def results():UnionResults=synchronized {
    require(!closed)
    val r=union.remote.stream(union.origin,"results",body,timeout=union.timeout)
    val stream=new UnionResults(r,()=>complete=true,()=>{if(!complete)try cancel()catch{case scala.util.control.NonFatal(_)=>()}})
    streams+=stream;stream
  }
  def close():Unit=synchronized {if(!closed){closed=true;streams.foreach(_.close());if(!complete)try cancel()catch{case scala.util.control.NonFatal(_)=>()}}}
}
/** Closing this subscription does not cancel the job. */
final class UnionEvents private[client](reader:BinarySource) extends Iterator[JsonNode] with AutoCloseable {
  private var pending:Option[JsonNode]=None
  private var closed=false
  def hasNext:Boolean={
    if(pending.nonEmpty)return true
    if(closed)return false
    try {
      reader.next() match {
        case None=>close();false
        case Some(bytes)=>require(bytes.length<=1024*1024,"event too large");pending=Some(Json.mapper.readTree(bytes));true
      }
    }catch{case scala.util.control.NonFatal(t)=>close();throw t}
  }
  def next():JsonNode={if(!hasNext)throw new NoSuchElementException;val n=pending.get;pending=None;n}
  def close():Unit=if(!closed){closed=true;reader.close()}
}
final class UnionResults private[client](in:BinarySource,success:()=>Unit,abort:()=>Unit) extends Iterator[OutputEvent] with AutoCloseable {
  private case class Port(repr:String,format:String,schema:Option[Schema],var ended:Boolean=false)
  private val ports=mutable.Map.empty[String,Port]
  private val allocator=new RootAllocator()
  private var pending:Option[OutputEvent]=None
  private var complete=false
  private var closed=false
  def hasNext:Boolean={
    if(pending.nonEmpty)return true
    if(closed)return false
    if(complete){close();return false}
    try {
      val frame=dacp.OperatorFrames.decode(in.next().getOrElse(throw new IllegalArgumentException("truncated Union result")))
      val bytes=frame.data;val m=Json.mapper.readTree(frame.metadata);val kind=m.path("type").asText;val port=m.path("port").asText
      kind match {
        case "error"=>throw new IllegalStateException(m.path("message").asText)
        case "complete"=>require(ports.nonEmpty && ports.values.forall(_.ended),"incomplete results");require(in.next().isEmpty,"events after completion");complete=true;success()
        case "start"=>
          val repr=m.path("representation").asText
          require(port.nonEmpty && !ports.contains(port) && Set("bytes","records")(repr),"invalid output start")
          val schema=if(repr=="records")Some(MessageSerializer.deserializeSchema(new ReadChannel(java.nio.channels.Channels.newChannel(new java.io.ByteArrayInputStream(java.util.Base64.getDecoder.decode(m.path("schema").asText))))))else None
          ports(port)=Port(repr,m.path("format").asText,schema)
        case "data"|"end"=>
          require(ports.contains(port) && !ports(port).ended,"invalid output event")
          if(kind=="end")ports(port).ended=true
          else ports(port).schema.foreach { s=>
            val reader=new ArrowStreamReader(new java.io.ByteArrayInputStream(bytes),allocator)
            try {require(reader.getVectorSchemaRoot.getSchema==s,"output schema changed");require(reader.loadNextBatch(),"missing batch");require(!reader.loadNextBatch(),"one batch per frame required")}finally reader.close()
          }
        case _=>throw new IllegalArgumentException("unknown result event")
      }
      val p=ports.get(port)
      pending=Some(OutputEvent(kind,port,Option(m.get("task")).map(_.asText),p.map(_.repr),p.map(_.format),p.flatMap(_.schema),bytes));true
    }catch{case scala.util.control.NonFatal(t)=>close();throw t}
  }
  def next():OutputEvent={if(!hasNext)throw new NoSuchElementException;val n=pending.get;pending=None;n}
  def close():Unit=if(!closed){closed=true;try in.close()finally {allocator.close();if(!complete)abort()}}
}
