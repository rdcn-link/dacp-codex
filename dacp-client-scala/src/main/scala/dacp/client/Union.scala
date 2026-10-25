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

/** Results and events use independent Flight streams over TLS with a user token. */
final class Union(url:String,tls:TlsConfig=TlsConfig(),timeoutSeconds:Long=1800) extends AutoCloseable {
  private[client] val origin=Json.origin(url)
  private[client] val remote=new Remote(None,credential=_=>tls.token.orElse(tls.tokenFile.orElse(sys.env.get("DACP_TOKEN_FILE").map(Path.of(_))).map(p=>java.nio.file.Files.readString(p).trim)),
    ca=tls.ca.orElse(sys.env.get("DACP_CA").map(Path.of(_))))
  private[client] val timeout=timeoutSeconds
  require(timeoutSeconds>0)
  def infer(yaml:String,variables:JsonNode=null):JsonNode={
    val request=Json.obj().put("job_yaml",WorkflowVariables.resolve(yaml,variables))
    val results=remote.dataClient(origin,None).doAction(new org.apache.arrow.flight.Action("dacp.infer_job",Json.mapper.writeValueAsBytes(request)),
      org.apache.arrow.flight.CallOptions.timeout(timeout,java.util.concurrent.TimeUnit.SECONDS)).asScala
    require(results.hasNext,"missing inference response");val raw=results.next().getBody
    require(raw.length<=1024*1024 && !results.hasNext,"invalid inference response");Json.mapper.readTree(raw)
  }
  def submit(yaml:String,executionPolicy:JsonNode=Json.obj(),variables:JsonNode=null):UnionExecution={
    val body=Json.obj().put("job_yaml",WorkflowVariables.resolve(yaml,variables));body.set[JsonNode]("execution_policy",executionPolicy)
    val n=remote.action(origin,"submit",body)
    new UnionExecution(this,Json.id(Json.text(n,"job_id")))
  }
  def replay(id:String,tasks:Option[Seq[String]]=None):UnionExecution={
    val body=Json.obj().put("job_id",Json.id(id));tasks.foreach(ts=>{val a=body.putArray("tasks");ts.foreach(a.add)})
    val n=remote.action(origin,"replay_job",body);new UnionExecution(this,Json.id(Json.text(n,"job_id")))
  }
  def execution(id:String):UnionExecution=new UnionExecution(this,Json.id(id))
  def close():Unit=remote.close()
}
final class UnionExecution private[client](union:Union,val id:String) extends AutoCloseable {
  private def body=Json.obj().put("job_id",id)
  @volatile private var complete=false
  private var closed=false
  private val streams=mutable.ArrayBuffer.empty[AutoCloseable]
  def replay(tasks:Option[Seq[String]]=None):UnionExecution=union.replay(id,tasks)
  def status():JsonNode=union.remote.action(union.origin,"job_status",body)
  def cancel():JsonNode=union.remote.action(union.origin,"cancel_job",body)
  def events(after:Long=0):UnionEvents=synchronized {
    require(!closed && after>=0)
    val r=union.remote.stream(union.origin,"events",body.put("after",after),timeout=union.timeout)
    val stream=new UnionEvents(r)
    streams+=stream;stream
  }
  def results(onRetry:Option[JsonNode=>Unit]=None):UnionResults=synchronized {
    require(!closed)
    val request=body;if(onRetry.nonEmpty)request.put("retry_aware",true)
    val r=union.remote.stream(union.origin,"results",request,timeout=union.timeout)
    val stream=new UnionResults(r,()=>complete=true,()=>{if(!complete)try cancel()catch{case scala.util.control.NonFatal(_)=>()}},onRetry)
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
final class UnionResults private[client](in:BinarySource,success:()=>Unit,abort:()=>Unit,onRetry:Option[JsonNode=>Unit]=None) extends Iterator[OutputEvent] with AutoCloseable {
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
        case "attempt_failed"=>
          require(onRetry.nonEmpty,"retry requires an onRetry callback");onRetry.get(m);ports.clear();return hasNext
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
