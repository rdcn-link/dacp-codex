package dacp.client

import java.net.URI
import java.nio.file.{Files,Path}
import java.util.Base64
import java.util.concurrent.TimeUnit
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ReadChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import dacp.protocol.Metadata
import scala.collection.mutable
import scala.util.Using

/** Payloads are owned byte arrays, valid after advancing or closing the stream. */
final case class OutputEvent(kind:String, port:String, task:Option[String], representation:Option[String],
  format:Option[String], schema:Option[Schema], data:Array[Byte], executionId:Option[String]=None) {
  /** Record payload contains one independent IPC stream. Caller closes this reader. */
  def records(allocator:org.apache.arrow.memory.BufferAllocator):ArrowStreamReader = {
    require(kind=="data" && representation.contains("records"),"event is not record data")
    new ArrowStreamReader(new java.io.ByteArrayInputStream(data),allocator)
  }
}

object Job {
  def infer(node:String,yaml:String,tls:TlsConfig=TlsConfig(),variables:com.fasterxml.jackson.databind.JsonNode=null):com.fasterxml.jackson.databind.JsonNode =
    inferAction(node,"dacp.infer_job",dacp.union.Json.obj().put("job_yaml",WorkflowVariables.resolve(yaml,variables)),tls)
  def inferOperator(node:String,operator:String,inputs:com.fasterxml.jackson.databind.JsonNode,
    params:com.fasterxml.jackson.databind.JsonNode=dacp.union.Json.obj(),tls:TlsConfig=TlsConfig()):com.fasterxml.jackson.databind.JsonNode={
    val n=dacp.union.Json.obj().put("operator",operator);n.set[com.fasterxml.jackson.databind.JsonNode]("inputs",inputs);n.set[com.fasterxml.jackson.databind.JsonNode]("params",params)
    inferAction(node,"dacp.infer",n,tls)
  }
  private def inferAction(node:String,action:String,body:com.fasterxml.jackson.databind.JsonNode,tls:TlsConfig):com.fasterxml.jackson.databind.JsonNode={
    import scala.jdk.CollectionConverters._
    val uri=new URI(node)
    require(uri.getScheme=="dacp" && uri.getHost!=null && uri.getUserInfo==null && uri.getQuery==null && uri.getFragment==null &&
      Set("", "/")(Option(uri.getPath).getOrElse("")),"expected DACP origin")
    val bytes=dacp.union.Json.mapper.writeValueAsBytes(body);require(bytes.length<=1024*1024,"inference request exceeds 1 MiB")
    val (client,allocator)=Source.connection(uri,tls)
    try {
      val results=client.doAction(new Action(action,bytes),CallOptions.timeout(120,TimeUnit.SECONDS)).asScala
      require(results.hasNext,"missing inference response");val raw=results.next().getBody
      require(raw.length<=1024*1024 && !results.hasNext,"invalid inference response")
      dacp.union.Json.mapper.readTree(raw)
    }finally Resources.closeAll(Seq(client,allocator))
  }

  def run(node:String, yaml:String, tls:TlsConfig=TlsConfig(), timeoutSeconds:Long=3600, executionPolicy:com.fasterxml.jackson.databind.JsonNode=JobStream.mapper.createObjectNode(),
    onRetry:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]=None, replayFrom:Option[String]=None, variables:com.fasterxml.jackson.databind.JsonNode=null):JobStream = {
    require(replayFrom.isEmpty || variables==null,"replay uses the original resolved variables")
    val resolved=WorkflowVariables.resolve(yaml,variables)
    val u=new URI(node)
    require(u.getScheme=="dacp" && u.getHost!=null && u.getUserInfo==null && u.getQuery==null && u.getFragment==null &&
      Set("", "/")(Option(u.getPath).getOrElse("")) && (u.getPort == -1 || u.getPort>0 && u.getPort<=65535),"expected node origin dacp://host[:port]")
    require(timeoutSeconds>0,"timeoutSeconds must be positive")
    require(executionPolicy.isObject,"executionPolicy must be an object")
    val builder=Metadata.OperatorRequest.newBuilder().setJobYaml(resolved).setRetryAware(onRetry.nonEmpty)

    if(replayFrom.isEmpty || executionPolicy.size()>0)builder.setExecutionPolicyYaml(com.google.protobuf.ByteString.copyFromUtf8(dacp.union.YamlDocument.dump(executionPolicy)))
    replayFrom.foreach(builder.setReplayFrom)
    val request=builder.build().toByteArray
    require(request.length<=1024*1024,"control request exceeds 1 MiB")
    val (client,allocator)=Source.connection(u,tls)
    try {
      open(client,allocator,request,timeoutSeconds,onRetry)
    } catch {case t:Throwable=>try client.close()catch{case _:Throwable=>()};allocator.close();throw DacpErrors.translate(t)}
  }
  def replay(node:String,executionId:String,tls:TlsConfig=TlsConfig(),timeoutSeconds:Long=3600,
    onRetry:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]=None):JobStream =
    run(node,"",tls,timeoutSeconds,onRetry=onRetry,replayFrom=Some(executionId))
  private[client] def open(client:FlightClient, allocator:RootAllocator,request:Array[Byte],timeout:Long,onRetry:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]=None):JobStream = {
    val exchange=client.doExchange(FlightDescriptor.command(request),CallOptions.timeout(timeout,TimeUnit.SECONDS))
    val root=VectorSchemaRoot.create(JobStream.envelope,allocator)
    try {exchange.getWriter.start(root);exchange.getWriter.completed();new JobStream(client,allocator,exchange,root,onRetry)}
    catch {case t:Throwable=>try exchange.close()finally root.close();throw t}
  }
}

object JobStream {
  private[client] val envelope=Schema.fromJSON("""{"fields":[{"name":"data","nullable":true,"type":{"name":"binary"},"children":[]}]}""")
  private[client] val mapper=new com.fasterxml.jackson.databind.ObjectMapper()
}
/** Single-consumer, closable iterator. Use scala.util.Using.resource, including on early exit. */
final class JobStream private[client](client:FlightClient,allocator:RootAllocator,
  exchange:FlightClient.ExchangeReaderWriter,root:VectorSchemaRoot,onRetry:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]=None) extends Iterator[OutputEvent] with AutoCloseable {
  private case class Port(representation:String, format:String, schema:Option[Schema],var ended:Boolean=false)
  private val ports=mutable.Map.empty[String,Port]
  var executionId:Option[String]=None
  private var pending:Option[OutputEvent]=None
  private var complete=false
  private var closed=false
  override def hasNext:Boolean = {
    if(pending.nonEmpty)return true
    if(closed)return false
    try {
      if(!exchange.getReader.next()) {
        require(complete,"job result stream truncated");close();return false
      }
      require(!complete,"event after completion")
      val batch=exchange.getReader.getRoot
      require(batch.getSchema==JobStream.envelope && batch.getRowCount==1,"invalid job stream envelope")
      val raw=batch.getVector(0).asInstanceOf[VarBinaryVector].get(0)
      require(raw!=null && raw.length>=8,"invalid frame")
      val in=new java.io.DataInputStream(new java.io.ByteArrayInputStream(raw))
      val h=in.readInt();val n=in.readInt()
      require(h>0 && h<=65536 && n>=0 && n<=8*1024*1024 && raw.length.toLong==8L+h+n,"invalid frame lengths")
      val header=new Array[Byte](h);in.readFully(header);val data=new Array[Byte](n);in.readFully(data)
      val meta=JobStream.mapper.readTree(header);val kind=meta.path("type").asText;val port=meta.path("port").asText
      executionId=Option(meta.get("execution_id")).map(_.asText).orElse(executionId)
      kind match {
        case "execution"=>require(executionId.nonEmpty && ports.isEmpty,"invalid execution metadata")
        case "attempt_failed"=>
          require(onRetry.nonEmpty,"retry notification requires onRetry callback")
          onRetry.get(meta);ports.clear();return hasNext
        case "start"=>
          val repr=meta.path("representation").asText
          require(port.nonEmpty && !ports.contains(port) && Set("bytes","records")(repr),"invalid output start")
          val schema=if(repr=="records")Some(MessageSerializer.deserializeSchema(new ReadChannel(java.nio.channels.Channels.newChannel(
            new java.io.ByteArrayInputStream(Base64.getDecoder.decode(meta.path("schema").asText))))))else None
          ports(port)=Port(repr,meta.path("format").asText,schema)
        case "data"|"end"=>
          require(ports.contains(port) && !ports(port).ended,"unexpected output port event")
          val state=ports(port)
          if(kind=="end")state.ended=true
          else state.schema.foreach{s=>
            val reader=new ArrowStreamReader(new java.io.ByteArrayInputStream(data),allocator)
            try {require(reader.getVectorSchemaRoot.getSchema==s,"output schema changed");require(reader.loadNextBatch(),"missing record batch");require(!reader.loadNextBatch(),"one record batch per frame required")}finally reader.close()
          }
        case "complete"=>require(ports.values.forall(_.ended),"incomplete output port");complete=true
        case _=>throw new IllegalArgumentException("unknown output event")
      }
      val state=ports.get(port)
      pending=Some(OutputEvent(kind,port,Option(meta.get("task")).map(_.asText),state.map(_.representation),state.map(_.format),state.flatMap(_.schema),data,executionId))
      true
    } catch {case t:Throwable=>try close()catch{case cleanup:Throwable=>t.addSuppressed(cleanup)};throw DacpErrors.translate(t)}
  }
  override def next():OutputEvent={if(!hasNext)throw new NoSuchElementException("job stream exhausted");val e=pending.get;pending=None;e}
  override def close():Unit=if(!closed){
    closed=true;pending=None
    try exchange.getReader.cancel("job stream closed",null)catch{case _:Throwable=>()}
    try exchange.close()catch{case _:Throwable=>()}
    try root.close()finally {try client.close()finally allocator.close()}
  }
}
