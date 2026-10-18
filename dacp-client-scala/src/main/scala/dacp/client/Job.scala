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
  format:Option[String], schema:Option[Schema], data:Array[Byte]) {
  /** Record payload contains one independent IPC stream. Caller closes this reader. */
  def records(allocator:org.apache.arrow.memory.BufferAllocator):ArrowStreamReader = {
    require(kind=="data" && representation.contains("records"),"event is not record data")
    new ArrowStreamReader(new java.io.ByteArrayInputStream(data),allocator)
  }
}

object Job {
  def run(node:String, yaml:String, tls:TlsConfig=TlsConfig(), timeoutSeconds:Long=3600):JobStream = {
    val u=new URI(node)
    require(u.getScheme=="dacp" && u.getHost!=null && u.getUserInfo==null && u.getQuery==null && u.getFragment==null &&
      Set("", "/")(Option(u.getPath).getOrElse("")) && (u.getPort == -1 || u.getPort>0 && u.getPort<=65535),"expected node origin dacp://host[:port]")
    require(timeoutSeconds>0,"timeoutSeconds must be positive")
    val request=Metadata.OperatorRequest.newBuilder().setJobYaml(yaml).build().toByteArray
    require(request.length<=1024*1024,"control request exceeds 1 MiB")
    def credential(p:Option[Path],env:String)=p.orElse(sys.env.get(env).map(Path.of(_)))
    val ca=credential(tls.ca,"DACP_CA");val cert=credential(tls.cert,"DACP_CERT");val key=credential(tls.key,"DACP_KEY")
    require(cert.isDefined==key.isDefined,"client certificate and key must be provided together")
    val allocator=new RootAllocator()
    var client:FlightClient=null
    try {
      client=Using.Manager {use=>
        val builder=FlightClient.builder(allocator,Location.forGrpcTls(u.getHost,if(u.getPort<0)8815 else u.getPort))
        ca.foreach(p=>builder.trustedCertificates(use(Files.newInputStream(p))))
        cert.foreach(p=>builder.clientCertificate(use(Files.newInputStream(p)),use(Files.newInputStream(key.get))))
        builder.build()
      }.get
      open(client,allocator,request,timeoutSeconds)
    } catch {case t:Throwable=>if(client!=null)try client.close()catch{case _:Throwable=>()};allocator.close();throw DacpErrors.translate(t)}
  }
  private[client] def open(client:FlightClient, allocator:RootAllocator,request:Array[Byte],timeout:Long):JobStream = {
    val exchange=client.doExchange(FlightDescriptor.command(request),CallOptions.timeout(timeout,TimeUnit.SECONDS))
    val root=VectorSchemaRoot.create(JobStream.envelope,allocator)
    try {exchange.getWriter.start(root);exchange.getWriter.completed();new JobStream(client,allocator,exchange,root)}
    catch {case t:Throwable=>try exchange.close()finally root.close();throw t}
  }
}

object JobStream {
  private[client] val envelope=Schema.fromJSON("""{"fields":[{"name":"data","nullable":true,"type":{"name":"binary"},"children":[]}]}""")
  private val mapper=new com.fasterxml.jackson.databind.ObjectMapper()
}
/** Single-consumer, closable iterator. Use scala.util.Using.resource, including on early exit. */
final class JobStream private[client](client:FlightClient,allocator:RootAllocator,
  exchange:FlightClient.ExchangeReaderWriter,root:VectorSchemaRoot) extends Iterator[OutputEvent] with AutoCloseable {
  private case class Port(representation:String, format:String, schema:Option[Schema],var ended:Boolean=false)
  private val ports=mutable.Map.empty[String,Port]
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
      kind match {
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
      pending=Some(OutputEvent(kind,port,Option(meta.get("task")).map(_.asText),state.map(_.representation),state.map(_.format),state.flatMap(_.schema),data))
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
