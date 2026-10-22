package dacp

import java.nio.file.{Files,Path}
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.node.ObjectNode

/** One JSON object per completed request. Files are never truncated or reused. */
final class AccessLog(directory:Path,maxBytes:Long) extends AutoCloseable {
  require(maxBytes>0,"access_log.max_bytes must be positive")
  Files.createDirectories(directory)
  private var channel:FileOutputStream=null
  private var size=0L
  private var closed=false
  private def rotate():Unit={
    if(channel!=null)channel.close()
    val name="access-"+Instant.now().toEpochMilli+"-"+UUID.randomUUID().toString+".jsonl"
    val path=Files.createFile(directory.resolve(name))
    channel=new FileOutputStream(path.toFile)
    size=0
  }
  def append(event:ObjectNode):Unit=synchronized {
    require(!closed,"access log is closed")
    val bytes=(Data.mapper.writeValueAsString(event)+"\n").getBytes(UTF_8)
    if(channel==null || (size>0 && size+bytes.length>maxBytes))rotate()
    val interrupted=Thread.interrupted()
    try {
      // FileOutputStream does not close the shared file descriptor on thread interruption.
      // A cancelled RPC must not poison logging for every subsequent request.
      channel.write(bytes)
      channel.getFD.sync()
      size+=bytes.length
    }finally if(interrupted)Thread.currentThread().interrupt()
  }
  def begin(operation:String):AccessEvent=new AccessEvent(this,operation)
  def close():Unit=synchronized {if(!closed){closed=true;if(channel!=null)channel.close()}}
}
final class AccessEvent(log:AccessLog,operation:String) {
  private val started=System.nanoTime()
  private val n=Data.mapper.createObjectNode().put("request_id",UUID.randomUUID().toString)
    .put("started_at",Instant.now().toString).put("operation",operation)
    .put("identity",scala.util.Try(dacp.union.Rpc.identity).getOrElse("unknown"))
  private var ended=false
  private var rows=0L
  private var bytes=0L
  def add(count:Long,payload:Long):Unit=synchronized {rows+=count;bytes+=payload}
  def resource(dataset:String,item:String="",view:String=""):Unit={n.put("dataset",dataset);n.put("item",item);n.put("view",view)}
  def finish(status:String):Unit=synchronized {if(!ended){ended=true;n.put("rows",rows).put("bytes",bytes).put("status",status).put("finished_at",Instant.now().toString).put("elapsed_ms",(System.nanoTime()-started)/1000000);log.append(n)}}
}

object AccessLogging {
  def status(t:Throwable):String=t match {
    case _:java.util.concurrent.CancellationException=>"cancelled"
    case e:org.apache.arrow.flight.FlightRuntimeException=>e.status().code() match {
      case org.apache.arrow.flight.FlightStatusCode.CANCELLED=>"cancelled"
      case org.apache.arrow.flight.FlightStatusCode.UNAUTHORIZED | org.apache.arrow.flight.FlightStatusCode.UNAUTHENTICATED=>"denied"
      case _=>"failed"
    }
    case _ if Option(t.getMessage).exists(_.contains("FORBIDDEN"))=>"denied"
    case _=>"failed"
  }
  def stream(audit:AccessEvent,out:org.apache.arrow.flight.FlightProducer.ServerStreamListener):org.apache.arrow.flight.FlightProducer.ServerStreamListener=
    new org.apache.arrow.flight.FlightProducer.ServerStreamListener {
      private var root:org.apache.arrow.vector.VectorSchemaRoot=null
      def isReady:Boolean=out.isReady
      def isCancelled:Boolean=out.isCancelled
      override def setOnReadyHandler(r:Runnable):Unit=out.setOnReadyHandler(r)
      def setOnCancelHandler(r:Runnable):Unit=out.setOnCancelHandler(()=>{try r.run()finally audit.finish("cancelled")})
      def start(r:org.apache.arrow.vector.VectorSchemaRoot,d:org.apache.arrow.vector.dictionary.DictionaryProvider,o:org.apache.arrow.vector.ipc.message.IpcOption):Unit={root=r;out.start(r,d,o)}
      private def count():Unit=if(root!=null) {
        val bytes=root.getFieldVectors.asScala.foldLeft(0L){(sum,v)=>sum+(v match {
          case binary:org.apache.arrow.vector.VarBinaryVector=>(0 until root.getRowCount).foldLeft(0L)((n,i)=>n+(if(binary.isNull(i))0 else binary.getValueLength(i)))
          case _=>v.getBufferSize.toLong
        })}
        audit.add(root.getRowCount,bytes)
      }
      def putNext():Unit={out.putNext();count()}
      def putNext(metadata:org.apache.arrow.memory.ArrowBuf):Unit={out.putNext(metadata);count()}
      def putMetadata(metadata:org.apache.arrow.memory.ArrowBuf):Unit=out.putMetadata(metadata)
      def error(t:Throwable):Unit={audit.finish(status(t));out.error(t)}
      def completed():Unit={audit.finish(if(out.isCancelled)"cancelled" else "completed");out.completed()}
      override def setUseZeroCopy(value:Boolean):Unit=out.setUseZeroCopy(value)
    }
  def results[A](audit:AccessEvent,out:org.apache.arrow.flight.FlightProducer.StreamListener[A]):org.apache.arrow.flight.FlightProducer.StreamListener[A]=
    new org.apache.arrow.flight.FlightProducer.StreamListener[A] {
      def onNext(value:A):Unit=out.onNext(value)
      def onCompleted():Unit={audit.finish("completed");out.onCompleted()}
      def onError(t:Throwable):Unit={audit.finish(status(t));out.onError(t)}
    }
}
