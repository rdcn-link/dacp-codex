package dacp

import _root_.xstream.data._

import java.nio.file.{Files,Path}
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.node.ObjectNode

private[dacp] object AccessLogOutput {
  trait Output extends AutoCloseable {def write(bytes:Array[Byte]):Unit;def sync():Unit}
  def file(path:Path):Output=new Output {
    private val stream=new FileOutputStream(path.toFile)
    def write(bytes:Array[Byte]):Unit=stream.write(bytes)
    def sync():Unit=stream.getFD.sync()
    def close():Unit=stream.close()
  }
}

/** Completed requests enqueue bounded records; one writer owns disk IO and rotation. */
final class AccessLog private[dacp](directory:Path,maxBytes:Long,open:Path=>AccessLogOutput.Output,capacity:Int=1024) extends AutoCloseable {
  def this(directory:Path,maxBytes:Long)=this(directory,maxBytes,AccessLogOutput.file,1024)
  require(maxBytes>0,"access_log.max_bytes must be positive")
  require(capacity>0,"access log queue capacity must be positive")
  Files.createDirectories(directory)
  private val queue=new java.util.concurrent.ArrayBlockingQueue[Array[Byte]](capacity)
  private val failure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
  @volatile private var closing=false
  private var channel:AccessLogOutput.Output=null
  private var size=0L
  private var dirty=false
  private var pending=0
  private var lastSync=System.nanoTime()
  private val interval=java.util.concurrent.TimeUnit.SECONDS.toNanos(1)
  private def sync():Unit=if(dirty){channel.sync();dirty=false;pending=0;lastSync=System.nanoTime()}
  private def rotate():Unit={
    if(channel!=null){sync();channel.close();channel=null}
    val name="access-"+Instant.now().toEpochMilli+"-"+UUID.randomUUID().toString+".jsonl"
    channel=open(Files.createFile(directory.resolve(name)));size=0
  }
  // Fail startup immediately when the destination cannot be opened.
  rotate()
  private def recordFailure(t:Throwable):Unit=if(failure.compareAndSet(null,t))org.slf4j.LoggerFactory.getLogger(classOf[AccessLog]).error("access log writer failed",t)
  private def check():Unit=Option(failure.get()).foreach(t=>throw new IllegalStateException("access log writer failed",t))
  private val writer=new Thread(()=>{
    try {
      var done=false
      while(!done){
        val wait=if(dirty)math.max(1L,interval-(System.nanoTime()-lastSync))else interval
        val bytes=queue.poll(wait,java.util.concurrent.TimeUnit.NANOSECONDS)
        if(bytes!=null && bytes.nonEmpty){
          if(size>0 && size+bytes.length>maxBytes)rotate()
          dirty=true;channel.write(bytes);size+=bytes.length;pending+=1
        }
        if(dirty && (pending>=256 || System.nanoTime()-lastSync>=interval))sync()
        done=closing && queue.isEmpty
      }
      sync()
    }catch{case scala.util.control.NonFatal(t)=>recordFailure(t)}
    finally if(channel!=null)try channel.close()catch{case scala.util.control.NonFatal(t)=>recordFailure(t)}
  },"dacp-access-log-writer")
  writer.setDaemon(true);writer.start()
  def append(event:ObjectNode):Unit={
    val bytes=(Data.mapper.writeValueAsString(event)+"\n").getBytes(UTF_8)
    var interrupted=Thread.interrupted()
    try {
      var accepted=false
      while(!accepted){
        synchronized{check();require(!closing,"access log is closed");accepted=queue.offer(bytes)}
        // A full bounded queue applies backpressure. Cancellation must not silently drop audit records.
        if(!accepted)try Thread.sleep(10)catch{case _:InterruptedException=>interrupted=true}
      }
    }finally if(interrupted)Thread.currentThread().interrupt()
  }
  def begin(operation:String):AccessEvent=new AccessEvent(this,operation)
  def close():Unit={
    synchronized{if(!closing){closing=true;queue.offer(Array.emptyByteArray)}}
    var interrupted=Thread.interrupted()
    try {
      while(writer.isAlive)try writer.join()catch{case _:InterruptedException=>interrupted=true}
      check()
    }finally if(interrupted)Thread.currentThread().interrupt()
  }
}
final class AccessEvent(log:AccessLog,operation:String) {
  private val started=System.nanoTime()
  private val n=Data.mapper.createObjectNode().put("request_id",UUID.randomUUID().toString)
    .put("started_at",Instant.now().toString).put("operation",operation)
    .put("identity",scala.util.Try(dacp.union.Rpc.identity).getOrElse("unknown"))
  private var ended=false
  private var rows=0L
  private var bytes=0L
  def actor(subject:String):Unit=n.put("identity",subject)
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
