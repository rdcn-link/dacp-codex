package dacp

import java.util.concurrent.{ArrayBlockingQueue,TimeUnit,CancellationException}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter,ArrowStreamReader}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.memory.BufferAllocator
import com.fasterxml.jackson.databind.JsonNode

case class PortContract(representations: Set[String], formats: Set[String], schema: Option[Schema]) {
  def accept(h: JsonNode): Unit = {
    val repr=h.path("representation").asText
    require(representations(repr),"unsupported port representation")
    require(formats.isEmpty || formats(h.path("format").asText),"unsupported port format")
    if(repr=="records") {
      val actual=StreamProtocol.schema(h)
      schema.foreach(s=>require(s==actual,"operator port schema mismatch"))
    } else require(repr=="bytes","unsupported representation; scientific representations need explicit contracts")
  }
}
object OperatorPorts {
  def read(n: JsonNode): Map[String,PortContract] = {
    require(n.isObject,"operator must declare a port mapping")
    n.fields.asScala.map { e =>
      require(e.getKey.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"invalid operator port")
      val v=e.getValue
      require(v.isObject && v.fieldNames.asScala.toSet.subsetOf(Set("representations","formats","schema")),"invalid port settings")
      require(v.path("representations").isArray && v.path("formats").isArray,"port representations/formats must be lists")
      val repr=v.path("representations").elements.asScala.map(_.asText).toSet
      require(repr.nonEmpty && repr.subsetOf(Set("bytes","records")),"unsupported representation")
      e.getKey -> PortContract(repr,v.path("formats").elements.asScala.map(_.asText).toSet,
        Option(v.get("schema")).map(s=>Schema.fromJSON(s.toString)))
    }.toMap
  }
}
final class StreamCancel(check: () => Boolean = () => false, timeoutSeconds: Long = 1800) extends AutoCloseable {
  private val failure=new AtomicReference[Throwable]()
  private val resources=new java.util.concurrent.ConcurrentLinkedDeque[AutoCloseable]()
  private val deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(timeoutSeconds)
  def remainingSeconds:Long=math.max(1L,(deadline-System.nanoTime()+TimeUnit.SECONDS.toNanos(1)-1)/TimeUnit.SECONDS.toNanos(1))
  /** Inspect a parent scope without treating interruption of a child worker as parent cancellation. */
  private[dacp] def checkState():Unit={
    Option(failure.get()).foreach(throw _)
    if(check())fail(new CancellationException("stream cancelled"))
    if(System.nanoTime()>deadline)fail(new java.util.concurrent.TimeoutException("job timed out"))
    Option(failure.get()).foreach(throw _)
  }
  def active(): Unit = {
    Option(failure.get()).foreach(throw _)
    if(Thread.currentThread().isInterrupted)fail(new CancellationException("stream cancelled"))
    checkState()
  }
  def add[T <: AutoCloseable](r: T): T = {resources.push(r);if(failure.get()!=null) try r.close() catch{case _:Throwable=>()};r}
  def cause(t: Throwable): Throwable = Option(failure.get()).getOrElse(t)
  def fail(t: Throwable): Unit = if(failure.compareAndSet(null,t)) closeResources()
  private def closeResources(): Unit = resources.iterator.asScala.foreach(r=>try r.close() catch{case _:Throwable=>()})
  def close(): Unit = {fail(new CancellationException("stream closed"));closeResources()}
}
trait FrameSource extends AutoCloseable { def next(): Option[OperatorFrame] }
final class FrameQueue(cancel: StreamCancel) extends FrameSource {
  private val queue=new ArrayBlockingQueue[Option[OperatorFrame]](2)
  def put(f: OperatorFrame): Unit = offer(Some(f))
  def end(): Unit = offer(None)
  private def offer(v: Option[OperatorFrame]): Unit = {cancel.active();while(!queue.offer(v,100,TimeUnit.MILLISECONDS)) cancel.active()}
  def next(): Option[OperatorFrame] = {cancel.active();var v=queue.poll(100,TimeUnit.MILLISECONDS);while(v==null){cancel.active();v=queue.poll(100,TimeUnit.MILLISECONDS)};v}
  def close(): Unit = ()
}
object StreamProtocol {
  def header(f: OperatorFrame): JsonNode = Data.mapper.readTree(f.metadata)
  def frame(kind: String, port: String = "", data: Array[Byte] = Array.emptyByteArray): OperatorFrame =
    OperatorFrame(Data.mapper.createObjectNode().put("type",kind).put("port",port).toString.getBytes(java.nio.charset.StandardCharsets.UTF_8),data)
  def start(port: String,repr: String,format: String,s: Option[Schema]=None): OperatorFrame = {
    val h=Data.mapper.createObjectNode().put("type","start").put("port",port).put("representation",repr).put("format",format)
    s.foreach(x=>h.put("schema",java.util.Base64.getEncoder.encodeToString(x.serializeAsMessage())))
    OperatorFrame(Data.mapper.writeValueAsBytes(h))
  }
  def schema(h: JsonNode): Schema = org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema(new org.apache.arrow.vector.ipc.ReadChannel(java.nio.channels.Channels.newChannel(new java.io.ByteArrayInputStream(java.util.Base64.getDecoder.decode(h.path("schema").asText)))))
  def rename(f: OperatorFrame,port: String,task: String=""): OperatorFrame = {
    val h=header(f).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();h.put("port",port);if(task.nonEmpty) h.put("task",task)
    OperatorFrame(Data.mapper.writeValueAsBytes(h),f.data)
  }
  def batch(root: VectorSchemaRoot): Array[Byte] = {
    val out=new java.io.ByteArrayOutputStream();val w=new ArrowStreamWriter(root,null,out)
    try {w.start();w.writeBatch();w.end();out.toByteArray} finally w.close()
  }
  def send(listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener,root: VectorSchemaRoot,f: OperatorFrame,cancel: StreamCancel): Unit = {
    while(!listener.isReady){cancel.active();Thread.sleep(10)};cancel.active()
    root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,OperatorFrames.encode(f));root.setRowCount(1);listener.putNext()
  }
  def receive(root: VectorSchemaRoot): OperatorFrame = {
    require(root.getSchema==PluginSchemas.bytes && root.getRowCount==1,"invalid stream envelope")
    OperatorFrames.decode(root.getVector(0).asInstanceOf[VarBinaryVector].get(0))
  }
  def thread(cancel: StreamCancel)(f: => Unit): Thread = {
    val t=new Thread(()=>try f catch{case t:Throwable=>cancel.fail(t)},"dacp-stream")
    t.setDaemon(true);cancel.add(new AutoCloseable{def close():Unit=t.interrupt()});t.start();t
  }
}
