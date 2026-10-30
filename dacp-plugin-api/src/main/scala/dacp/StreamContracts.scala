package dacp


import java.util.concurrent.{TimeUnit,CancellationException}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.vector.types.pojo.Schema

case class PortContract(representations: Set[String], formats: Set[String], schema: Option[Schema]) {
  def accept(h: JsonNode): Unit = {
    val repr=h.path("representation").asText
    require(representations(repr),"unsupported port representation")
    require(formats.isEmpty || formats(h.path("format").asText),"unsupported port format")
    if(repr=="records") {
      val actual=org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(java.nio.ByteBuffer.wrap(java.util.Base64.getDecoder.decode(h.path("schema").asText)))
      schema.foreach(s=>require(s==actual,"operator port schema mismatch"))
    } else if(repr!="bytes")require(h.path("structure").isObject && h.path("structure").path("view").asText==repr,"scientific stream needs a structure descriptor")
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
      require(repr.nonEmpty && repr.forall(_.matches("[a-z][a-z0-9_-]*")),"unsupported representation")
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
