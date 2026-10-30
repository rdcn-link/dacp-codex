package xstream.data

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.pojo.Schema

/** Borrowed blocks are valid only until accept returns. Copy explicitly to retain them. */
final case class ByteChunk(bytes: Array[Byte], offset: Int, length: Int) {
  require(offset >= 0 && length >= 0 && offset <= bytes.length - length, "invalid byte slice")
}
final case class RecordBatch(root: VectorSchemaRoot, dictionaries: DictionaryProvider = null)
trait DataDescriptor[K]
final case class BytesDescriptor(mediaType: String = "application/octet-stream") extends DataDescriptor[ByteChunk]
final case class RecordsDescriptor(schema: Schema, dictionaries: DictionaryProvider = null) extends DataDescriptor[RecordBatch]
trait DataConsumer[K] {
  /** Exactly once, before any chunks, including an empty stream. */
  def start(descriptor: DataDescriptor[K]): Unit
  def accept(chunk: K): Unit
}
final case class StreamContext(allocator: BufferAllocator, check: () => Unit = () => ())
/** One consumption, synchronous backpressure, caller-owned allocator. Failure propagates to caller. */
trait DataStream[K] extends AutoCloseable {
  def consume(consumer: DataConsumer[K]): Unit
}
object DataStream {
  def apply[K](read: DataConsumer[K] => Unit): DataStream[K] = scoped((out,_)=>read(out))
  private[data] def scoped[K](read:(DataConsumer[K],()=>Unit)=>Unit):DataStream[K]=new DataStream[K] {
    private val used = new AtomicBoolean(false)
    private val closed = new AtomicBoolean(false)
    def consume(out: DataConsumer[K]): Unit = {
      require(!closed.get() && used.compareAndSet(false,true), "stream is closed or already consumed")
      var started = false
      val guarded = new DataConsumer[K] {
        private def active(): Unit = if(closed.get())throw new java.util.concurrent.CancellationException("stream closed")
        def start(d: DataDescriptor[K]): Unit = { active(); require(!started && d!=null,"invalid stream descriptor"); started=true; out.start(d) }
        def accept(k: K): Unit = { active(); require(started,"stream has no descriptor"); out.accept(k) }
      }
      def active():Unit=if(closed.get())throw new java.util.concurrent.CancellationException("stream closed")
      try { read(guarded,()=>active()); require(started,"stream has no descriptor") } finally close()
    }
    def close(): Unit = closed.set(true)
  }
}
/** Identity token: extensions can define additional typed local views without a core registry. */
class DataViewType[K,S](val name: String)
trait DataView[K,S] {
  def open(selection: S, context: StreamContext): DataStream[K]
}
case object AllBytes
object DataViews {
  val Bytes = new DataViewType[ByteChunk,AllBytes.type]("bytes")
  val Records = new DataViewType[RecordBatch,Scan]("records")
  /** Safe identity match is centralized; the same token always carries the same block/selection types. */
  def when[K,S,A,B](requested:DataViewType[K,S],expected:DataViewType[A,B])(view: => DataView[A,B]):Option[DataView[K,S]] =
    if(requested eq expected)Some(view.asInstanceOf[DataView[K,S]])else None
  def apply[K,S](read: (S,StreamContext,DataConsumer[K]) => Unit): DataView[K,S] = new DataView[K,S] {
    def open(selection:S,context:StreamContext):DataStream[K] = DataStream.scoped { (out,active) =>
      val scoped=context.copy(check=()=>{active();context.check()})
      scoped.check(); read(selection,scoped,out)
    }
  }
}
