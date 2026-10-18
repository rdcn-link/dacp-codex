package dacp

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

object TestTrackedHandler {
  val opened = new AtomicInteger()
  val closed = new AtomicInteger()
}
class TestTrackedHandler extends example.MessageFileHandler {
  override val id = "test-tracked"
  override val extensions = Set("tracked")
  override def openStructure(path: Path, options: FileOptions, context: DatasetContext, check: () => Unit): DatasetAccess = {
    val delegate = super.openStructure(path,options,context,check)
    TestTrackedHandler.opened.incrementAndGet()
    new DatasetAccess {
      def list(): Seq[Item] = delegate.list()
      def list(path: String): Seq[Item] = delegate.list(path)
      def describe(path: String): Item = delegate.describe(path)
      def count(path: String): Option[Long] = delegate.count(path)
      def stream(path: String, view: String, a: BufferAllocator, l: ServerStreamListener, ready: () => Unit): Unit =
        delegate.stream(path,view,a,l,ready)
      override def close(): Unit = { delegate.close(); TestTrackedHandler.closed.incrementAndGet() }
    }
  }
}
