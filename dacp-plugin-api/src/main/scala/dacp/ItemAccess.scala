package dacp

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** A request-scoped content handle. Storage identifiers and clients remain plugin-private.
  * stream/scan are synchronous, must honour ready before reads/emits, and complete the listener
  * on success. The caller closes the handle on success, failure, and cancellation.
  */
trait ItemAccess extends AutoCloseable {
  def count(): Option[Long]
  def stream(view: String, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit
  def scan(request: Scan, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit =
    ScanStreams.stream(request,allocator,listener)(filtered => stream("records",allocator,filtered,ready))
  def close(): Unit = ()
}

/** Optional building block for datasets backed by independently opened objects or cursors.
  * Listing/metadata stay dataset-owned; opening content does not require a local file or SQL table.
  */
trait ItemDatasetAccess extends DatasetAccess {
  def openItem(path: String): ItemAccess
  private def useItem[A](path: String)(f: ItemAccess => A): A = {
    LogicalPath.validate(path)
    val item = openItem(path)
    try f(item) finally item.close()
  }
  final def count(path: String): Option[Long] = {
    LogicalPath.validate(path)
    if (describe(path).kind == "folder") None else useItem(path)(_.count())
  }
  final override def scan(path: String, request: Scan, allocator: BufferAllocator,
                          listener: ServerStreamListener, ready: () => Unit): Unit = {
    LogicalPath.validate(path)
    ready()
    require(describe(path).views.exists(_.contains("records")), "unsupported stream view")
    useItem(path)(_.scan(request,allocator,listener,ready))
  }
  final def stream(path: String, view: String, allocator: BufferAllocator,
                   listener: ServerStreamListener, ready: () => Unit): Unit = {
    LogicalPath.validate(path)
    ready()
    require(describe(path).views.exists(_.contains(view)), "unsupported stream view")
    useItem(path)(_.stream(view,allocator,listener,ready))
  }
}
