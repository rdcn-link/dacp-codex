package dacp

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Storage-independent stream dispatch. Providers own I/O and stream resources. */
object DatasetStreams {
  def stream(access: DatasetAccess, path: String, view: String, allocator: BufferAllocator,
             listener: ServerStreamListener, ready: () => Unit): Unit = {
    ready()
    access.stream(path,view,allocator,listener,ready)
  }
}
