package dacp


import java.net.URI
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, SynchronousQueue, ThreadPoolExecutor, TimeUnit, RejectedExecutionException}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import dacp.protocol.Metadata


object NodeConnection {
  def open(config: Config, allocator: BufferAllocator, value: String): FlightClient = {
    val u = DacpUri.parse(value,false)
    val origin=DacpUri.origin(value)
    dacp.union.ClientTransport.open(allocator,origin,Some(config.ca),Some(config.cert),Some(config.key),
      ()=>if(origin==DacpUri.origin(config.uri("")))Some(config.internalToken)else config.remoteItemCredentials.get(origin).map(p=>java.nio.file.Files.readString(p).trim))
  }
}
