package dacp.client

import java.io.{InputStream, IOException}
import java.net.URI
import java.time.Instant
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import dacp.protocol.Metadata
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

private[client] object Resources {
  def closeAll(values: Seq[AutoCloseable]): Unit = {
    var failure: Throwable = null
    values.foreach { value =>
      try DacpErrors.protect(value.close()) catch {
        case NonFatal(error) => if (failure == null) failure = error else failure.addSuppressed(error)
      }
    }
    if (failure != null) throw failure
  }
}

/** PEM credentials; omitted values fall back to DACP_CA, DACP_CERT, and DACP_KEY. */
final case class TlsConfig(ca: Option[Path] = None, cert: Option[Path] = None, key: Option[Path] = None)
final case class Item(path: String, format: String, schema: Option[Schema], kind: String, views: Seq[String],
                      lastModifiedTimeMs: Option[Long] = None, totalSizeBytes: Option[Long] = None,
                      recordCount: Option[Long] = None) {
  def lastModifiedTime: Option[Instant] = lastModifiedTimeMs.map(Instant.ofEpochMilli)
}

object Source {
  private[client] def parse(value: String): (URI, String, Option[String]) = {
    val u = new URI(value)
    require(u.getScheme == "dacp" && u.getHost != null && u.getUserInfo == null &&
      u.getQuery == null && u.getFragment == null && (u.getPort == -1 || u.getPort > 0 && u.getPort <= 65535),
      "expected dacp://host[:port]/dataset[/item]")
    val path = Option(u.getPath).getOrElse("").stripPrefix("/")
    val parts = path.split("/", -1)
    require(!path.contains("\\") && !path.exists(_.isControl) && parts.nonEmpty && parts.forall(p => p.nonEmpty && p != "." && p != ".."), "invalid dataset/item path")
    (u, parts.head, if (parts.length > 1) Some("/" + parts.tail.mkString("/")) else None)
  }

  /** Open a dataset. Listing metadata and reading contents remain separate operations. */
  def open(uri: String, tls: TlsConfig = TlsConfig(), timeoutSeconds: Long = 30): Dataset = {
    val (u, name, item) = parse(uri)
    require(item.isEmpty, "use Source.openItem for an item URI")
    require(timeoutSeconds > 0, "timeoutSeconds must be positive")
    def credential(value: Option[Path], env: String): Option[Path] = value.orElse(sys.env.get(env).map(Path.of(_)))
    val ca = credential(tls.ca, "DACP_CA")
    val cert = credential(tls.cert, "DACP_CERT")
    val key = credential(tls.key, "DACP_KEY")
    require(cert.isDefined == key.isDefined, "client certificate and key must be provided together")
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val client = Using.Manager { use =>
        val builder = FlightClient.builder(allocator, Location.forGrpcTls(u.getHost, if (u.getPort < 0) 8815 else u.getPort))
        ca.foreach(p => builder.trustedCertificates(use(Files.newInputStream(p))))
        cert.foreach(p => builder.clientCertificate(use(Files.newInputStream(p)), use(Files.newInputStream(key.get))))
        builder.build()
      }.get
      new Dataset(client, allocator, name, timeoutSeconds)
    } catch { case t: Throwable => allocator.close(); throw DacpErrors.translate(t) }
  }

  /** Direct item access; closing the item also closes its private connection. */
  def openItem(uri: String, tls: TlsConfig = TlsConfig(), timeoutSeconds: Long = 30): ItemStream = {
    val (u, name, item) = parse(uri)
    require(item.isDefined, "expected an item URI")
    val ds = open(new URI("dacp", null, u.getHost, u.getPort, "/" + name, null, null).toString, tls, timeoutSeconds)
    try {
      val stream = ds.open(item.get)
      stream.ownsDataset = true
      stream
    } catch { case t: Throwable => ds.close(); throw DacpErrors.translate(t) }
  }
}

/** Dataset, item handles, and readers are intended for single-threaded use. */
final class Dataset private[client] (
    private[client] val client: FlightClient, allocator: RootAllocator,
    val name: String, timeoutSeconds: Long) extends AutoCloseable {
  private var closed = false
  private val items = mutable.Set.empty[ItemStream]
  private[client] def check(): Unit = require(!closed, "dataset is closed")
  private[client] def option: CallOption = CallOptions.timeout(timeoutSeconds, TimeUnit.SECONDS)
  private def logical(path: String): String = {
    require(path.startsWith("/") && !path.contains("\\") && !path.exists(_.isControl) &&
      (path == "/" || path.substring(1).split("/", -1).forall(p => p.nonEmpty && p != "." && p != "..")),
      "expected absolute logical path")
    path
  }
  def list(): Seq[Item] = listing(FlightDescriptor.path("datasets", name))
  def list(path: String): Seq[Item] = listing(FlightDescriptor.path("datasets", name, "list", logical(path)))
  private def listing(descriptor: FlightDescriptor): Seq[Item] = {
    check()
    val info = DacpErrors.protect(client.getInfo(descriptor, option))
    Metadata.DataSet.parseFrom(info.getAppMetadata).getItemsList.asScala.map { i =>
      val schema = if (i.getArrowSchema.isEmpty) None else Some(Schema.deserializeMessage(java.nio.ByteBuffer.wrap(i.getArrowSchema.toByteArray)))
      Item(i.getPath, i.getFormat, schema, i.getKind, i.getViewsList.asScala.toVector,
        if (i.hasLastModifiedTimeMs) Some(i.getLastModifiedTimeMs) else None,
        if (i.hasTotalSizeBytes) Some(i.getTotalSizeBytes) else None,
        if (i.hasRecordCount) Some(i.getRecordCount) else None)
    }.toVector
  }
  /** Explicit exact record count; absent for non-record data. */
  def count(path: String): Option[Long] = {
    check()
    val info = DacpErrors.protect(client.getInfo(FlightDescriptor.path("datasets", name, "count", logical(path)), option))
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    if (metadata.hasRecordCount) Some(metadata.getRecordCount) else None
  }
  def open(path: String): ItemStream = {
    check()
    logical(path)
    val info = DacpErrors.protect(client.getInfo(FlightDescriptor.path("datasets", name, path), option))
    require(!info.getEndpoints.isEmpty, "item has no Flight endpoint")
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    require(metadata.getViewsCount > 0, "item has no readable views; use list(path) for folders")
    val item = new ItemStream(this, info.getEndpoints.get(0).getTicket, metadata.getViewsList.asScala.toVector)
    items += item
    item
  }
  private[client] def remove(item: ItemStream): Unit = items -= item
  override def close(): Unit = if (!closed) {
    closed = true
    Resources.closeAll(items.toVector ++ Seq(client, allocator))
  }
}

final class ItemStream private[client] (dataset: Dataset, ticket: Ticket, val views: Seq[String]) extends AutoCloseable {
  private var closed = false
  private val readers = mutable.Set.empty[AutoCloseable]
  private[client] var ownsDataset = false
  private def flight(view: String): FlightStream = {
    dataset.check()
    require(!closed, "item stream is closed")
    require(views.contains(view), "item does not support requested view")
    val t = Metadata.Ticket.parseFrom(ticket.getBytes).toBuilder.setView(view).build()
    DacpErrors.protect(dataset.client.getStream(new Ticket(t.toByteArray), dataset.option))
  }
  def asRecords(): RecordStream = {
    val reader = new RecordStream(flight("records"), r => readers -= r)
    readers += reader
    reader
  }
  def asBytes(): InputStream = {
    val reader = new ByteStream(flight("bytes"), r => readers -= r)
    readers += reader
    reader
  }
  override def close(): Unit = if (!closed) {
    closed = true
    try Resources.closeAll(readers.toVector)
    finally {
      dataset.remove(this)
      if (ownsDataset) dataset.close()
    }
  }
}

/** The root is reused by next(); consume/copy a batch before advancing or closing. */
final class RecordStream private[client] (stream: FlightStream, released: AutoCloseable => Unit) extends AutoCloseable {
  private var closed = false
  private var ended = false
  def schema: Schema = { require(!closed, "reader is closed"); DacpErrors.protect(stream.getSchema) }
  def root: VectorSchemaRoot = { require(!closed, "reader is closed"); DacpErrors.protect(stream.getRoot) }
  def next(): Boolean = {
    require(!closed, "reader is closed; reopen the item to retry")
    if (ended) return false
    try {
      val more = stream.next()
      ended = !more
      more
    } catch { case t: Throwable => close(); throw DacpErrors.translate(t) }
  }
  override def close(): Unit = if (!closed) {
    closed = true
    try DacpErrors.protect(stream.close()) finally released(this)
  }
}

private[client] final class ByteStream(stream: FlightStream, released: AutoCloseable => Unit) extends InputStream {
  private var closed = false
  private var ended = false
  private var row = 0
  private var current = Array.emptyByteArray
  private var offset = 0
  private var root: VectorSchemaRoot = null
  override def read(): Int = {
    val b = new Array[Byte](1)
    if (read(b, 0, 1) < 0) -1 else b(0) & 0xff
  }
  override def read(target: Array[Byte], start: Int, length: Int): Int = {
    java.util.Objects.checkFromIndexSize(start, length, target.length)
    if (closed) throw new IOException("reader is closed; reopen the item to retry")
    if (length == 0) return 0
    try {
      while (offset == current.length) {
        if (root != null && row < root.getRowCount) {
          current = Option(root.getVector(0).asInstanceOf[VarBinaryVector].get(row)).getOrElse(Array.emptyByteArray)
          row += 1; offset = 0
        } else {
          if (ended) return -1
          if (!stream.next()) { ended = true; return -1 }
          root = stream.getRoot; row = 0
        }
      }
      val n = math.min(length, current.length - offset)
      System.arraycopy(current, offset, target, start, n)
      offset += n
      n
    } catch { case t: Throwable => close(); throw DacpErrors.translate(t) }
  }
  override def close(): Unit = if (!closed) {
    closed = true
    current = Array.emptyByteArray
    try DacpErrors.protect(stream.close()) finally released(this)
  }
}
