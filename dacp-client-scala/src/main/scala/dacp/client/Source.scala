package dacp.client

import _root_.xstream.data._

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
final case class TlsConfig(ca: Option[Path] = None, cert: Option[Path] = None, key: Option[Path] = None, token:Option[String]=None,tokenFile:Option[Path]=None)
final case class Item(path: String, format: String, schema: Option[Schema], kind: String, views: Seq[String],
                      lastModifiedTimeMs: Option[Long] = None, totalSizeBytes: Option[Long] = None,
                      recordCount: Option[Long] = None) {
  def lastModifiedTime: Option[Instant] = lastModifiedTimeMs.map(Instant.ofEpochMilli)
}

final case class DatasetInfo(name:String,uri:String,description:String,nodeIdentity:String,observedAtMs:Long)
object DatasetInfo {
  private[client] def fromFlight(info:FlightInfo):DatasetInfo={
    val m=Metadata.DataSet.parseFrom(info.getAppMetadata)
    DatasetInfo(m.getName,m.getUri,m.getDescription,m.getNodeIdentity,m.getObservedAtMs)
  }
}

final case class DatasetDirectory(entries:Vector[DatasetInfo],complete:Boolean,unavailableSources:Int) extends IndexedSeq[DatasetInfo] {
  def length:Int=entries.length
  def apply(index:Int):DatasetInfo=entries(index)
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
    val (client,allocator)=connection(u,tls)
    new Dataset(client,allocator,name,timeoutSeconds)
  }

  /** Public metadata only. Accepts either a DACP node origin or a Union origin. */
  def datasets(origin:String,tls:TlsConfig=TlsConfig(),timeoutSeconds:Long=30):DatasetDirectory={
    val u=new URI(origin)
    require(u.getScheme=="dacp" && u.getHost!=null && u.getUserInfo==null && u.getQuery==null && u.getFragment==null &&
      (u.getPath==null || u.getPath.isEmpty || u.getPath=="/") && (u.getPort == -1 || u.getPort>0 && u.getPort<=65535) && timeoutSeconds>0,"expected DACP origin and positive timeout")
    val (client,allocator)=connection(u,tls)
    try DacpErrors.protect {
      val results=client.doAction(new Action("dacp.catalog",Array.emptyByteArray),CallOptions.timeout(timeoutSeconds,TimeUnit.SECONDS)).asScala.toVector
      require(results.size==1,"invalid directory response")
      val d=Metadata.Directory.parseFrom(results.head.getBody)
      DatasetDirectory(d.getDatasetsList.asScala.map(m=>DatasetInfo(m.getName,m.getUri,m.getDescription,m.getNodeIdentity,m.getObservedAtMs)).toVector,d.getComplete,d.getUnavailableSources)
    }
    finally Resources.closeAll(Seq(client,allocator))
  }

  /** Public Union logical execution nodes, using ordinary client credentials. */
  def nodes(origin:String,tls:TlsConfig=TlsConfig(),timeoutSeconds:Long=30):com.fasterxml.jackson.databind.JsonNode={
    val canonical=dacp.union.Json.origin(origin)
    require(timeoutSeconds>0,"timeoutSeconds must be positive")
    val (client,allocator)=connection(new URI(canonical),tls)
    try {
      val results=client.doAction(new Action("dacp.nodes",Array.emptyByteArray),CallOptions.timeout(timeoutSeconds,TimeUnit.SECONDS)).asScala
      require(results.hasNext,"missing node directory response");val bytes=results.next().getBody
      require(bytes.length<=1024*1024 && !results.hasNext,"invalid node directory response")
      dacp.union.Json.mapper.readTree(bytes)
    }finally Resources.closeAll(Seq(client,allocator))
  }

  private[client] def connection(u:URI,tls:TlsConfig):(FlightClient,RootAllocator)={
    def credential(value: Option[Path], env: String): Option[Path] = value.orElse(sys.env.get(env).map(Path.of(_)))
    val ca = credential(tls.ca, "DACP_CA")
    val cert = credential(tls.cert, "DACP_CERT")
    val key = credential(tls.key, "DACP_KEY")
    require(cert.isDefined == key.isDefined, "client certificate and key must be provided together")
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val client=dacp.union.ClientTransport.open(allocator,new URI("dacp",null,u.getHost,if(u.getPort<0)8815 else u.getPort,null,null,null).toString,
        ca,cert,key,()=>tls.token.orElse(credential(tls.tokenFile,"DACP_TOKEN_FILE").map(p=>Files.readString(p).trim)))
      (client,allocator)
    }catch{case t:Throwable=>allocator.close();throw DacpErrors.translate(t)}
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
    private[client] val client: FlightClient, private[client] val allocator: RootAllocator,
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
  /** Item metadata only; never opens a content stream or requests an exact row count. */
  def describe(path:String):Item={
    check();logical(path)
    val info=DacpErrors.protect(client.getInfo(FlightDescriptor.path("datasets",name,path),option))
    val i=Metadata.Item.parseFrom(info.getAppMetadata)
    val schema=if(i.getArrowSchema.isEmpty)None else Some(Schema.deserializeMessage(java.nio.ByteBuffer.wrap(i.getArrowSchema.toByteArray)))
    Item(i.getPath,i.getFormat,schema,i.getKind,i.getViewsList.asScala.toVector,
      if(i.hasLastModifiedTimeMs)Some(i.getLastModifiedTimeMs)else None,
      if(i.hasTotalSizeBytes)Some(i.getTotalSizeBytes)else None,
      if(i.hasRecordCount)Some(i.getRecordCount)else None)
  }
  /** Explicit exact record count; absent for non-record data. */
  def count(path: String): Option[Long] = {
    check()
    val info = DacpErrors.protect(client.getInfo(FlightDescriptor.path("datasets", name, "count", logical(path)), option))
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    if (metadata.hasRecordCount) Some(metadata.getRecordCount) else None
  }
  def open(path: String, predicate: Option[dacp.client.Predicate] = None, columns: Option[Seq[String]] = None,
           limit: Option[Long] = None): ItemStream = {
    require(limit.forall(_ >= 0), "limit must be nonnegative")
    require(columns.forall(c => c.nonEmpty && c.forall(n => n != null && n.nonEmpty) && c.distinct.size == c.size),
      "projection requires unique nonempty columns")
    check()
    logical(path)
    val info = DacpErrors.protect(client.getInfo(FlightDescriptor.path("datasets", name, path), option))
    require(!info.getEndpoints.isEmpty, "item has no Flight endpoint")
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    require(metadata.getViewsCount > 0, "item has no readable views; use list(path) for folders")
    val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder
    if(predicate.isDefined || columns.isDefined || limit.isDefined) {
      require(metadata.getViewsList.contains("records"), "scan options require records view")
      val scan = Metadata.Scan.newBuilder()
      predicate.foreach(p => scan.setPredicate(p.message))
      columns.foreach(c => scan.setProjection(Metadata.Scan.Projection.newBuilder().addAllColumns(c.asJava)))
      limit.foreach(scan.setLimit)
      ticket.setScan(scan)
    }
    val item = new ItemStream(this, new Ticket(ticket.build().toByteArray), metadata.getViewsList.asScala.toVector)
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
  private def flight(view: String, selection:Option[com.fasterxml.jackson.databind.JsonNode]=None): FlightStream = {
    dataset.check()
    require(!closed, "item stream is closed")
    require(views.contains(view), "item does not support requested view")
    val builder=Metadata.Ticket.parseFrom(ticket.getBytes).toBuilder.setView(view)
    selection.foreach(n=>builder.setSelectionJson(com.google.protobuf.ByteString.copyFrom(Data.mapper.writeValueAsBytes(n))))
    val t=builder.build()
    require(!t.hasScan || view == "records", "scan options require records view")
    DacpErrors.protect(dataset.client.getStream(new Ticket(t.toByteArray), dataset.option))
  }
  private var codecEnvironment:Option[PluginEnvironment]=None
  /** Typed scientific stream; descriptor and chunks are decoded by a discovered codec. */
  def asView[K,S](kind:DataViewType[K,S],selection:S):DataStream[K]={
    val environment=codecEnvironment.getOrElse {val e=new PluginEnvironment();codecEnvironment=Some(e);e}
    val codec=DataViewCodecs.forType(environment,kind).getOrElse(throw new IllegalArgumentException("view codec unavailable: "+kind.name))
    val view=codec.remoteView(kind)((request,out,context)=>{
      val reader=flight(kind.name,Some(request))
      readers+=reader
      try dacp.union.ScientificWire.consume(reader,kind.name,out,context.check) finally {reader.cancel("view closed",null);reader.close();readers-=reader}
    }).get
    // No allocator is used for decoded scientific blocks; the transport owns its allocator.
    val stream=view.open(selection,StreamContext(dataset.allocator))
    readers+=stream
    stream
  }
  def asArray(selection:ArraySelection=ArraySelection()):DataStream[ArrayChunk]=asView(ScientificViews.Arrays,selection)
  def asImage(selection:ImageSelection=ImageSelection()):DataStream[ImageTile]=asView(ScientificViews.Images,selection)
  def asSequences(selection:SequenceSelection=SequenceSelection()):DataStream[SequenceChunk]=asView(ScientificViews.Sequences,selection)
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
    try Resources.closeAll(readers.toVector ++ codecEnvironment.toVector)
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
