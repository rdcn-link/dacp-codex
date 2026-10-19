package dacp

import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import dacp.protocol.Metadata
import org.yaml.snakeyaml.Yaml

case class Config(host: String, port: Int, publicHost: String, datasets: Map[String, DatasetDefinition], prefixes: Seq[String],
                  cert: Path, key: Path, ca: Path, batchSize: Int = 1024, byteChunk: Int = 65536,
                  inferSample: Int = 4, localOperatorLibrary: Path = Path.of("../operators"),
                  taskPeers: Seq[String] = Seq.empty,
                  taskInputPrefixes: Seq[String] = Seq.empty, maxConcurrentTasks: Int = 2,
                  remoteOperatorLibraryUrls: Seq[String] = Seq.empty, operatorCache: Path = Path.of(".operator-cache"),
                  operatorMaxBytes: Long = 8L * 1024 * 1024 * 1024,
                  sqlSources: Map[String, SqlConfig] = Map.empty, countFileRecordsOnStartup: Boolean = false, debug: Boolean = false, sourceRuntimes: Map[String, SourceRuntime] = Map.empty, union: Option[NodeUnionSettings] = None, executionHistoryDirectory: Option[Path] = None) extends DatasetContext with AutoCloseable {
  private[dacp] val taskSlots=new java.util.concurrent.Semaphore(maxConcurrentTasks)
  private var historyStore:ExecutionHistory=null
  def executionHistory:ExecutionHistory=synchronized {
    if(historyStore==null)historyStore=new ExecutionHistory(executionHistoryDirectory.getOrElse(Path.of(".dacp-executions",port.toString)))
    historyStore
  }
  def close(): Unit = {published.values.foreach(_.close());synchronized {if(historyStore!=null)historyStore.close()}}
  def sqlSourceNames: Set[String] = sqlSources.keySet
  def publish(items: Seq[PublishedItem]): PublishedDataset = DatasetProviders.publish(items,this)
  def openNamespace(items: Seq[PublishedItem], check: () => Unit): DatasetAccess = new NamespaceAdapter(items,this,check)
  require(datasets.keySet.forall(n => n.nonEmpty && !n.contains("/") && n != "." && n != ".." && !n.contains("\\") && !n.exists(_.isControl)), "invalid dataset name")
  private lazy val published = datasets.map { case (name,definition) =>
    name -> DatasetProviders.byId(definition.provider).create(definition,this)
  }
  def initializeDatasets(): Unit = { published; () }
  def openDataset(name: String, check: () => Unit = () => ()): DatasetAccess = published(name).open(check)
  def hasDataset(name: String): Boolean = datasets.contains(name)
  remoteOperatorLibraryUrls.foreach { base =>
    val uri = URI.create(base)
    require(Set("http", "https").contains(uri.getScheme) && uri.getHost != null &&
      uri.getQuery == null && uri.getFragment == null && uri.getUserInfo == null,
      "invalid remote_operator_library_urls entry")
  }
  require(operatorMaxBytes > 0, "operator_max_bytes must be positive")
  require(batchSize > 0 && byteChunk > 0 && inferSample > 0 && maxConcurrentTasks > 0)
  def uri(ds: String, item: String = ""): String = new URI("dacp", null, publicHost, port, "/" + ds + item, null, null).toASCIIString
  def authorize(ds: String, item: String): Unit = {
    val candidate = uri(ds, item)
    // Connection identity comes from mandatory mTLS. This node-wide allowlist is URI-segment bounded.
    require(prefixes.exists { p => val clean = p.stripSuffix("/"); candidate == clean || candidate.startsWith(clean + "/") }, "FORBIDDEN: URI prefix denied")
  }
}
object Config {
  def load(path: Path): Config = {
    val in = Files.newInputStream(path)
    val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val m = try new Yaml(options).load[java.util.Map[String,Object]](in).asScala finally in.close()
    val settings = Set("host", "port", "public_host", "datasets", "sql_sources", "allowed_uri_prefixes",
      "tls_cert", "tls_key", "tls_ca", "batch_size", "byte_chunk", "infer_sample",
      "local_operator_library", "task_peers", "task_input_prefixes", "max_concurrent_tasks",
      "remote_operator_library_urls", "operator_cache", "operator_max_bytes", "count_file_records_on_startup", "plugin_directory", "mode", "source_runtimes", "union", "execution_history_directory")
    require(m.keySet.subsetOf(settings), "unknown server setting")
    PluginLoader.initialize(m.get("plugin_directory").map(v => Path.of(v.toString)))
    Formats.initialize()
    def str(k: String) = m(k).toString
    def n(k: String, d: Int) = m.get(k).map(_.toString.toInt).getOrElse(d)
    def strings(k: String): Seq[String] = m.get(k).map(_.asInstanceOf[java.util.List[String]].asScala.toSeq).getOrElse(Seq.empty)
    val sql = m.get("sql_sources").map(_.asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map { case (name,raw) =>
      val v = raw.asScala
      def opt(k: String) = v.get(k).map(_.toString)
      def secret(k: String): Option[String] = opt(k + "_env").map(e => sys.env.getOrElse(e,throw new IllegalArgumentException(s"missing environment variable $e"))).orElse(opt(k))
      name -> SqlConfig(v("url").toString,secret("user"),secret("password"),opt("schema"),
        v.get("tables").map(_.asInstanceOf[java.util.List[String]].asScala.toSeq),
        opt("fetch_size").map(_.toInt).getOrElse(n("batch_size",1024)),opt("query_timeout_seconds").map(_.toInt).getOrElse(60))
    }.toMap).getOrElse(Map.empty[String,SqlConfig])
    val definitions = m.get("datasets").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.map { case (name,raw) =>
      require(raw.isInstanceOf[java.util.Map[_,_]],"datasets require provider settings")
      val v = raw.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
      require(v.contains("provider"), "dataset provider is required")
      val provider = v("provider").toString
      val factory = DatasetProviders.byId(provider)
      val settingsKey = factory.itemSettingsKey
      require(v.keySet.subsetOf(Set("provider","source",settingsKey)),"unknown dataset setting")
      val source = v.get("source").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.toMap).getOrElse(Map.empty[String,Object])
      val items = v.get(settingsKey).map(_.asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map {
        case (path,settings) => path -> settings.asScala.toMap
      }.toMap).getOrElse(Map.empty[String,Map[String,Object]])
      name -> DatasetDefinition(provider,source,items)
    }.toMap).getOrElse(Map.empty[String,DatasetDefinition])
    val mode=m.get("mode").map(_.toString).getOrElse("NORMAL")
    require(Set("NORMAL","DEBUG")(mode),"mode must be NORMAL or DEBUG")
    val runtimes=m.get("source_runtimes").map(_.asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map {case(id,raw)=>
      val r=raw.asScala
      require(r.keySet==Set("image","executor"),"source runtime requires image and executor")
      id -> SourceRuntime(r("image").toString,r("executor").toString)
    }.toMap).getOrElse(Map.empty[String,SourceRuntime])
    val config = Config(str("host"), n("port",8815), str("public_host"), definitions,
      m("allowed_uri_prefixes").asInstanceOf[java.util.List[String]].asScala.toSeq,
      Path.of(str("tls_cert")), Path.of(str("tls_key")), Path.of(str("tls_ca")), n("batch_size",1024), n("byte_chunk",65536), n("infer_sample",4),
      Path.of(m.get("local_operator_library").map(_.toString).getOrElse("../operators")), strings("task_peers"), strings("task_input_prefixes"), n("max_concurrent_tasks",2), strings("remote_operator_library_urls"),
      Path.of(m.get("operator_cache").map(_.toString).getOrElse(".operator-cache")),
      m.get("operator_max_bytes").map(_.toString.toLong).getOrElse(8L * 1024 * 1024 * 1024), sql,
      m.get("count_file_records_on_startup").exists(_.toString.toBoolean), mode=="DEBUG", runtimes, m.get("union").map(v=>NodeUnionSettings.read(Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v))),m.get("execution_history_directory").map(v=>Path.of(v.toString)))
    config.initializeDatasets()
    config
  }
}
class Producer(config: Config, allocator: BufferAllocator, tasks: Option[TaskService] = None, union: Option[UnionWorker] = None) extends NoOpFlightProducer {
  override def doExchange(ctx: FlightProducer.CallContext, reader: FlightStream, writer: FlightProducer.ServerStreamListener): Unit = tasks match {
    case Some(service) => service.exchange(ctx,reader,writer)
    case None => writer.error(CallStatus.UNIMPLEMENTED.withDescription("task execution disabled").toRuntimeException)
  }
  private def adapter(ds: String, item: String, ctx: FlightProducer.CallContext): DatasetAccess = {
    def active(): Unit = if (ctx.isCancelled) throw new java.util.concurrent.CancellationException()
    if (!config.hasDataset(ds))
      throw CallStatus.NOT_FOUND.withDescription("NOT_FOUND: dataset not found; check the dataset name").toRuntimeException
    config.authorize(ds,item)
    config.openDataset(ds,() => active())
  }

  private def fail(t: Throwable): FlightRuntimeException = t match {
    case e: FlightRuntimeException => e
    case e: java.io.FileNotFoundException => CallStatus.NOT_FOUND.withDescription("NOT_FOUND: logical resource unavailable").toRuntimeException
    case e: java.nio.file.NoSuchFileException => CallStatus.NOT_FOUND.withDescription("NOT_FOUND: logical resource unavailable").toRuntimeException
    case e: ScanFailure => CallStatus.INVALID_ARGUMENT.withDescription("SCAN_ERROR: " + e.getMessage).toRuntimeException
    case e: FormatFailure => CallStatus.INVALID_ARGUMENT.withDescription("FORMAT_ERROR: " + e.getMessage).toRuntimeException
    case e: java.sql.SQLException =>
      System.err.println(s"SQL_ERROR: state=${e.getSQLState}, code=${e.getErrorCode}")
      CallStatus.INTERNAL.withDescription("SQL_ERROR: database operation failed; see server log").toRuntimeException
    case e if Option(e.getMessage).exists(_.contains("FORBIDDEN")) => CallStatus.UNAUTHORIZED.withDescription("FORBIDDEN: logical URI prefix denied").toRuntimeException
    case e if Option(e.getMessage).exists(_.contains("SCHEMA_CONFLICT")) =>
      CallStatus.INVALID_ARGUMENT.withDescription("SCHEMA_CONFLICT: data does not match its schema").toRuntimeException
    case e if Set("requirement failed: folder has no content stream", "requirement failed: listing requires a folder",
      "requirement failed: expected absolute logical path", "requirement failed: invalid logical path",
      "requirement failed: unsupported stream view").contains(e.getMessage) =>
      CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage.stripPrefix("requirement failed: ")).toRuntimeException
    case _ => CallStatus.INVALID_ARGUMENT.withDescription("INVALID_ARGUMENT: invalid logical request or unavailable content").toRuntimeException
  }
  private def itemMetadata(i: Item): Metadata.Item = {
    val b = Metadata.Item.newBuilder().setPath(i.path).setFormat(i.format).setKind(i.kind)
      .addAllViews(NamespaceAdapter.views(i).asJava)
    i.schema.foreach(s => b.setArrowSchema(com.google.protobuf.ByteString.copyFrom(s.serializeAsMessage())))
    i.lastModifiedTimeMs.foreach(n => b.setLastModifiedTimeMs(n))
    i.totalSizeBytes.foreach(n => b.setTotalSizeBytes(n))
    i.recordCount.foreach(n => b.setRecordCount(n))
    b.build()
  }
  override def getFlightInfo(ctx: FlightProducer.CallContext, desc: FlightDescriptor): FlightInfo = try {
    val p = desc.getPath.asScala.toSeq
    require(p.size == 2 || p.size == 3 || (p.size == 4 && Set("list", "count")(p(2))), "invalid logical descriptor")
    require(p.head == "datasets", "unknown descriptor namespace")
    val ds = p(1); val item = if (p.size == 4) p(3) else p.lift(2).getOrElse("")
    if (item.nonEmpty) LogicalPath.validate(item)
    val a = adapter(ds,item,ctx)
    try {
    if (p.size == 4 && p(2) == "count") {
      val metadata = itemMetadata(Item(item,"",None,recordCount=a.count(item)))
      new FlightInfo(PluginSchemas.bytes, desc, java.util.Collections.emptyList[FlightEndpoint](), -1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,metadata.toByteArray)
    } else if (p.size == 2 || p.size == 4) {
      val meta = Metadata.DataSet.newBuilder().setUri(config.uri(ds))
      // Permit item-specific prefixes while keeping list itself restricted by the dataset prefix.
      (if (p.size == 4) a.list(item) else a.list()).filter(i =>
        scala.util.Try(config.authorize(ds,i.path)).isSuccess).foreach { i =>
        meta.addItems(itemMetadata(i))
      }
      new FlightInfo(PluginSchemas.bytes, desc, java.util.Collections.emptyList[FlightEndpoint](), -1,-1,false,org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,meta.build.toByteArray)
    } else {
      val description = a.describe(item)
      require(NamespaceAdapter.views(description).nonEmpty, "folder has no content stream")
      val schema = description.schema.get
      val ticket = Metadata.Ticket.newBuilder().setDataset(ds).setItem(item).setView(NamespaceAdapter.views(description).head).build()
      new FlightInfo(schema,desc,List(new FlightEndpoint(new Ticket(ticket.toByteArray))).asJava,-1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,
        itemMetadata(description).toByteArray)
    }
    } finally a.close()
  } catch { case t: Throwable => throw fail(t) }
  override def doAction(ctx:FlightProducer.CallContext,action:Action,listener:FlightProducer.StreamListener[Result]):Unit=
    union match {
      case Some(worker) if action.getType.startsWith(dacp.union.Rpc.Prefix)=>worker.doAction(ctx,action,listener)
      case _=>listener.onError(CallStatus.UNIMPLEMENTED.withDescription("unknown action").toRuntimeException)
    }
  override def getStream(ctx: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    if(dacp.union.Rpc.isTicket(ticket.getBytes)){
      union match {case Some(worker)=>worker.getStream(ctx,ticket,listener);case None=>listener.error(CallStatus.UNIMPLEMENTED.withDescription("Union disabled").toRuntimeException)}
      return
    }
    try {
      val t = Metadata.Ticket.parseFrom(ticket.getBytes)
      require(Set("records","bytes").contains(t.getView), "invalid view")
      val namespace = adapter(t.getDataset,t.getItem,ctx)
      try {
      val lock = new Object
      listener.setOnReadyHandler(() => lock.synchronized(lock.notifyAll()))
      listener.setOnCancelHandler(() => lock.synchronized(lock.notifyAll()))
      def ready(): Unit = lock.synchronized {
        while (!listener.isReady && !listener.isCancelled) lock.wait(100)
        if (listener.isCancelled) throw new java.util.concurrent.CancellationException()
      }
      if(t.hasScan) {
        require(t.getView == "records","scan options require records view")
        namespace.scan(t.getItem,ScanProtocol.decode(t.getScan),allocator,listener,() => ready())
      } else namespace.stream(t.getItem,t.getView,allocator,listener,() => ready())
      } finally namespace.close()
    } catch { case _: java.util.concurrent.CancellationException => (); case t: Throwable => listener.error(fail(t)) }
  }
}
object Main {
  def main(args: Array[String]): Unit = {
    if(args.headOption.contains("history")){ExecutionHistoryAdmin.main(args.drop(1));return}
    if(args.headOption.contains("union")){UnionNodeAdmin.main(args.drop(1));return}
    val config = Config.load(Path.of(args.headOption.getOrElse("../examples/server.yaml")))
    val allocator = new RootAllocator(Long.MaxValue)
    val executor = new TaskExecutor(config,allocator)
    config.executionHistory
    val tasks = new TaskService(config,allocator,executor)
    val union=config.union.map(s=>new UnionWorker(config,s,allocator))
    val producer = new Producer(config,allocator,Some(tasks),union)
    val server = dacp.union.Rpc.secure(FlightServer.builder(allocator,Location.forGrpcTls(config.host,config.port),producer),new dacp.union.Tls(config.cert,config.key,config.ca))
      .maxInboundMessageSize(dacp.union.Rpc.MaxMessage).build()
    try {server.start();union.foreach(_.start())} catch {case scala.util.control.NonFatal(t)=>
      union.foreach(_.close());server.close();tasks.close();config.close();PluginLoader.close();allocator.close();throw t
    }
    Runtime.getRuntime.addShutdownHook(new Thread(() => { union.foreach(_.close()); server.close(); tasks.close(); config.close(); PluginLoader.close(); allocator.close() }))
    val log = org.slf4j.LoggerFactory.getLogger("dacp.Main")
    log.debug(s"remote_operator_library_urls (${config.remoteOperatorLibraryUrls.size}):")
    config.remoteOperatorLibraryUrls.zipWithIndex.foreach { case (url,index) =>
      log.debug(s"  [$index] $url")
    }
    println(s"dacp listening on ${config.uri("")}")
    val datasetNames = config.datasets.keySet.toSeq.sorted
    println(s"configured datasets (${datasetNames.size}):")
    datasetNames.foreach { name =>
      println(s"  $name ${config.uri(name)}")
    }
    require(args.length <= 1,"submit jobs through the shell or SDK")
    server.awaitTermination()
  }
}
