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
                  sources: Map[String, SourceDefinition] = Map.empty, countFileRecordsOnStartup: Boolean = false, debug: Boolean = false, sourceRuntimes: Map[String, SourceRuntime] = Map.empty, union: Option[NodeUnionSettings] = None, executionStore: Option[ExecutionStoreDefinition] = None,
                  datasetCatalog: Option[DatasetCatalogDefinition] = None, catalogOverride: Option[DatasetCatalog] = None, pluginEnvironment: Option[PluginEnvironment] = None, pluginDirectory: Option[Path] = None, ownsPluginEnvironment:Boolean=false, accessLogDirectory:Option[Path]=None, accessLogMaxBytes:Long=64L*1024*1024, operatorFusionEnabled:Boolean=true, web:NodeWebSettings=NodeWebSettings()) extends CatalogContext with AutoCloseable {
  private var suppliedPlugins=pluginEnvironment
  private var ownsPlugins=ownsPluginEnvironment || pluginEnvironment.isEmpty
  private[dacp] def installPlugins(value:PluginEnvironment,owned:Boolean):Unit=synchronized {require(activePlugins==null && !closed.get(),"plugins must be supplied before initialization");suppliedPlugins=Some(value);ownsPlugins=owned}
  private var activePlugins:PluginEnvironment=null
  def plugins:PluginEnvironment=synchronized {
    require(!closed.get(),"configuration is closed")
    if(activePlugins==null)activePlugins=suppliedPlugins.getOrElse(new PluginEnvironment(pluginDirectory))
    activePlugins
  }
  private[dacp] val taskSlots=new java.util.concurrent.Semaphore(maxConcurrentTasks)
  private var accessLogger:AccessLog=null
  def accessLog:AccessLog=synchronized {
    require(!closed.get(),"configuration is closed")
    if(accessLogger==null)accessLogger=new AccessLog(accessLogDirectory.getOrElse(Path.of(".dacp-access",port.toString)),accessLogMaxBytes)
    accessLogger
  }
  private var historyStore:ExecutionHistory=null
  def executionHistory:ExecutionHistory=synchronized {
    if(historyStore==null)historyStore=new ExecutionHistory({val d=executionStore.getOrElse(ExecutionStoreDefinition("file",Map("directory"->Path.of(".dacp-executions",port.toString).toString)));plugins.byId(classOf[ExecutionStoreProvider],d.provider).open(d.settings,false)})
    historyStore
  }
  private var activeCatalog:DatasetCatalog=null
  private var suppliedCatalog=catalogOverride
  private[dacp] def installCatalog(value:DatasetCatalog):Unit=synchronized {
    require(activeCatalog==null && !closed.get(),"catalog must be supplied before initialization")
    suppliedCatalog=Some(value)
  }
  private val closed=new AtomicBoolean()
  private def catalog:DatasetCatalog=synchronized {
    require(!closed.get(),"configuration is closed")
    if(activeCatalog==null)activeCatalog=suppliedCatalog.getOrElse(DatasetCatalogs.create(datasetCatalog.getOrElse(DatasetCatalogDefinition("static")),this))
    activeCatalog
  }
  def close():Unit=if(closed.compareAndSet(false,true)) {
    var failure:Throwable=null
    val owned=synchronized {Option(activeCatalog).orElse(suppliedCatalog).toSeq++Option(historyStore).toSeq++Option(accessLogger).toSeq++(if(ownsPlugins)Option(activePlugins).orElse(suppliedPlugins).toSeq else Seq.empty)}
    owned.foreach(p=>try p.close() catch{case scala.util.control.NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)})
    if(failure!=null)throw failure
  }
  def configuredDatasets:Map[String,DatasetDefinition]=datasets
  private val compositionDepth=new ThreadLocal[Int] {override def initialValue():Int=0}
  def createDataset(definition:DatasetDefinition):PublishedDataset={
    val depth=compositionDepth.get();require(depth<32,"dataset composition exceeds maximum depth")
    compositionDepth.set(depth+1)
    try plugins.byId(classOf[DatasetProvider],definition.provider).create(definition,this)finally compositionDepth.set(depth)
  }
  datasets.keys.foreach(DatasetNames.validate)
  require(datasetCatalog.isEmpty || datasets.isEmpty,"datasets and dataset_catalog are mutually exclusive")
  def initializeDatasets(): Unit = { catalog; () }
  def datasetInfos(check:()=>Unit=()=>()):Seq[DatasetInfo]={
    val result=catalog.list(check).toVector
    result.foreach(i=>DatasetNames.validate(i.name))
    require(result.map(_.name).distinct.size==result.size,"duplicate dataset name")
    result.sortBy(_.name)
  }
  def openDataset(name:String,check:()=>Unit=()=>()):DatasetAccess={
    DatasetNames.validate(name);check()
    val lease=try catalog.open(name,check) catch{case _:java.io.FileNotFoundException=>throw new DatasetNotFound}
    val access=try {check();lease.open(check)}catch{case scala.util.control.NonFatal(t)=>try lease.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
    new DatasetAccess {
      private val ended=new AtomicBoolean()
      def list():Seq[Item]=access.list()
      def list(path:String):Seq[Item]=access.list(path)
      def describe(path:String):Item=access.describe(path)
      def count(path:String):Option[Long]=access.count(path)
      override def scan(path:String,request:Scan,allocator:BufferAllocator,listener:FlightProducer.ServerStreamListener,ready:()=>Unit):Unit=access.scan(path,request,allocator,listener,ready)
      def stream(path:String,view:String,allocator:BufferAllocator,listener:FlightProducer.ServerStreamListener,ready:()=>Unit):Unit=access.stream(path,view,allocator,listener,ready)
      override def close():Unit=if(ended.compareAndSet(false,true))try access.close()finally lease.close()
    }
  }
  def hasDataset(name:String):Boolean={DatasetNames.validate(name);datasetInfos().exists(_.name==name)}
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
  def load(path: Path, catalogOverride:Option[DatasetCatalog]=None,providedPlugins:Option[PluginEnvironment]=None,ownsProvidedPlugins:Boolean=false): Config = {
    val in = Files.newInputStream(path)
    val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val m = try new Yaml(options).load[java.util.Map[String,Object]](in).asScala finally in.close()
    val settings = Set("host", "port", "public_host", "datasets", "sources", "allowed_uri_prefixes",
      "tls_cert", "tls_key", "tls_ca", "batch_size", "byte_chunk", "infer_sample",
      "operator_fusion_enabled", "local_operator_library", "task_peers", "task_input_prefixes", "max_concurrent_tasks",
      "remote_operator_library_urls", "operator_cache", "operator_max_bytes", "count_file_records_on_startup", "plugin_directory", "mode", "source_runtimes", "union", "execution_store", "dataset_catalog", "access_log", "web")
    require(m.keySet.subsetOf(settings), "unknown server setting")
    val environment=providedPlugins.getOrElse(new PluginEnvironment(m.get("plugin_directory").map(v=>Path.of(v.toString))))
    try {
    def str(k: String) = m(k).toString
    def n(k: String, d: Int) = m.get(k).map(_.toString.toInt).getOrElse(d)
    def strings(k: String): Seq[String] = m.get(k).map(_.asInstanceOf[java.util.List[String]].asScala.toSeq).getOrElse(Seq.empty)
    val sources=m.get("sources").map(_.asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map {case(name,raw)=>
      val value=raw.asScala
      require(value.keySet==Set("provider","settings"),"source requires provider and settings")
      name->SourceDefinition(value("provider").toString,value("settings").asInstanceOf[java.util.Map[String,Object]].asScala.toMap)
    }.toMap).getOrElse(Map.empty[String,SourceDefinition])
    val definitions=(if(catalogOverride.isDefined)None else m.get("datasets")).map(_.asInstanceOf[java.util.Map[String,Object]].asScala.map {case(name,raw)=>
      require(raw.isInstanceOf[java.util.Map[_,_]],"datasets require provider settings");name->environment.datasetDefinition(raw.asInstanceOf[java.util.Map[String,Object]].asScala.toMap)
    }.toMap).getOrElse(Map.empty[String,DatasetDefinition])
    val catalogDefinition=if(catalogOverride.isDefined)None else m.get("dataset_catalog").map { raw=>
      require(raw.isInstanceOf[java.util.Map[_,_]],"dataset_catalog must be a mapping")
      val value=raw.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
      require(value.keySet.subsetOf(Set("provider","source")) && value.get("provider").exists(_.isInstanceOf[String]),"invalid dataset_catalog settings")
      val source=value.get("source").map { raw=>
        require(raw.isInstanceOf[java.util.Map[_,_]],"dataset_catalog source must be a mapping")
        raw.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
      }.getOrElse(Map.empty[String,Object])
      DatasetCatalogDefinition(value("provider").toString,source)
    }
    val mode=m.get("mode").map(_.toString).getOrElse("NORMAL")
    require(Set("NORMAL","DEBUG")(mode),"mode must be NORMAL or DEBUG")
    val runtimes=m.get("source_runtimes").map(_.asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map {case(id,raw)=>
      val r=raw.asScala
      require(r.keySet==Set("image","executor"),"source runtime requires image and executor")
      id -> SourceRuntime(r("image").toString,r("executor").toString)
    }.toMap).getOrElse(Map.empty[String,SourceRuntime])
    val access=m.get("access_log").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.toMap).getOrElse(Map.empty[String,Object])
    require(access.keySet.subsetOf(Set("directory","max_bytes")),"unknown access_log setting")
    val web=NodeWebSettings.read(m.get("web"))
    val config = Config(str("host"), n("port",8815), str("public_host"), definitions,
      m("allowed_uri_prefixes").asInstanceOf[java.util.List[String]].asScala.toSeq,
      Path.of(str("tls_cert")), Path.of(str("tls_key")), Path.of(str("tls_ca")), n("batch_size",1024), n("byte_chunk",65536), n("infer_sample",4),
      Path.of(m.get("local_operator_library").map(_.toString).getOrElse("../operators")), strings("task_peers"), strings("task_input_prefixes"), n("max_concurrent_tasks",2), strings("remote_operator_library_urls"),
      Path.of(m.get("operator_cache").map(_.toString).getOrElse(".operator-cache")),
      m.get("operator_max_bytes").map(_.toString.toLong).getOrElse(8L * 1024 * 1024 * 1024), sources,
      m.get("count_file_records_on_startup").exists(_.toString.toBoolean), mode=="DEBUG", runtimes, m.get("union").map(v=>NodeUnionSettings.read(Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v))),m.get("execution_store").map {v=>val raw=v.asInstanceOf[java.util.Map[String,Object]].asScala;require(raw.keySet==Set("provider","settings"),"execution_store requires provider and settings");ExecutionStoreDefinition(raw("provider").toString,raw("settings").asInstanceOf[java.util.Map[String,Object]].asScala.toMap)},catalogDefinition,catalogOverride,Some(environment),ownsPluginEnvironment=providedPlugins.isEmpty || ownsProvidedPlugins,accessLogDirectory=access.get("directory").map(v=>Path.of(v.toString)),accessLogMaxBytes=access.get("max_bytes").map(_.toString.toLong).getOrElse(64L*1024*1024),operatorFusionEnabled=m.get("operator_fusion_enabled").map { value=>require(value.isInstanceOf[java.lang.Boolean],"operator_fusion_enabled must be a boolean");value.asInstanceOf[java.lang.Boolean].booleanValue }.getOrElse(true),web=web)
    try {config.initializeDatasets();config} catch {case scala.util.control.NonFatal(t)=>try config.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
    }catch{case scala.util.control.NonFatal(t)=>if(providedPlugins.isEmpty || ownsProvidedPlugins)try environment.close()catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
  }
}
class Producer(config: Config, allocator: BufferAllocator, tasks: Option[TaskService] = None, union: Option[UnionWorker] = None) extends NoOpFlightProducer {
  override def doExchange(ctx: FlightProducer.CallContext, reader: FlightStream, writer: FlightProducer.ServerStreamListener): Unit = {
    val audit=config.accessLog.begin("do_exchange");val out=AccessLogging.stream(audit,writer)
    try tasks match {
      case Some(service)=>service.exchange(ctx,reader,out)
      case None=>out.error(CallStatus.UNIMPLEMENTED.withDescription("task execution disabled").toRuntimeException)
    }catch{case scala.util.control.NonFatal(t)=>out.error(fail(t))}
  }
  private def adapter(ds: String, item: String, ctx: FlightProducer.CallContext): DatasetAccess = {
    def active(): Unit = if (ctx.isCancelled) throw new java.util.concurrent.CancellationException()
    config.authorize(ds,item)
    config.openDataset(ds,() => active())
  }

  private def fail(t: Throwable): FlightRuntimeException = t match {
    case e: FlightRuntimeException => e
    case _: java.util.concurrent.CancellationException => CallStatus.CANCELLED.withDescription("request cancelled").toRuntimeException
    case _: DatasetNotFound => CallStatus.NOT_FOUND.withDescription("NOT_FOUND: dataset not found; check the dataset name").toRuntimeException
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
      .addAllViews(config.plugins.views(i).asJava)
    i.schema.foreach(s => b.setArrowSchema(com.google.protobuf.ByteString.copyFrom(s.serializeAsMessage())))
    i.lastModifiedTimeMs.foreach(n => b.setLastModifiedTimeMs(n))
    i.totalSizeBytes.foreach(n => b.setTotalSizeBytes(n))
    i.recordCount.foreach(n => b.setRecordCount(n))
    b.build()
  }
  override def getFlightInfo(ctx: FlightProducer.CallContext, desc: FlightDescriptor): FlightInfo = {
    val audit=config.accessLog.begin("get_flight_info")
    try {
    val p = desc.getPath.asScala.toSeq
    require(p.size == 2 || p.size == 3 || (p.size == 4 && Set("list", "count")(p(2))), "invalid logical descriptor")
    require(p.head == "datasets", "unknown descriptor namespace")
    val ds = p(1); val item = if (p.size == 4) p(3) else p.lift(2).getOrElse("")
    audit.resource(ds,item)
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
      require(config.plugins.views(description).nonEmpty, "folder has no content stream")
      val schema = description.schema.get
      val ticket = Metadata.Ticket.newBuilder().setDataset(ds).setItem(item).setView(config.plugins.views(description).head).build()
      new FlightInfo(schema,desc,List(new FlightEndpoint(new Ticket(ticket.toByteArray))).asJava,-1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,
        itemMetadata(description).toByteArray)
    }
    } finally a.close()
  } catch { case t: Throwable => audit.finish(AccessLogging.status(t));throw fail(t) }finally audit.finish(if(ctx.isCancelled)"cancelled" else "completed")
  }
  override def listFlights(ctx:FlightProducer.CallContext,criteria:Criteria,raw:FlightProducer.StreamListener[FlightInfo]):Unit={
    val audit=config.accessLog.begin("list_flights");val listener=AccessLogging.results(audit,raw)
    try {
    def check():Unit=if(ctx.isCancelled)throw new java.util.concurrent.CancellationException()
    require(criteria.getExpression.isEmpty || java.util.Arrays.equals(criteria.getExpression,"local".getBytes(java.nio.charset.StandardCharsets.UTF_8)),"unsupported dataset directory criteria")
    config.datasetInfos(()=>check()).foreach { info=>
      check()
      val meta=DatasetDiscovery.metadata(config,info)
      listener.onNext(DatasetDiscovery.flight(meta,FlightDescriptor.path("datasets",info.name)))
    }
    listener.onCompleted()
  }catch{case scala.util.control.NonFatal(t)=>listener.onError(fail(t))}
  }
  override def doAction(ctx:FlightProducer.CallContext,action:Action,raw:FlightProducer.StreamListener[Result]):Unit={
    val audit=config.accessLog.begin("do_action");val listener=AccessLogging.results(audit,raw)
    try union match {
      case Some(worker) if action.getType.startsWith(dacp.union.Rpc.Prefix)=>worker.doAction(ctx,action,listener)
      case _=>listener.onError(CallStatus.UNIMPLEMENTED.withDescription("unknown action").toRuntimeException)
    }catch{case scala.util.control.NonFatal(t)=>listener.onError(fail(t))}
  }
  override def getStream(ctx: FlightProducer.CallContext, ticket: Ticket, raw: FlightProducer.ServerStreamListener): Unit = {
    val audit=config.accessLog.begin("get_stream");val listener=AccessLogging.stream(audit,raw)
    if(dacp.union.Rpc.isTicket(ticket.getBytes)){
      union match {case Some(worker)=>worker.getStream(ctx,ticket,listener);case None=>listener.error(CallStatus.UNIMPLEMENTED.withDescription("Union disabled").toRuntimeException)}
      audit.finish(if(ctx.isCancelled)"cancelled" else "completed")
      return
    }
    try {
      val t = Metadata.Ticket.parseFrom(ticket.getBytes)
      audit.resource(t.getDataset,t.getItem,t.getView)
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
          audit.finish(if(ctx.isCancelled || listener.isCancelled)"cancelled" else "completed")
    } catch {
      case _: java.util.concurrent.CancellationException => audit.finish("cancelled")
      case t: Throwable => audit.finish(if(Option(t.getMessage).exists(_.contains("FORBIDDEN")))"denied" else "failed");listener.error(fail(t))
    }
  }
}
object Main {
  def main(args: Array[String]): Unit = {
    if(args.headOption.contains("history")){ExecutionHistoryAdmin.main(args.drop(1));return}
    if(args.headOption.contains("union")){UnionNodeAdmin.main(args.drop(1));return}
    require(args.length<=1,"submit jobs through the shell or SDK")
    val server=DacpServer.builder().configFile(args.headOption.getOrElse("../examples/server.yaml")).build()
    val stopped=new AtomicBoolean()
    def stop():Unit=if(stopped.compareAndSet(false,true))server.close()
    val hook=new Thread(()=>stop())
    Runtime.getRuntime.addShutdownHook(hook)
    try {server.start();server.awaitTermination()}finally {
      try Runtime.getRuntime.removeShutdownHook(hook)catch{case _:IllegalStateException=>()}
      stop()
    }
  }
}
