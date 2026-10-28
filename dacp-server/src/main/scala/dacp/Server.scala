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

case class Config(host: String, port: Int, publicHost: String, datasets: Map[String, DatasetDefinition],
                  cert: Path, key: Path, ca: Path, batchSize: Int = 1024, byteChunk: Int = 65536,
                  inferSample: Int = 4, localOperatorLibrary: Path = Path.of("../operators"),
                  taskPeers: Seq[String] = Seq.empty,
                  taskInputPrefixes: Seq[String] = Seq.empty, maxConcurrentTasks: Int = 2,
                  remoteOperatorLibraryUrls: Seq[String] = Seq.empty, operatorCache: Path = Path.of(".operator-cache"),
                  operatorMaxBytes: Long = 8L * 1024 * 1024 * 1024,
                  sources: Map[String, SourceDefinition] = Map.empty, countFileRecordsOnStartup: Boolean = false, debug: Boolean = false, sourceRuntimes: Map[String, SourceRuntime] = Map.empty, union: Option[NodeUnionSettings] = None, executionStore: Option[ExecutionStoreDefinition] = None,
                  datasetCatalog: Option[DatasetCatalogDefinition] = None, catalogOverride: Option[DatasetCatalog] = None, pluginEnvironment: Option[PluginEnvironment] = None, pluginDirectory: Option[Path] = None, ownsPluginEnvironment:Boolean=false, accessLogDirectory:Option[Path]=None, accessLogMaxBytes:Long=64L*1024*1024, operatorFusionEnabled:Boolean=true, web:NodeWebSettings=NodeWebSettings(), materializationStore:Option[MaterializationStoreDefinition]=None, executionTimeoutSeconds:Long=1800, s3:S3Settings=S3Settings(),credentialDirectory:Option[Path]=None,providedCredentials:Option[dacp.union.Credentials]=None,inferenceWorkers:InferenceWorkerSettings=InferenceWorkerSettings()) extends CatalogContext with OperatorEnvironment with AutoCloseable {
  require(executionTimeoutSeconds>0 && executionTimeoutSeconds<=86400,"invalid execution_timeout_seconds")
  lazy val credentials=providedCredentials.getOrElse(new dacp.union.Credentials(credentialDirectory.getOrElse(Path.of(".dacp-security",port.toString))))
  lazy val internalToken:String=credentials.internalToken()
  @volatile private[dacp] var runningWebUrl:Option[String]=None
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
  private var inferencePool:InferenceWorkerPool=null
  private[dacp] def inferenceWorkerPool:InferenceWorkerPool=synchronized {
    require(!closed.get(),"configuration is closed")
    if(inferencePool==null)inferencePool=new InferenceWorkerPool(inferenceWorkers)
    inferencePool
  }
  private val materializationStores=scala.collection.mutable.Map.empty[MaterializationStoreDefinition,MaterializationStore]
  private[dacp] def openMaterialization(d:MaterializationStoreDefinition):MaterializationStore=synchronized {
    require(!closed.get(),"configuration is closed")
    materializationStores.getOrElseUpdate(d,plugins.byId(classOf[MaterializationStoreProvider],d.provider).open(d.settings))
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
    val owned=synchronized {Option(inferencePool).toSeq++Option(activeCatalog).orElse(suppliedCatalog).toSeq++Option(historyStore).toSeq++materializationStores.values.toSeq++Option(accessLogger).toSeq++(if(ownsPlugins)Option(activePlugins).orElse(suppliedPlugins).toSeq else Seq.empty)}
    owned.foreach(p=>try p.close() catch{case scala.util.control.NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)})
    if(providedCredentials.isEmpty)credentials.close()
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
  def datasetDirectory(check:()=>Unit=()=>()):DatasetDirectory={
    val snapshot=catalog.directory(check)
    val result=snapshot.entries.toVector
    result.foreach { i=>DatasetNames.validate(i.name);DatasetDescriptions.validate(i.description) }
    require(result.map(_.name).distinct.size==result.size,"duplicate dataset name")
    snapshot.copy(entries=result.sortBy(_.name))
  }
  def datasetInfos(check:()=>Unit=()=>()):Seq[DatasetInfo]={
    val snapshot=datasetDirectory(check)
    if(!snapshot.complete)throw CallStatus.UNAVAILABLE.withDescription("dataset directory incomplete; use dacp.catalog for partial results").toRuntimeException
    snapshot.entries
  }
  def openDataset(name:String,check:()=>Unit=()=>()):DatasetAccess={
    DatasetNames.validate(name);check()
    val lease=try catalog.open(name,check) catch{case _:java.io.FileNotFoundException=>throw new DatasetNotFound}
    val access=try {check();ItemByteViews.wrap(lease.open(check),i=>plugins.views(i),byteChunk)}catch{case scala.util.control.NonFatal(t)=>try lease.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
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

}
object Config {
  def load(path: Path, catalogOverride:Option[DatasetCatalog]=None,providedPlugins:Option[PluginEnvironment]=None,ownsProvidedPlugins:Boolean=false): Config = {
    val in = Files.newInputStream(path)
    val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val m = try new Yaml(options).load[java.util.Map[String,Object]](in).asScala finally in.close()
    val settings = Set("host", "port", "public_host", "datasets", "sources",
      "tls_cert", "tls_key", "tls_ca", "credential_directory", "batch_size", "byte_chunk", "infer_sample",
      "inference_workers", "operator_fusion_enabled", "local_operator_library", "task_peers", "task_input_prefixes", "max_concurrent_tasks",
      "remote_operator_library_urls", "operator_cache", "operator_max_bytes", "count_file_records_on_startup", "plugin_directory", "mode", "source_runtimes", "union", "execution_store", "dataset_catalog", "access_log", "web", "s3", "materialization_store", "execution_timeout_seconds")
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
      Path.of(str("tls_cert")), Path.of(str("tls_key")), Path.of(str("tls_ca")), n("batch_size",1024), n("byte_chunk",65536), n("infer_sample",4),
      Path.of(m.get("local_operator_library").map(_.toString).getOrElse("../operators")), strings("task_peers"), strings("task_input_prefixes"), n("max_concurrent_tasks",2), strings("remote_operator_library_urls"),
      Path.of(m.get("operator_cache").map(_.toString).getOrElse(".operator-cache")),
      m.get("operator_max_bytes").map(_.toString.toLong).getOrElse(8L * 1024 * 1024 * 1024), sources,
      m.get("count_file_records_on_startup").exists(_.toString.toBoolean), mode=="DEBUG", runtimes, m.get("union").map(v=>NodeUnionSettings.read(Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v))),m.get("execution_store").map {v=>val raw=v.asInstanceOf[java.util.Map[String,Object]].asScala;require(raw.keySet==Set("provider","settings"),"execution_store requires provider and settings");ExecutionStoreDefinition(raw("provider").toString,raw("settings").asInstanceOf[java.util.Map[String,Object]].asScala.toMap)},catalogDefinition,catalogOverride,Some(environment),ownsPluginEnvironment=providedPlugins.isEmpty || ownsProvidedPlugins,accessLogDirectory=access.get("directory").map(v=>Path.of(v.toString)),accessLogMaxBytes=access.get("max_bytes").map(_.toString.toLong).getOrElse(64L*1024*1024),operatorFusionEnabled=m.get("operator_fusion_enabled").map { value=>require(value.isInstanceOf[java.lang.Boolean],"operator_fusion_enabled must be a boolean");value.asInstanceOf[java.lang.Boolean].booleanValue }.getOrElse(true),web=web,inferenceWorkers=InferenceWorkerSettings.read(m.get("inference_workers")),s3=S3Settings.read(m.get("s3")),credentialDirectory=m.get("credential_directory").map(v=>Path.of(v.toString)),executionTimeoutSeconds=m.get("execution_timeout_seconds").map(v=>dacp.union.Json.number(Data.mapper.createObjectNode().set[com.fasterxml.jackson.databind.JsonNode]("timeout",Data.mapper.valueToTree(v)),"timeout",1,86400)).getOrElse(1800L),materializationStore=m.get("materialization_store").map { v=>
        val n=Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v)
        dacp.union.Json.fields(n,Set("provider","settings"));require(n.path("settings").isObject,"materialization settings must be object")
        MaterializationStoreDefinition(dacp.union.Json.text(n,"provider"),Data.mapper.convertValue(n.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap)
      })
    try {config.initializeDatasets();config} catch {case scala.util.control.NonFatal(t)=>try config.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
    }catch{case scala.util.control.NonFatal(t)=>if(providedPlugins.isEmpty || ownsProvidedPlugins)try environment.close()catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
  }
}
class Producer(config: Config, allocator: BufferAllocator, tasks: Option[TaskService] = None, union: Option[UnionWorker] = None) extends NoOpFlightProducer {
  override def doExchange(ctx: FlightProducer.CallContext, reader: FlightStream, writer: FlightProducer.ServerStreamListener): Unit = {
    dacp.union.Rpc.clientOperation()
    val audit=config.accessLog.begin("do_exchange");val out=AccessLogging.stream(audit,writer)
    try tasks match {
      case Some(service)=>service.exchange(ctx,reader,out)
      case None=>out.error(CallStatus.UNIMPLEMENTED.withDescription("task execution disabled").toRuntimeException)
    }catch{case scala.util.control.NonFatal(t)=>out.error(fail(t))}
  }
  protected def inferTask(task:Task,inputs:Map[String,StreamDescriptor],cancel:StreamCancel):InferenceResult=WorkflowInference.task(config,allocator,task,inputs,cancel)
  private def adapter(ds: String, item: String, ctx: FlightProducer.CallContext): DatasetAccess = {
    val serviceLease=if(dacp.union.Rpc.isAuthenticatedCall && dacp.union.Rpc.principal.kind=="union")Some(dacp.union.Rpc.principal.active)else None
    def active(): Unit = {
      if(ctx.isCancelled)throw new java.util.concurrent.CancellationException()
      if(serviceLease.exists(check=> !check()))throw CallStatus.UNAUTHORIZED.withDescription("Union credential revoked or expired").toRuntimeException
    }
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
    dacp.union.Rpc.clientOperation()
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
      val publicInfo=config.datasetDirectory(()=>if(ctx.isCancelled)throw new java.util.concurrent.CancellationException()).entries.find(_.name==ds).getOrElse(throw new DatasetNotFound)
      val meta = DatasetDiscovery.metadata(config,publicInfo).toBuilder
      (if (p.size == 4) a.list(item) else a.list()).foreach { i =>
        meta.addItems(itemMetadata(i))
      }
      new FlightInfo(PluginSchemas.bytes, desc, java.util.Collections.emptyList[FlightEndpoint](), -1,-1,false,org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,meta.build.toByteArray)
    } else {
      val description = a.describe(item)
      require(config.plugins.views(description).nonEmpty, "folder has no content stream")
      val schema = description.schema.getOrElse {require(config.plugins.views(description).head=="bytes","records view requires schema");PluginSchemas.bytes}
      val ticket = Metadata.Ticket.newBuilder().setDataset(ds).setItem(item).setView(config.plugins.views(description).head).build()
      new FlightInfo(schema,desc,List(new FlightEndpoint(new Ticket(ticket.toByteArray))).asJava,-1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,
        itemMetadata(description).toByteArray)
    }
    } finally a.close()
  } catch { case t: Throwable => audit.finish(AccessLogging.status(t));throw fail(t) }finally audit.finish(if(ctx.isCancelled)"cancelled" else "completed")
  }
  override def listFlights(ctx:FlightProducer.CallContext,criteria:Criteria,raw:FlightProducer.StreamListener[FlightInfo]):Unit={
    dacp.union.Rpc.clientOperation()
    val audit=config.accessLog.begin("list_flights");val listener=AccessLogging.results(audit,raw)
    try {
    def check():Unit=if(ctx.isCancelled)throw new java.util.concurrent.CancellationException()
    require(criteria.getExpression.isEmpty,"unsupported dataset directory criteria")
    config.datasetInfos(()=>check()).foreach { info=>
      check()
      val meta=DatasetDiscovery.metadata(config,info)
      listener.onNext(DatasetDiscovery.flight(meta,FlightDescriptor.path("datasets",info.name)))
    }
    listener.onCompleted()
  }catch{case scala.util.control.NonFatal(t)=>listener.onError(fail(t))}
  }
  override def doAction(ctx:FlightProducer.CallContext,action:Action,raw:FlightProducer.StreamListener[Result]):Unit={
    if(!action.getType.startsWith(dacp.union.Rpc.Prefix))dacp.union.Rpc.clientOperation()
    val audit=config.accessLog.begin("do_action");val listener=AccessLogging.results(audit,raw)
    try if(action.getType=="dacp.catalog") {
      require(action.getBody.isEmpty,"catalog action takes no body")
      val snapshot=config.datasetDirectory(()=>if(ctx.isCancelled)throw new java.util.concurrent.CancellationException())
      val directory=Metadata.Directory.newBuilder().setComplete(snapshot.complete).setUnavailableSources(snapshot.unavailableSources)
      snapshot.entries.foreach(i=>directory.addDatasets(DatasetDiscovery.metadata(config,i)))
      listener.onNext(new Result(directory.build().toByteArray));listener.onCompleted()
    } else if(Set("dacp.infer","dacp.infer_job")(action.getType)) {
      require(action.getBody.length<=StructureInference.MaxBytes,"inference request exceeds 1 MiB")
      val n=Data.mapper.readTree(action.getBody)
      val c=new StreamCancel(()=>ctx.isCancelled,timeoutSeconds=120)
      try {
        val result=if(action.getType=="dacp.infer") {
          val t=WorkflowInference.parseTask(n)
          StructureInference.json(inferTask(t,StructureInference.ports(n.path("inputs")),c))
        }else {
          require(n.isObject && n.size==1 && n.path("job_yaml").isTextual,"expected job_yaml")
          WorkflowInference.plan(config,allocator,Jobs.parse(n.path("job_yaml").asText),c)
        }
        val bytes=Data.mapper.writeValueAsBytes(result);require(bytes.length<=StructureInference.MaxBytes,"inference result exceeds 1 MiB")
        listener.onNext(new Result(bytes));listener.onCompleted()
      }catch{
        case t:java.util.concurrent.TimeoutException=>
          org.slf4j.LoggerFactory.getLogger(classOf[Producer]).warn("structure inference timed out",t)
          throw CallStatus.TIMED_OUT.withDescription(t.getMessage).toRuntimeException
        case scala.util.control.NonFatal(t)=>
        org.slf4j.LoggerFactory.getLogger(classOf[Producer]).warn("structure inference failed",t);throw t
      }finally c.close()
    } else union match {
      case Some(worker) if action.getType.startsWith(dacp.union.Rpc.Prefix)=>worker.doAction(ctx,action,listener)
      case _=>listener.onError(CallStatus.UNIMPLEMENTED.withDescription("unknown action").toRuntimeException)
    }catch{case scala.util.control.NonFatal(t)=>listener.onError(fail(t))}
  }
  override def getStream(ctx: FlightProducer.CallContext, ticket: Ticket, raw: FlightProducer.ServerStreamListener): Unit = {
    if(!dacp.union.Rpc.isTicket(ticket.getBytes))dacp.union.Rpc.clientOperation()
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
      val serviceLease=if(dacp.union.Rpc.isAuthenticatedCall && dacp.union.Rpc.principal.kind=="union")Some(dacp.union.Rpc.principal.active)else None
      def ready(): Unit = lock.synchronized {
        if(serviceLease.exists(check=> !check()))throw CallStatus.UNAUTHORIZED.withDescription("Union credential revoked or expired").toRuntimeException
        while (!listener.isReady && !listener.isCancelled) {if(serviceLease.exists(check=> !check()))throw CallStatus.UNAUTHORIZED.withDescription("Union credential revoked or expired").toRuntimeException;lock.wait(100)}
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
    if(args.headOption.contains("credentials")){dacp.union.CredentialAdmin.main(args.drop(1));return}
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
