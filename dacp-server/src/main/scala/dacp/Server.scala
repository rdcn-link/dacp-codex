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

case class Config(host: String, port: Int, publicHost: String, datasets: Map[String, Seq[PublishedItem]], prefixes: Seq[String],
                  cert: Path, key: Path, ca: Path, batchSize: Int = 1024, byteChunk: Int = 65536,
                  inferSample: Int = 4, ttlSeconds: Long = 3600, operatorLibrary: Path = Path.of("../operators"),
                  outputRoot: Path = Path.of("../examples/output"), taskPeers: Seq[String] = Seq.empty,
                  taskInputPrefixes: Seq[String] = Seq.empty, maxConcurrentTasks: Int = 2,
                  operatorLibraryUrl: Option[String] = None, operatorCache: Path = Path.of(".operator-cache"),
                  operatorMaxBytes: Long = 8L * 1024 * 1024 * 1024,
                  sqlSources: Map[String, SqlConfig] = Map.empty) {
  require(datasets.keySet.forall(n => n.nonEmpty && !n.contains("/") && n != "." && n != ".." && !n.contains("\\") && !n.exists(_.isControl)), "invalid dataset name")
  def hasDataset(name: String): Boolean = datasets.contains(name)
  require(operatorMaxBytes > 0, "operator_max_bytes must be positive")
  require(batchSize > 0 && byteChunk > 0 && inferSample > 0 && ttlSeconds > 0 && maxConcurrentTasks > 0)
  def uri(ds: String, item: String = ""): String = new URI("dacp", null, publicHost, port, "/" + ds + item, null, null).toASCIIString
  def authorize(ds: String, item: String): Unit = {
    val candidate = uri(ds, item)
    // Connection identity comes from mandatory mTLS. This node-wide allowlist is URI-segment bounded.
    require(prefixes.exists { p => val clean = p.stripSuffix("/"); candidate == clean || candidate.startsWith(clean + "/") }, "FORBIDDEN: URI prefix denied")
  }
}
object Config {
  def load(path: Path): Config = {
    Formats.initialize()
    val in = Files.newInputStream(path)
    val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
    val m = try new Yaml(options).load[java.util.Map[String,Object]](in).asScala finally in.close()
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
    val published = m.get("datasets").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.map { case (name, raw) =>
      require(raw.isInstanceOf[java.util.Map[_,_]], "datasets must use explicit logical item mappings")
      val dataset = raw.asInstanceOf[java.util.Map[String,Object]].asScala
      val items = dataset("items").asInstanceOf[java.util.Map[String,java.util.Map[String,Object]]].asScala.map { case (logical, mapping) =>
        val v = mapping.asScala
        PublishedItem(logical, v.get("file").map(x => Path.of(x.toString)), v.get("sql").map(_.toString),
          v.get("table").map(_.toString), v.get("kind").map(_.toString).getOrElse("data"),
          FileOptions(v.get("format").map(_.toString),v.get("sheet").map(_.toString),
            v.get("header_row").map(_.toString.toInt).getOrElse(1),
            v.get("schema").map { raw =>
              val columns = raw.asInstanceOf[java.util.List[java.util.Map[String,String]]].asScala
              new org.apache.arrow.vector.types.pojo.Schema(columns.map(c => Data.field(c.get("name"),c.get("type"))).asJava)
            }))
      }.toVector
      name -> items
    }.toMap).getOrElse(Map.empty[String,Seq[PublishedItem]])
    require(!m.contains("sql_datasets"), "use private sql_sources and explicit datasets item mappings")
    val config = Config(str("host"), n("port",8815), str("public_host"), published,
      m("allowed_uri_prefixes").asInstanceOf[java.util.List[String]].asScala.toSeq,
      Path.of(str("tls_cert")), Path.of(str("tls_key")), Path.of(str("tls_ca")), n("batch_size",1024), n("byte_chunk",65536), n("infer_sample",4),
      n("output_ttl_seconds",3600).toLong, Path.of(m.get("operator_library").map(_.toString).getOrElse("../operators")), Path.of(str("output_root")), strings("task_peers"), strings("task_input_prefixes"), n("max_concurrent_tasks",2), m.get("operator_library_url").map(_.toString),
      Path.of(m.get("operator_cache").map(_.toString).getOrElse(".operator-cache")),
      m.get("operator_max_bytes").map(_.toString.toLong).getOrElse(8L * 1024 * 1024 * 1024), sql)
    published.values.foreach(items => new NamespaceAdapter(items,config))
    config
  }
}
class Producer(config: Config, allocator: BufferAllocator, outputs: OutputRegistry, tasks: Option[TaskService] = None) extends NoOpFlightProducer {
  override def listActions(ctx: FlightProducer.CallContext, listener: FlightProducer.StreamListener[ActionType]): Unit = {
    tasks.foreach { _ =>
      listener.onNext(new ActionType(TaskService.ExecuteAction,"Execute one task on this node"))
      listener.onNext(new ActionType(TaskService.JobAction,"Coordinate a distributed YAML DAG"))
    }
    listener.onCompleted()
  }
  override def doAction(ctx: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    tasks match {
      case Some(service) => service.handle(action,listener)
      case None => listener.onError(CallStatus.UNIMPLEMENTED.withDescription("task execution disabled").toRuntimeException)
    }
  }
  private def adapter(ds: String, item: String, ctx: FlightProducer.CallContext): NamespaceAdapter = {
    def active(): Unit = if (ctx.isCancelled) throw new java.util.concurrent.CancellationException()
    val output = outputs.lookup(ds)
    if (output.isEmpty && !config.hasDataset(ds))
      throw CallStatus.NOT_FOUND.withDescription("NOT_FOUND: dataset not found; check the dataset name").toRuntimeException
    config.authorize(ds,item)
    output match {
      case Some(base) => NamespaceAdapter.output(base,config,() => active())
      case None => new NamespaceAdapter(config.datasets(ds),config,() => active())
    }
  }

  private def fail(t: Throwable): FlightRuntimeException = t match {
    case e: FlightRuntimeException => e
    case e: Expired => CallStatus.NOT_FOUND.withDescription(e.getMessage).toRuntimeException
    case e: java.io.FileNotFoundException => CallStatus.NOT_FOUND.withDescription("NOT_FOUND: logical resource unavailable").toRuntimeException
    case e: java.nio.file.NoSuchFileException => CallStatus.NOT_FOUND.withDescription("NOT_FOUND: logical resource unavailable").toRuntimeException
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
    if (p.size == 4 && p(2) == "count") {
      val metadata = itemMetadata(Item(item,"",None,recordCount=a.count(item)))
      new FlightInfo(Data.bytesSchema, desc, java.util.Collections.emptyList[FlightEndpoint](), -1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,metadata.toByteArray)
    } else if (p.size == 2 || p.size == 4) {
      val meta = Metadata.DataSet.newBuilder().setUri(config.uri(ds))
      // Permit item-specific prefixes while keeping list itself restricted by the dataset prefix.
      (if (p.size == 4) a.list(item) else a.list()).filter(i =>
        scala.util.Try(config.authorize(ds,i.path)).isSuccess).foreach { i =>
        meta.addItems(itemMetadata(i))
      }
      new FlightInfo(Data.bytesSchema, desc, java.util.Collections.emptyList[FlightEndpoint](), -1,-1,false,org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,meta.build.toByteArray)
    } else {
      val description = a.describe(item)
      require(NamespaceAdapter.views(description).nonEmpty, "folder has no content stream")
      val schema = description.schema.get
      val ticket = Metadata.Ticket.newBuilder().setDataset(ds).setItem(item).setView(NamespaceAdapter.views(description).head).build()
      new FlightInfo(schema,desc,List(new FlightEndpoint(new Ticket(ticket.toByteArray))).asJava,-1,-1,false,
        org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,
        itemMetadata(description).toByteArray)
    }
  } catch { case t: Throwable => throw fail(t) }
  override def getStream(ctx: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    try {
      val t = Metadata.Ticket.parseFrom(ticket.getBytes)
      require(Set("records","bytes").contains(t.getView), "invalid view")
      val namespace = adapter(t.getDataset,t.getItem,ctx)
      val lock = new Object
      listener.setOnReadyHandler(() => lock.synchronized(lock.notifyAll()))
      listener.setOnCancelHandler(() => lock.synchronized(lock.notifyAll()))
      def ready(): Unit = lock.synchronized {
        while (!listener.isReady && !listener.isCancelled) lock.wait(100)
        if (listener.isCancelled) throw new java.util.concurrent.CancellationException()
        // Expiry invalidates active streams too, before reading the next batch.
        outputs.lookup(t.getDataset)
      }
      namespace.withResolved(t.getItem, () => ready()) { (source, internalItem) =>
      val recordsSupported = source match {
        case _: SqlAdapter => true
        case d: DirectoryAdapter => Formats.detect(d.resolve(internalItem),d.options).records
      }
      require(t.getView == "bytes" || recordsSupported, "unsupported stream view")
      source match {
        case sql: SqlAdapter =>
          ready()
          if (t.getView == "records") sql.stream(internalItem,allocator)(root => listener.start(root),root => listener.putNext(),() => ready())
          else SqlAdapter.bytes(sql,internalItem,allocator,config.byteChunk,listener,() => ready())
          listener.completed()
          return
        case _ => ()
      }
      val p = source.asInstanceOf[DirectoryAdapter].resolve(internalItem)
      val bytes = t.getView == "bytes"
      if (bytes) {
        val root = VectorSchemaRoot.create(Data.bytesSchema,allocator)
        val in = Files.newInputStream(p)
        try {
          listener.start(root)
          val buffer = new Array[Byte](config.byteChunk)
          var done = false
          while (!done) { ready(); val n = in.read(buffer)
            if (n < 0) done = true else {
              root.clear(); root.allocateNew(); root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,buffer,0,n)
              root.setRowCount(1); listener.putNext()
            }
          }
        } finally { in.close(); root.close() }
      } else {
        val options = source.asInstanceOf[DirectoryAdapter].options
        Formats.detect(p,options).stream(p,options,allocator,config.batchSize,config.inferSample)(
          root => listener.start(root), root => listener.putNext(), () => ready())
      }
      listener.completed()
      }
    } catch { case _: java.util.concurrent.CancellationException => (); case t: Throwable => listener.error(fail(t)) }
  }
}
object Main {
  def main(args: Array[String]): Unit = {
    if (args.headOption.contains("operator")) {
      val a = new RootAllocator(Long.MaxValue)
      try sys.env.getOrElse("DACP_OPERATOR","format_convert@1.0") match {
        case "format_convert@1.0" =>
          FormatConvert.run(Path.of(args(1)),Path.of(args(2)),Path.of(args(3)))
          OperatorOutput.encode(Path.of(args(2)),a)
        case "identity@1.0" => Identity.run(Path.of(args(1)),Path.of(args(2)),a)
        case op => throw new IllegalArgumentException("unknown built-in operator " + op)
      } finally a.close()
      return
    }
    val config = Config.load(Path.of(args.headOption.getOrElse("../examples/server.yaml")))
    val allocator = new RootAllocator(Long.MaxValue)
    val outputs = new OutputRegistry(config.ttlSeconds * 1000)
    val executor = new TaskExecutor(config,allocator,outputs)
    val tasks = new TaskService(config,allocator,executor.execute)
    val producer = new Producer(config,allocator,outputs,Some(tasks))
    val server = FlightServer.builder(allocator,Location.forGrpcTls(config.host,config.port),producer)
      .useTls(config.cert.toFile,config.key.toFile).useMTlsClientVerification(config.ca.toFile)
      .build().start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => { server.close(); tasks.close(); outputs.close(); allocator.close() }))
    println(s"dacp listening on ${config.uri("")}")
    val datasetNames = config.datasets.keySet.toSeq.sorted
    println(s"configured datasets (${datasetNames.size}):")
    datasetNames.foreach { name =>
      println(s"  $name ${config.uri(name)}")
    }
    val jobFailure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    if (args.length > 1) {
      val thread = new Thread(() => try { val result = tasks.runJob(Files.readString(Path.of(args(1)))); println("job outputs: " + result) } catch { case e: Throwable => jobFailure.set(e); e.printStackTrace(); server.close() }, "dacp-job")
      thread.start()
    }
    server.awaitTermination()
    Option(jobFailure.get()).foreach(e => throw new IllegalStateException("job failed",e))
  }
}
