package dacp

import _root_.xstream.data._

import java.nio.file.Path
import org.apache.arrow.flight.{FlightServer,Location}
import org.apache.arrow.memory.RootAllocator
import scala.util.control.NonFatal

/** Embedded server: owns its Config/catalog, but never installs JVM hooks or closes shared plugin loaders. */
final class DacpServer private(config:Config) extends AutoCloseable {
  private var state="built"
  private var allocator:RootAllocator=null
  private var tasks:TaskService=null
  private var engine:JobEngine=null
  def jobEngine:JobEngine=synchronized {require(engine!=null,"server is not started");engine}
  private var union:Option[UnionWorker]=None
  private var flight:FlightServer=null
  private var flightThreads:java.util.concurrent.ExecutorService=null

  @volatile private var s3:S3Service=null
  def s3Port:Option[Int]=synchronized {Option(s3).map(_.port)}
  private var dashboard:NodeDashboard=null
  private var startedAt=0L
  def webPort:Option[Int]=synchronized {Option(dashboard).map(_.port)}

  def start():DacpServer=synchronized {
    require(state=="built","server can only be started once")
    try {
      config.initializeDatasets()
      allocator=new RootAllocator(Long.MaxValue)
      config.accessLog
      config.executionHistory
      engine=new JobEngine(config,allocator)
      tasks=new TaskService(config,allocator,engine.executor,Some(engine))
      union=config.union.map(s=>new UnionWorker(config,s,allocator))
      flightThreads=java.util.concurrent.Executors.newFixedThreadPool(128)
      flight=dacp.union.Rpc.secure(FlightServer.builder(allocator,Location.forGrpcTls(config.host,config.port),new Producer(config,allocator,Some(tasks),union)),new dacp.union.Tls(config.cert,config.key,config.ca),Some(config.credentials))
        .maxInboundMessageSize(dacp.union.Rpc.MaxMessage).executor(flightThreads).build()
      flight.start();union.foreach(_.start());startedAt=System.currentTimeMillis()
      if(config.web.enabled){dashboard=new NodeDashboard(config,flight.getPort,startedAt,()=>union.toVector.flatMap(_.executionSnapshots),()=>Option(s3).map(_.snapshot).getOrElse(dacp.union.Json.obj().put("enabled",config.s3.enabled).put("state",if(config.s3.enabled)"starting"else "disabled")));dashboard.start();config.runningWebUrl=Some(dashboard.publicUrl)}
      if(config.s3.enabled){s3=new S3Service(config,flight.getPort,allocator);s3.start()}
      state="started"
      val log=org.slf4j.LoggerFactory.getLogger("dacp.Main")
      log.debug(s"remote_operator_library_urls (${config.remoteOperatorLibraryUrls.size}):")
      config.remoteOperatorLibraryUrls.zipWithIndex.foreach {case(url,index)=>log.debug(s"  [$index] $url")}
      log.info(s"dacp listening on ${config.uri("")}")
      this
    }catch{case NonFatal(t)=>try close()catch{case NonFatal(e)=>t.addSuppressed(e)};throw t}
  }
  def boundPort:Int=synchronized {require(state=="started","server is not running");flight.getPort}
  def location:Location=Location.forGrpcTls(config.publicHost,config.publicPort.getOrElse(boundPort))
  def awaitTermination():Unit={
    val running=synchronized {require(state=="started","server is not running");flight}
    running.awaitTermination()
  }
  def close():Unit=synchronized {
    if(state!="closed") {
      state="closed"
      config.runningWebUrl=None
      var failure:Throwable=null
      val resources=Option(s3).toSeq++Option(dashboard).toSeq++union.toSeq++Option(tasks).toSeq++Option(flight).toSeq++Option(flightThreads).toSeq.map(pool=>new AutoCloseable {
        def close():Unit={pool.shutdownNow();require(pool.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS),"Flight handlers failed to stop")}
      })++Option(engine).toSeq++Seq(config)++Option(allocator).toSeq
      resources.foreach(r=>try r.close()catch{case NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)})
      if(failure!=null)throw failure
    }
  }
}
object DacpServer {
  def builder():Builder=new Builder
  final class Builder private[DacpServer]() {
    private var file:Option[Path]=None
    private var supplied:Option[Config]=None
    private var catalogInstance:Option[DataCatalog]=None
    private var pluginInstance:Option[(PluginEnvironment,Boolean)]=None
    def plugins(value:PluginEnvironment,owned:Boolean=false):Builder={require(!built && value!=null,"invalid plugins or reused builder");pluginInstance=Some(value->owned);this}
    private var built=false
    def configFile(path:String):Builder=configFile(Path.of(path))
    def configFile(path:Path):Builder={require(!built,"builder already used");file=Some(path);supplied=None;this}
    def config(value:Config):Builder={require(!built,"builder already used");supplied=Some(value);file=None;this}
    def catalog(value:DataCatalog):Builder={require(!built && value!=null,"invalid catalog or reused builder");catalogInstance=Some(value);this}
    def build():DacpServer={
      require(!built,"builder already used")
      require(file.isDefined || supplied.isDefined,"configFile or config required")
      val config=supplied match {
        case Some(c)=>catalogInstance.foreach(c.installCatalog);pluginInstance.foreach{case(p,owned)=>c.installPlugins(p,owned)};c
        case None=>Config.load(file.get,catalogInstance,pluginInstance.map(_._1),pluginInstance.exists(_._2))
      }
      built=true
      new DacpServer(config)
    }
  }
}
