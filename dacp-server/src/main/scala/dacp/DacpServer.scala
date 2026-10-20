package dacp

import java.nio.file.Path
import org.apache.arrow.flight.{FlightServer,Location}
import org.apache.arrow.memory.RootAllocator
import scala.util.control.NonFatal

/** Embedded server: owns its Config/catalog, but never installs JVM hooks or closes shared plugin loaders. */
final class DacpServer private(config:Config) extends AutoCloseable {
  private var state="built"
  private var allocator:RootAllocator=null
  private var tasks:TaskService=null
  private var union:Option[UnionWorker]=None
  private var flight:FlightServer=null

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
      tasks=new TaskService(config,allocator,new TaskExecutor(config,allocator))
      union=config.union.map(s=>new UnionWorker(config,s,allocator))
      flight=dacp.union.Rpc.secure(FlightServer.builder(allocator,Location.forGrpcTls(config.host,config.port),new Producer(config,allocator,Some(tasks),union)),new dacp.union.Tls(config.cert,config.key,config.ca))
        .maxInboundMessageSize(dacp.union.Rpc.MaxMessage).build()
      flight.start();union.foreach(_.start());startedAt=System.currentTimeMillis()
      if(config.web.enabled){dashboard=new NodeDashboard(config,flight.getPort,startedAt,()=>union.toVector.flatMap(_.executionSnapshots));dashboard.start()}
      state="started"
      val log=org.slf4j.LoggerFactory.getLogger("dacp.Main")
      log.debug(s"remote_operator_library_urls (${config.remoteOperatorLibraryUrls.size}):")
      config.remoteOperatorLibraryUrls.zipWithIndex.foreach {case(url,index)=>log.debug(s"  [$index] $url")}
      log.info(s"dacp listening on ${config.uri("")}")
      this
    }catch{case NonFatal(t)=>try close()catch{case NonFatal(e)=>t.addSuppressed(e)};throw t}
  }
  def boundPort:Int=synchronized {require(state=="started","server is not running");flight.getPort}
  def location:Location=Location.forGrpcTls(config.publicHost,boundPort)
  def awaitTermination():Unit={
    val running=synchronized {require(state=="started","server is not running");flight}
    running.awaitTermination()
  }
  def close():Unit=synchronized {
    if(state!="closed") {
      state="closed"
      var failure:Throwable=null
      val resources=Option(dashboard).toSeq++union.toSeq++Option(tasks).toSeq++Option(flight).toSeq++Seq(config)++Option(allocator).toSeq
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
    private var catalogInstance:Option[DatasetCatalog]=None
    private var pluginInstance:Option[(PluginEnvironment,Boolean)]=None
    def plugins(value:PluginEnvironment,owned:Boolean=false):Builder={require(!built && value!=null,"invalid plugins or reused builder");pluginInstance=Some(value->owned);this}
    private var built=false
    def configFile(path:String):Builder=configFile(Path.of(path))
    def configFile(path:Path):Builder={require(!built,"builder already used");file=Some(path);supplied=None;this}
    def config(value:Config):Builder={require(!built,"builder already used");supplied=Some(value);file=None;this}
    def catalog(value:DatasetCatalog):Builder={require(!built && value!=null,"invalid catalog or reused builder");catalogInstance=Some(value);this}
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
