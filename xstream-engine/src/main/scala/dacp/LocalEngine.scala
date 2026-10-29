package dacp

import java.nio.file.Path
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.memory.{BufferAllocator,RootAllocator}

/** Standalone engine host. Remote Item access is supplied as an adapter, never a server dependency. */
final class LocalEngine(settings:JsonNode,val plugins:PluginEnvironment,
  remote:(InputBinding,BufferAllocator,StreamCancel)=>FrameSource=(_,_,_)=>throw new IllegalArgumentException("No remote Item adapter configured"),
  remoteCompute:Option[ComputePushdown]=None) extends EngineEnvironment with AutoCloseable {
  private val catalogHost=new ConfiguredDataCatalog(settings,plugins)
  val allocator=new RootAllocator(Long.MaxValue)
  val engine=new JobEngine(this,allocator)
  require(!settings.has("compute_pushdown_enabled") || settings.path("compute_pushdown_enabled").isBoolean,"compute_pushdown_enabled must be boolean")
  override val computePushdown:Option[ComputePushdown]=if(settings.path("compute_pushdown_enabled").asBoolean(true))remoteCompute.orElse(Some(new DacpComputePushdown(settings)))else None
  def debug:Boolean=false
  def sourceRuntimes:Map[String,SourceRuntime]=Map.empty
  def operatorFusionEnabled:Boolean=settings.path("operator_fusion_enabled").asBoolean(true)
  def maxConcurrentTasks:Int=settings.path("max_concurrent_tasks").asInt(2)
  private[dacp] val taskSlots=new java.util.concurrent.Semaphore(maxConcurrentTasks)
  def executionTimeoutSeconds:Long=settings.path("execution_timeout_seconds").asLong(1800)
  def localOperatorLibrary:Path=Path.of(settings.path("operator_cache").asText(".xstream-operators"),"library")
  def operatorCache:Path=Path.of(settings.path("operator_cache").asText(".xstream-operators"))
  def operatorMaxBytes:Long=settings.path("operator_max_bytes").asLong(8L*1024*1024*1024)
  def remoteOperatorLibraryUrls:Seq[String]={import scala.jdk.CollectionConverters._;settings.path("remote_operator_library_urls").elements.asScala.map(_.asText).toVector}
  private[dacp] val inferenceWorkerPool=new InferenceWorkerPool(InferenceWorkerSettings())
  private val stores=scala.collection.mutable.Map.empty[MaterializationStoreDefinition,MaterializationStore]
  def materializationStore:Option[MaterializationStoreDefinition]=Option(settings.get("materialization_store")).map {n=>import scala.jdk.CollectionConverters._;MaterializationStoreDefinition(n.path("provider").asText,Data.mapper.convertValue(n.path("settings"),classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap)}
  private[dacp] def openMaterialization(d:MaterializationStoreDefinition):MaterializationStore=synchronized {stores.getOrElseUpdate(d,plugins.byId(classOf[MaterializationStoreProvider],d.provider).open(d.settings))}
  val executionHistory=new ExecutionHistory(plugins.byId(classOf[ExecutionStoreProvider],"file").open(Map("directory"->settings.path("execution_directory").asText(".xstream-executions")),false))
  def openItem(a:BufferAllocator,b:InputBinding,c:StreamCancel):FrameSource=if(b.uri.exists(_.startsWith("context:")))CatalogItemSource.open(engine.dataCatalog,plugins,a,b,c)else remote(b,a,c)
  def close():Unit={try {engine.close();inferenceWorkerPool.close();stores.values.foreach(_.close());executionHistory.close()}finally {try allocator.close()finally catalogHost.close()}}
}
