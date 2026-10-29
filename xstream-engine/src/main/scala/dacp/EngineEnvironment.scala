package dacp
import org.apache.arrow.memory.BufferAllocator
/** Host boundary: the engine never starts a data server or owns host credentials. */
trait EngineEnvironment extends OperatorEnvironment {
  def computePushdown:Option[ComputePushdown]=None
  def debug:Boolean
  def sourceRuntimes:Map[String,SourceRuntime]
  def operatorFusionEnabled:Boolean
  def maxConcurrentTasks:Int
  private[dacp] def taskSlots:java.util.concurrent.Semaphore
  def executionTimeoutSeconds:Long
  def executionHistory:ExecutionHistory
  def materializationStore:Option[MaterializationStoreDefinition]
  private[dacp] def openMaterialization(d:MaterializationStoreDefinition):MaterializationStore
  def openItem(allocator:BufferAllocator,binding:InputBinding,cancel:StreamCancel):FrameSource
}
