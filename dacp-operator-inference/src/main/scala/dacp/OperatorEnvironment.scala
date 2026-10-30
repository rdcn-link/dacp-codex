package dacp

import _root_.xstream.data._

import java.nio.file.Path

/** Host-owned configuration. Executors are discovered through the same plugin environment. */
trait OperatorEnvironment {
  def localOperatorLibrary:Path
  def remoteOperatorLibraryUrls:Seq[String]
  def operatorCache:Path
  def operatorMaxBytes:Long
  def plugins:PluginEnvironment
  private[dacp] def inferenceWorkerPool:InferenceWorkerPool
}

/** Independent inference resources; no server Config, execution engine or dataset implementation. */
final class OperatorInferenceService(val localOperatorLibrary:Path,val remoteOperatorLibraryUrls:Seq[String],
  val operatorCache:Path,val operatorMaxBytes:Long,val plugins:PluginEnvironment,
  settings:InferenceWorkerSettings=InferenceWorkerSettings()) extends OperatorEnvironment with AutoCloseable {
  require(operatorMaxBytes>0,"operator_max_bytes must be positive")
  private[dacp] val inferenceWorkerPool=new InferenceWorkerPool(settings)
  def infer(reference:String,request:InferenceRequest,cancel:StreamCancel):InferenceResult={
    cancel.active()
    OperatorInference.withDocker { docker=>
      val operator=OperatorLibrary.resolve(this,reference,docker)
      cancel.active()
      OperatorInference.run(this,operator,request,cancel)
    }
  }
  def close():Unit=inferenceWorkerPool.close()
}
