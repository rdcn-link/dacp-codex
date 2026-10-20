package dacp

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Public capabilities shared by all providers; storage details stay inside the provider.
  * Implement directly for dataset-scoped access, or extend ItemDatasetAccess for per-object handles.
  */
trait DatasetAccess extends DataSetAdapter with AutoCloseable {
  def close(): Unit = ()
  def list(path: String): Seq[Item]
  def count(path: String): Option[Long]
  /** Override only when the source preserves Scan semantics; otherwise use this streaming fallback. */
  def scan(path: String, request: Scan, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit =
    ScanStreams.stream(request,allocator,listener)(filtered => stream(path,"records",allocator,filtered,ready))
  def stream(path: String, view: String, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit
}
trait PublishedDataset extends AutoCloseable {
  def close(): Unit = ()
  def open(check: () => Unit): DatasetAccess
}
case class DatasetDefinition(provider: String, source: Map[String,Object] = Map.empty,
                             items: Map[String,Map[String,Object]] = Map.empty, metadata:DatasetMetadata=DatasetMetadata()) {
  override def toString: String = s"DatasetDefinition($provider,<private settings>)"
}
case class SourceDefinition(provider:String,settings:Map[String,Object]) {
  override def toString:String=s"SourceDefinition($provider,<private settings>)"
}
/** ServiceLoader extension point; each provider owns parsing and construction of its settings. */
trait DatasetProvider extends Plugin {
  def id: String
  /** Dataset-level key for logical-path settings; source holds provider-wide parameters. */
  def itemSettingsKey: String = "items"
  def create(definition: DatasetDefinition, context: DatasetContext): PublishedDataset
}
/** Server services available to plugins without depending on server implementation classes. */
trait DatasetContext {
  def batchSize: Int
  def inferSample: Int
  def byteChunk: Int
  def countFileRecordsOnStartup: Boolean
  def plugins: PluginEnvironment
  def sources: Map[String,SourceDefinition]
  /** Construct a dataset through an installed provider without referencing its implementation. */
  def createDataset(definition: DatasetDefinition): PublishedDataset
}
