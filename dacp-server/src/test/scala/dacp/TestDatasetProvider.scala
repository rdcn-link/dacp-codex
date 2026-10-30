package dacp

import _root_.xstream.data._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Demonstrates an external provider without file or JDBC bindings. */
class TestDatasetProvider extends DatasetProvider {
  val id = "test-dataset"
  override val itemSettingsKey = "entries"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = new PublishedDataset {
    val payload = d.source.getOrElse("message","plugin").toString.getBytes("UTF-8")
    def open(check: () => Unit): DatasetAccess = new BatchDatasetAccess {
      private val item = Item("/message","test-native",Some(PluginSchemas.bytes),views=Some(Seq("bytes")))
      def list(): Seq[Item] = { check(); Seq(item) }
      def list(path: String): Seq[Item] = { LogicalPath.validate(path); require(path == "/","listing requires a folder"); list() }
      def describe(path: String): Item = {
        check(); LogicalPath.validate(path)
        if(path == "/") ItemProperties.folder("/")
        else if(path == item.path) item else throw new java.io.FileNotFoundException("logical item not found")
      }
      def count(path: String): Option[Long] = { describe(path); None }
      def read(path: String, view: String, allocator: BufferAllocator, listener: BatchReceiver, ready: () => Unit): Unit = {
        require(describe(path).kind == "data" && view == "bytes","unsupported stream view")
        val root = VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
        try {
          ready(); root.allocateNew(); root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,payload)
          root.setRowCount(1); listener.start(root); listener.putNext(); listener.completed()
        } finally root.close()
      }
    }
  }
}
