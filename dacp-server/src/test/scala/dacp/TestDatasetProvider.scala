package dacp

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Demonstrates an external provider without file or JDBC bindings. */
class TestDatasetProvider extends DatasetProvider {
  val id = "test-dataset"
  def create(d: DatasetDefinition, config: Config): PublishedDataset = new PublishedDataset {
    val payload = d.source.getOrElse("message","plugin").toString.getBytes("UTF-8")
    def open(check: () => Unit): DatasetAccess = new DatasetAccess {
      private val item = Item("/message","test-native",Some(Data.bytesSchema),views=Some(Seq("bytes")))
      def list(): Seq[Item] = { check(); Seq(item) }
      def list(path: String): Seq[Item] = { LogicalPath.validate(path); require(path == "/","listing requires a folder"); list() }
      def describe(path: String): Item = {
        check(); LogicalPath.validate(path)
        if(path == "/") ItemProperties.folder("/")
        else if(path == item.path) item else throw new java.io.FileNotFoundException("logical item not found")
      }
      def count(path: String): Option[Long] = { describe(path); None }
      def stream(path: String, view: String, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit = {
        require(describe(path).kind == "data" && view == "bytes","unsupported stream view")
        val root = VectorSchemaRoot.create(Data.bytesSchema,allocator)
        try {
          ready(); root.allocateNew(); root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,payload)
          root.setRowCount(1); listener.start(root); listener.putNext(); listener.completed()
        } finally root.close()
      }
    }
  }
}
