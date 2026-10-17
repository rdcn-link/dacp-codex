package dacp

import java.nio.file.Files
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Shared file/SQL streaming for namespace-backed providers. */
object DatasetStreams {
  def stream(namespace: NamespaceAdapter, path: String, view: String, allocator: BufferAllocator, config: Config,
             listener: ServerStreamListener, ready: () => Unit): Unit = {
    require(Set("records","bytes")(view),"invalid view")
    namespace.withResolved(path,ready) { (source,internalItem) =>
      source match {
        case sql: SqlAdapter =>
          ready()
          if (view == "records") sql.stream(internalItem,allocator)(root => listener.start(root),root => listener.putNext(),ready)
          else SqlAdapter.bytes(sql,internalItem,allocator,config.byteChunk,listener,ready)
        case file: DirectoryAdapter =>
          val path = file.resolve(internalItem)
          val handler = Formats.detect(path,file.options)
          require(view == "bytes" || handler.records,"unsupported stream view")
          if (view == "bytes") {
            val root = VectorSchemaRoot.create(Data.bytesSchema,allocator)
            try {
              val in = Files.newInputStream(path)
              try {
                listener.start(root)
                val buffer = new Array[Byte](config.byteChunk)
                var done = false
                while (!done) {
                  ready(); val n = in.read(buffer)
                  if (n < 0) done = true else {
                    root.clear(); root.allocateNew()
                    root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,buffer,0,n)
                    root.setRowCount(1); listener.putNext()
                  }
                }
              } finally in.close()
            } finally root.close()
          } else handler.stream(path,file.options,allocator,config.batchSize,config.inferSample)(
            root => listener.start(root),root => listener.putNext(),ready)
      }
      listener.completed()
    }
  }
}
