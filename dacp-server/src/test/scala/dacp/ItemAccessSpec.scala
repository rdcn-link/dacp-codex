package dacp

import _root_.xstream.data._

import java.util.concurrent.CancellationException
import org.apache.arrow.memory.{BufferAllocator,RootAllocator}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.scalatest.funsuite.AnyFunSuite

class ItemAccessSpec extends AnyFunSuite {
  test("object handles close after count, stream, scan, failure and mid-stream cancellation") {
    var opened = 0
    var closed = 0
    var fail = false
    val access = new ItemDatasetAccess {
      def list(): Seq[Item] = Seq(describe("/object"))
      def list(path: String): Seq[Item] = { require(path == "/"); list() }
      def describe(path: String): Item = {
        LogicalPath.validate(path)
        if (path == "/") Item(path,"",None,"folder",views=Some(Seq.empty))
        else if (path == "/object") Item(path,"remote-object",Some(PluginSchemas.bytes),views=Some(Seq("bytes","records")))
        else throw new java.io.FileNotFoundException()
      }
      def openItem(path: String): ItemAccess = {
        opened += 1
        new BatchItemAccess {
          def count(): Option[Long] = if (fail) throw new java.io.IOException() else Some(1L)
          def read(view: String, a: BufferAllocator, l: BatchReceiver, ready: () => Unit): Unit = {
            ready()
            if (fail) throw new java.io.IOException()
            val root=org.apache.arrow.vector.VectorSchemaRoot.create(PluginSchemas.bytes,a)
            try l.start(root) finally root.close()
          }
          override def readScan(s: Scan, a: BufferAllocator, l: BatchReceiver, ready: () => Unit): Unit = read("records",a,l,ready)
          override def close(): Unit = { closed += 1 }
        }
      }
    }
    val sink=new BatchReceiver {def start(r:org.apache.arrow.vector.VectorSchemaRoot,d:org.apache.arrow.vector.dictionary.DictionaryProvider,o:org.apache.arrow.vector.ipc.message.IpcOption):Unit=();def putNext():Unit=()}
    val allocator = new RootAllocator()
    try {
      assert(access.count("/").isEmpty && opened == 0)
      assert(access.count("/object").contains(1L))
      BatchStreams.read(access,"/object","bytes",allocator,sink,() => ())
      BatchStreams.scan(access,"/object",Scan(),allocator,sink,() => ())
      fail = true
      intercept[java.io.IOException](access.count("/object"))
      intercept[java.io.IOException](BatchStreams.read(access,"/object","bytes",allocator,sink,() => ()))
      intercept[java.io.IOException](BatchStreams.scan(access,"/object",Scan(),allocator,sink,() => ()))
      fail = false
      var checks = 0
      intercept[CancellationException] {
        BatchStreams.read(access,"/object","bytes",allocator,sink,() => { checks += 1; if (checks == 3) throw new CancellationException() })
      }
      assert(opened == 7 && closed == opened)
      intercept[IllegalArgumentException](BatchStreams.read(access,"/object","image",allocator,sink,() => ()))
      intercept[IllegalArgumentException](access.count("/../object"))
      intercept[java.io.FileNotFoundException](access.count("/missing"))
      assert(opened == 7)
    } finally allocator.close()
  }
}
