package dacp

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
        new ItemAccess {
          def count(): Option[Long] = if (fail) throw new java.io.IOException() else Some(1L)
          def stream(view: String, a: BufferAllocator, l: ServerStreamListener, ready: () => Unit): Unit = {
            ready()
            if (fail) throw new java.io.IOException()
          }
          override def scan(s: Scan, a: BufferAllocator, l: ServerStreamListener, ready: () => Unit): Unit = stream("records",a,l,ready)
          override def close(): Unit = { closed += 1 }
        }
      }
    }
    val allocator = new RootAllocator()
    try {
      assert(access.count("/").isEmpty && opened == 0)
      assert(access.count("/object").contains(1L))
      access.stream("/object","bytes",allocator,null,() => ())
      access.scan("/object",Scan(),allocator,null,() => ())
      fail = true
      intercept[java.io.IOException](access.count("/object"))
      intercept[java.io.IOException](access.stream("/object","bytes",allocator,null,() => ()))
      intercept[java.io.IOException](access.scan("/object",Scan(),allocator,null,() => ()))
      fail = false
      var checks = 0
      intercept[CancellationException] {
        access.stream("/object","bytes",allocator,null,() => { checks += 1; if (checks == 2) throw new CancellationException() })
      }
      assert(opened == 7 && closed == opened)
      intercept[IllegalArgumentException](access.stream("/object","image",allocator,null,() => ()))
      intercept[IllegalArgumentException](access.count("/../object"))
      intercept[java.io.FileNotFoundException](access.count("/missing"))
      assert(opened == 7)
    } finally allocator.close()
  }
}
