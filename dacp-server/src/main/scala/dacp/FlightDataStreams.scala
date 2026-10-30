package dacp

import _root_.xstream.data._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.IpcOption

/** Transport boundary. Plugins and typed streams do not implement Flight listeners. */
object FlightDataStreams {
  def receiver(out:ServerStreamListener):BatchReceiver=new BatchReceiver {
    def start(r:VectorSchemaRoot,d:DictionaryProvider,o:IpcOption):Unit=out.start(r,d,o)
    def putNext():Unit=out.putNext()
    override def completed():Unit=out.completed()
  }
  def send(access:DatasetAccess,path:String,view:String,a:BufferAllocator,out:ServerStreamListener,check:()=>Unit):Unit=
    BatchStreams.read(access,path,view,a,receiver(out),check)
  def scan(access:DatasetAccess,path:String,s:Scan,a:BufferAllocator,out:ServerStreamListener,check:()=>Unit):Unit=
    BatchStreams.scan(access,path,s,a,receiver(out),check)
}
