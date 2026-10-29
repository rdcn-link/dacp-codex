package dacp

import org.apache.arrow.memory.{ArrowBuf,BufferAllocator}
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Direct, bounded catalog-to-engine bridge; no server, socket or temporary data file. */
object CatalogItemSource {
  def open(catalog:DataCatalog,plugins:PluginEnvironment,allocator:BufferAllocator,binding:InputBinding,cancel:StreamCancel):FrameSource={
    val uri=java.net.URI.create(binding.uri.get);require(uri.getScheme=="context","expected context Item URI")
    val parts=uri.getPath.stripPrefix("/").split("/",2);require(parts.length==2,"expected dataset and Item")
    val queue=new FrameQueue(cancel)
    val worker=StreamProtocol.thread(cancel){
      val lease=cancel.add(catalog.open(parts(0),()=>cancel.active()))
      val access=cancel.add(ItemByteViews.wrap(lease.open(()=>cancel.active()),i=>plugins.views(i),65536))
      try {
        val path="/"+parts(1);val item=access.describe(path)
        require(item.kind!="folder","input must support a data stream")
        val view=ItemViews.select(binding.view,plugins.views(item));var root:VectorSchemaRoot=null
        val listener=new ServerStreamListener {
          def isReady:Boolean=true
          def isCancelled:Boolean=try {cancel.active();false}catch{case _:Throwable=>true}
          override def setOnReadyHandler(r:Runnable):Unit=()
          def setOnCancelHandler(r:Runnable):Unit=()
          def start(value:VectorSchemaRoot,d:DictionaryProvider,o:IpcOption):Unit={require(d==null,"dictionary streams unsupported");root=value;queue.put(StreamProtocol.start("",view,item.format,if(view=="records")Some(root.getSchema)else None))}
          def putNext():Unit={cancel.active();if(view=="records")queue.put(StreamProtocol.frame("data",data=StreamProtocol.batch(root)))else {val v=root.getVector(0).asInstanceOf[VarBinaryVector];(0 until root.getRowCount).foreach(i=>v.get(i).grouped(65536).foreach(b=>queue.put(StreamProtocol.frame("data",data=b))))}}
          def putNext(metadata:ArrowBuf):Unit=putNext()
          def putMetadata(metadata:ArrowBuf):Unit=()
          def completed():Unit=()
          def error(t:Throwable):Unit=throw t
        }
        binding.scan match {case Some(scan)=>require(view=="records","sql requires records view");access.scan(path,scan,allocator,listener,()=>cancel.active());case None=>access.stream(path,view,allocator,listener,()=>cancel.active())}
        queue.put(StreamProtocol.frame("end"));queue.end()
      }finally {access.close();lease.close()}
    }
    new FrameSource {def next():Option[OperatorFrame]=queue.next();def close():Unit={worker.interrupt();worker.join(5000)}}
  }
}
