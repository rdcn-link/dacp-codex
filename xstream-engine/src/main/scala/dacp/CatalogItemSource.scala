package dacp

import _root_.xstream.data._

import org.apache.arrow.memory.BufferAllocator


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
        val view=ItemViews.select(binding.view,plugins.views(item))
        ItemFrameStreams.consume(access,path,view,item.format,binding.scan,StreamContext(allocator,()=>cancel.active()),if(Set("bytes","records")(view))None else Some(DataViewCodecs.named(plugins,view)),binding.selection)(queue.put)
        queue.put(StreamProtocol.frame("end"));queue.end()
      }finally {access.close();lease.close()}
    }
    new FrameSource {def next():Option[OperatorFrame]=queue.next();def close():Unit={worker.interrupt();worker.join(5000)}}
  }
}
