package dacp

import org.apache.arrow.flight._
import dacp.protocol.Metadata
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarBinaryVector
import scala.jdk.CollectionConverters._

/** Built-in transport source. Backend and format implementations stay behind Item APIs. */
object ItemSource {
  def open(config:Config,allocator:BufferAllocator,binding:InputBinding,c:StreamCancel):FrameSource={
    require(config.taskInputPrefixes.isEmpty || config.taskInputPrefixes.exists(DacpUri.within(binding.uri.get,_)),"FORBIDDEN: task input URI prefix denied")
    val uri=binding.uri.get;val u=DacpUri.parse(uri);val split=u.getPath.stripPrefix("/").split("/",2)
    val client=c.add(NodeConnection.open(config,allocator,uri))
    val info=client.getInfo(FlightDescriptor.path("datasets",split(0),"/"+split(1)))
    val meta=Metadata.Item.parseFrom(info.getAppMetadata)
    require(meta.getKind=="data","input must be a data item")
    val view=ItemViews.select(binding.view,meta.getViewsList.asScala)
    val ticket=Metadata.Ticket.newBuilder().setDataset(split(0)).setItem("/"+split(1)).setView(view).build()
    val stream=c.add(client.getStream(new Ticket(ticket.toByteArray)))
    new FrameSource {
      private var began=false;private var ended=false;private var pending=Iterator.empty[Array[Byte]]
      def next():Option[OperatorFrame]={
        c.active()
        if(!began){began=true;return Some(StreamProtocol.start("",view,meta.getFormat,if(view=="records")Some(stream.getSchema)else None))}
        if(ended)return None
        if(view=="bytes") {
          while(!pending.hasNext) {
            if(!stream.next()){ended=true;return Some(StreamProtocol.frame("end"))}
            val root=stream.getRoot;require(root.getSchema==PluginSchemas.bytes,"invalid byte input schema")
            pending=(0 until root.getRowCount).iterator.flatMap(i=>root.getVector(0).asInstanceOf[VarBinaryVector].get(i).grouped(65536))
          }
          Some(StreamProtocol.frame("data",data=pending.next()))
        } else if(stream.next())Some(StreamProtocol.frame("data",data=StreamProtocol.batch(stream.getRoot)))
        else {ended=true;Some(StreamProtocol.frame("end"))}
      }
      def close():Unit={stream.close();client.close()}
    }
  }
}
