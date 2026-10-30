package dacp

import _root_.xstream.data._

import org.apache.arrow.flight._
import dacp.protocol.Metadata
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarBinaryVector
import scala.jdk.CollectionConverters._

/** Built-in transport source. Backend and format implementations stay behind Item APIs. */
object ItemSource {
  def open(config:Config,allocator:BufferAllocator,binding:InputBinding,c:StreamCancel):FrameSource={
    val authorizedUri=if(binding.uri.get.startsWith("context:"))DacpUri.origin(config.uri(""))+java.net.URI.create(binding.uri.get).getRawPath else binding.uri.get
    require(config.taskInputPrefixes.isEmpty || config.taskInputPrefixes.exists(DacpUri.within(authorizedUri,_)),"FORBIDDEN: task input URI prefix denied")
    if(binding.uri.exists(_.startsWith("context:"))){config.initializeDatasets();return CatalogItemSource.open(DataCatalogRuntime.catalog,config.plugins,allocator,binding,c)}
    val local=binding.uri.exists(u=>DacpUri.origin(u)==DacpUri.origin(config.uri("")))
    if(local){config.initializeDatasets();val u=java.net.URI.create(binding.uri.get);return CatalogItemSource.open(DataCatalogRuntime.catalog,config.plugins,allocator,binding.copy(uri=Some("context:"+u.getRawPath)),c)}
    require(config.taskInputPrefixes.isEmpty || config.taskInputPrefixes.exists(DacpUri.within(binding.uri.get,_)),"FORBIDDEN: task input URI prefix denied")
    val uri=binding.uri.get;val u=DacpUri.parse(uri);val split=u.getPath.stripPrefix("/").split("/",2)
    val client=c.add(NodeConnection.open(config,allocator,uri))
    val info=client.getInfo(FlightDescriptor.path("datasets",split(0),"/"+split(1)))
    val meta=Metadata.Item.parseFrom(info.getAppMetadata)
    require(meta.getKind=="data","input must be a data item")
    val view=ItemViews.select(binding.view,meta.getViewsList.asScala)
    val ticketBuilder=Metadata.Ticket.newBuilder().setDataset(split(0)).setItem("/"+split(1)).setView(view)
    binding.scan.foreach { scan=>require(view=="records","sql requires records view");scan.bind(info.getSchema);ticketBuilder.setScan(ScanProtocol.encode(scan)) }
    binding.selection.foreach(n=>ticketBuilder.setSelectionJson(com.google.protobuf.ByteString.copyFrom(Data.mapper.writeValueAsBytes(n))))
    val ticket=ticketBuilder.build()
    val stream=c.add(client.getStream(new Ticket(ticket.toByteArray)))
    if(!Set("records","bytes")(view))return ItemFrameStreams.remote(meta.getFormat,c){out=>
      try dacp.union.ScientificWire.consume(stream,view,out,()=>c.active())finally {stream.close();client.close()}
    }
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
