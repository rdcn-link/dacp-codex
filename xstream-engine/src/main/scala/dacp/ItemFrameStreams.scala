package dacp

import _root_.xstream.data._
/** Typed local streams are encoded only at the operator protocol boundary. */
object ItemFrameStreams {
  def encoded(format:String,emit:OperatorFrame=>Unit):EncodedDataConsumer=new EncodedDataConsumer {
    def start(d:com.fasterxml.jackson.databind.JsonNode):Unit=emit(StreamProtocol.start("",d.path("view").asText,format,structure=Some(d)))
    def accept(b:EncodedDataBlock):Unit={
      val h=Data.mapper.createObjectNode().put("type","data").put("port","")
      h.set[com.fasterxml.jackson.databind.JsonNode]("block",b.metadata)
      emit(OperatorFrame(Data.mapper.writeValueAsBytes(h),b.data))
    }
  }
  def remote(format:String,cancel:StreamCancel)(read:EncodedDataConsumer=>Unit):FrameSource={
    val queue=new FrameQueue(cancel)
    val worker=StreamProtocol.thread(cancel){read(encoded(format,queue.put));queue.put(StreamProtocol.frame("end"));queue.end()}
    new FrameSource {def next():Option[OperatorFrame]=queue.next();def close():Unit={worker.interrupt();worker.join(5000)}}
  }

  def consume(access:DatasetAccess,path:String,view:String,format:String,scan:Option[Scan],context:StreamContext,codec:Option[DataViewCodec]=None,selection:Option[com.fasterxml.jackson.databind.JsonNode]=None)(emit:OperatorFrame=>Unit):Unit={
    if(view=="records") {
      val stream=access.requireView(path,DataViews.Records).open(scan.getOrElse(Scan()),context)
      try stream.consume(new DataConsumer[RecordBatch] {
        def start(d:DataDescriptor[RecordBatch]):Unit={val r=d.asInstanceOf[RecordsDescriptor];require(r.dictionaries==null,"dictionary input unsupported");emit(StreamProtocol.start("",view,format,Some(r.schema)))}
        def accept(b:RecordBatch):Unit={context.check();emit(StreamProtocol.frame("data",data=StreamProtocol.batch(b.root)))}
      }) finally stream.close()
    } else if(view!="bytes") {
      require(scan.isEmpty,"scientific views cannot use SQL")
      val c=codec.getOrElse(throw new IllegalArgumentException("view codec unavailable: "+view))
      c.send(access,path,selection.getOrElse(Data.mapper.createObjectNode()),context,encoded(format,emit))
    } else {
      require(view=="bytes" && scan.isEmpty,"unsupported input view or selection")
      val stream=access.requireView(path,DataViews.Bytes).open(AllBytes,context)
      try stream.consume(new DataConsumer[ByteChunk] {
        def start(d:DataDescriptor[ByteChunk]):Unit=emit(StreamProtocol.start("",view,format,None))
        def accept(b:ByteChunk):Unit={var at=b.offset;val end=at+b.length;while(at<end){context.check();val n=math.min(65536,end-at);emit(StreamProtocol.frame("data",data=java.util.Arrays.copyOfRange(b.bytes,at,at+n)));at+=n}}
      }) finally stream.close()
    }
  }
}
