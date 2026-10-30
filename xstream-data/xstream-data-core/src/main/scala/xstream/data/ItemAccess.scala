package xstream.data

/** Request-scoped handle; its views must be consumed before the handle is closed. */
trait ItemAccess extends AutoCloseable {
  def count():Option[Long]
  def view[K,S](kind:DataViewType[K,S]):Option[DataView[K,S]]
  def close():Unit=()
}
trait BatchItemAccess extends ItemAccess {
  protected def read(view:String,a:org.apache.arrow.memory.BufferAllocator,out:BatchReceiver,check:()=>Unit):Unit
  protected def readScan(s:Scan,a:org.apache.arrow.memory.BufferAllocator,out:BatchReceiver,check:()=>Unit):Unit=
    ScanStreams.stream(s,a,out)(filtered=>read("records",a,filtered,check))
  def view[K,S](kind:DataViewType[K,S]):Option[DataView[K,S]] = {
    if(kind eq DataViews.Records)Some(DataViews[RecordBatch,Scan]((s,c,o)=>if(s==Scan())read("records",c.allocator,BatchStreams.records(o,c.check),c.check)else readScan(s,c.allocator,BatchStreams.records(o,c.check),c.check)).asInstanceOf[DataView[K,S]])
    else if(kind eq DataViews.Bytes)Some(DataViews[ByteChunk,AllBytes.type]((_,c,o)=>read("bytes",c.allocator,BatchStreams.bytes(o,c.check),c.check)).asInstanceOf[DataView[K,S]])
    else None
  }
}
/** Opens and closes per-item resources inside consumption, including on failures. */
trait ItemDatasetAccess extends DatasetAccess {
  def openItem(path:String):ItemAccess
  private def use[A](path:String)(f:ItemAccess=>A):A={LogicalPath.validate(path);val h=openItem(path);try f(h)finally h.close()}
  final def count(path:String):Option[Long]={LogicalPath.validate(path);if(describe(path).kind=="folder")None else use(path)(_.count())}
  final def view[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]={
    LogicalPath.validate(path)
    if(!describe(path).views.exists(_.contains(kind.name)))None
    else Some(DataViews[K,S]((selection,context,out)=>{
      use(path){h=>val v=h.view(kind).getOrElse(throw new IllegalArgumentException("unsupported stream view"));val s=v.open(selection,context);try s.consume(out)finally s.close()}
    }))
  }
}
