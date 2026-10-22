package dacp

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator

/** Server-side representation adapter. A provider's declared views always describe native support. */
object ItemByteViews {
  def wrap(source:DatasetAccess,nativeViews:Item=>Seq[String],chunkSize:Int):DatasetAccess=new DatasetAccess {
    private def fallback(item:Item):Boolean={val views=nativeViews(item);views.contains("records") && !views.contains("bytes")}
    private def published(item:Item):Item=if(fallback(item))
      item.copy(views=Some(nativeViews(item):+"bytes"),totalSizeBytes=None) else item
    def list():Seq[Item]=source.list().map(published)
    def list(path:String):Seq[Item]=source.list(path).map(published)
    def describe(path:String):Item=published(source.describe(path))
    def count(path:String):Option[Long]=source.count(path)
    override def scan(path:String,request:Scan,allocator:BufferAllocator,listener:ServerStreamListener,ready:()=>Unit):Unit=
      source.scan(path,request,allocator,listener,ready)
    def stream(path:String,view:String,allocator:BufferAllocator,listener:ServerStreamListener,ready:()=>Unit):Unit={
      LogicalPath.validate(path);ready()
      if(view=="bytes" && fallback(source.describe(path)))
        RecordJsonl.stream(allocator,listener,ready,chunkSize)(l=>source.stream(path,"records",allocator,l,ready))
      else source.stream(path,view,allocator,listener,ready)
    }
    override def close():Unit=source.close()
  }
}
