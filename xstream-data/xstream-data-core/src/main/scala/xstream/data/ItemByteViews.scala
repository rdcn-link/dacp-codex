package xstream.data


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
    def view[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]={
      LogicalPath.validate(path)
      if((kind eq DataViews.Bytes) && fallback(source.describe(path)))
        Some(DataViews[ByteChunk,AllBytes.type]((_,context,out)=>{
          val records=source.requireView(path,DataViews.Records).open(Scan(),context)
          try RecordJsonl.encode(out,context.check,chunkSize)(records.consume) finally records.close()
        }).asInstanceOf[DataView[K,S]])
      else source.view(path,kind)
    }
    override def close():Unit=source.close()
  }
}
