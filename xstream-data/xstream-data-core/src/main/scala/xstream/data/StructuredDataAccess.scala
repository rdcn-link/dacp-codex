package xstream.data

/** Metadata/browsing helper for structured file plugins. Content acquisition remains plugin-owned. */
abstract class StructuredDataAccess(source:ByteSource,chunkBytes:Int=65536) extends DatasetAccess {
  protected def entries:Seq[Item]
  protected def content[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]
  def list():Seq[Item]=entries
  def describe(path:String):Item={LogicalPath.validate(path);if(path=="/")Item("/","",None,"folder",views=Some(Seq("bytes")))else entries.find(_.path==path).getOrElse(throw new java.io.FileNotFoundException("structured item not found"))}
  def list(path:String):Seq[Item]={require(describe(path).kind=="folder","listing requires folder");entries.filter(i=>LogicalPath.parent(i.path)==path)}
  def count(path:String):Option[Long]=describe(path).recordCount
  final def view[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]={
    val item=describe(path)
    if(path=="/")DataViews.when(kind,DataViews.Bytes)(DataViews[ByteChunk,AllBytes.type]((_,c,o)=>{
      val in=source.open(c.check)
      try {o.start(BytesDescriptor());val b=new Array[Byte](chunkBytes);var end=false;while(!end){c.check();val n=in.read(b);if(n<0)end=true else if(n>0)o.accept(ByteChunk(b,0,n))}}finally in.close()
    }))
    else if(!item.views.exists(_.contains(kind.name)))None else content(path,kind)
  }
}
/** Iterates bounded, dense chunks of a strided selection, preserving original coordinates and rank. */
object ArrayBlocks {
  def sections(selection:ArraySection,dimensions:Vector[Dimension],encoding:NumericEncoding,maxBytes:Int)(emit:ArraySection=>Unit):Unit={
    selection.validate(dimensions);require(maxBytes>=encoding.bytesPerValue,"chunk bound too small")
    if(selection.shape.contains(0L))return
    val rank=selection.shape.size
    if(rank==0){emit(selection);return}
    val tile=Array.fill[Long](rank)(1);var left=maxBytes/encoding.bytesPerValue
    for(i<-(0 until rank).reverse){tile(i)=math.min(selection.shape(i),left.toLong);left=math.max(1,(left/tile(i)).toInt)}
    val at=Array.fill[Long](rank)(0);var done=false
    while(!done){
      val shape=at.indices.map(i=>math.min(tile(i),selection.shape(i)-at(i))).toVector
      val origin=at.indices.map(i=>Math.addExact(selection.origin(i),Math.multiplyExact(at(i),selection.stride(i)))).toVector
      emit(ArraySection(origin,shape,selection.stride))
      var i=rank-1;var carry=true
      while(i>=0 && carry){at(i)+=tile(i);if(at(i)>=selection.shape(i)){at(i)=0;i-=1}else carry=false}
      done=carry
    }
  }
  def selected(request:ArraySelection,dimensions:Vector[Dimension]):ArraySection=request.section.getOrElse(ArraySection(Vector.fill(dimensions.size)(0L),dimensions.map(_.length),Vector.fill(dimensions.size)(1L)))
}
