package xstream.data

import java.nio.{ByteBuffer,ByteOrder}
import com.fasterxml.jackson.databind.JsonNode

/** Local scientific contracts. Format readers and network codecs are separate plugins. */
final case class NumericEncoding(dataType:String,byteOrder:ByteOrder) {
  val bytesPerValue:Int=dataType match {
    case "int8"|"uint8"=>1
    case "int16"|"uint16"=>2
    case "int32"|"uint32"|"float32"=>4
    case "int64"|"uint64"|"float64"=>8
    case _=>throw new IllegalArgumentException("unsupported scientific numeric encoding")
  }
  require(byteOrder!=null,"byte order required")
}
final case class Dimension(name:String,length:Long,coordinateItem:Option[String]=None,unit:Option[String]=None) {
  require(name.nonEmpty && length>=0,"invalid dimension")
  coordinateItem.foreach(LogicalPath.validate)
}
final case class ArraySection(origin:Vector[Long],shape:Vector[Long],stride:Vector[Long]) {
  require(origin.size==shape.size && shape.size==stride.size,"array section rank mismatch")
  require(origin.forall(_>=0) && shape.forall(_>=0) && stride.forall(_>0),"invalid array section")
  def validate(dimensions:Vector[Dimension]):Unit={
    require(dimensions.size==shape.size,"array section rank mismatch")
    dimensions.indices.foreach { i=>
      require(origin(i)<=dimensions(i).length,"array origin out of bounds")
      if(shape(i)>0)require(BigInt(origin(i))+BigInt(shape(i)-1)*stride(i)<dimensions(i).length,"array section out of bounds")
    }
  }
}
final case class ArrayDescriptor(dimensions:Vector[Dimension],encoding:NumericEncoding,attributes:Map[String,JsonNode]=Map.empty) extends DataDescriptor[ArrayChunk] {
  require(dimensions!=null,"array dimensions required")
}
/** Dense row-major values; origin/stride locate values in the original variable. Borrowed buffer. */
final case class ArrayChunk(section:ArraySection,encoding:NumericEncoding,values:ByteBuffer) {
  require(section.shape.foldLeft(BigInt(encoding.bytesPerValue))(_*_ )==values.remaining(),"array payload size mismatch")
}
final case class ImageRegion(x:Long,y:Long,width:Long,height:Long) {
  require(x>=0 && y>=0 && width>=0 && height>=0,"invalid image region")
}
final case class ImageDescriptor(width:Long,height:Long,channels:Int,encoding:NumericEncoding,attributes:Map[String,JsonNode]=Map.empty) extends DataDescriptor[ImageTile] {
  require(width>=0 && height>=0 && channels>0,"invalid image dimensions")
}
/** Dense pixel-interleaved tile, row-major. Coordinates and channels must match its descriptor. */
final case class ImageTile(region:ImageRegion,channels:Int,encoding:NumericEncoding,values:ByteBuffer) {
  require(channels>0 && BigInt(region.width)*region.height*channels*encoding.bytesPerValue==values.remaining(),"image payload size mismatch")
}
final case class SequenceSelection(ids:Set[String]=Set.empty,offset:Long=0,length:Option[Long]=None) {
  require(ids.forall(_.nonEmpty) && offset>=0 && length.forall(_>=0),"invalid sequence selection")
}
final case class SequenceDescriptor(alphabet:String,attributes:Map[String,JsonNode]=Map.empty) extends DataDescriptor[SequenceChunk] {
  require(alphabet.nonEmpty,"sequence alphabet required")
}
/** Offset counts symbols, not file bytes. A final empty chunk can represent an empty sequence. */
final case class SequenceChunk(id:String,offset:Long,symbols:CharSequence,last:Boolean,description:Option[String]=None) {
  require(id.nonEmpty && offset>=0 && symbols!=null,"invalid sequence chunk")
}
final case class ArraySelection(section:Option[ArraySection]=None)
final case class ImageSelection(region:Option[ImageRegion]=None)
object ScientificViews {
  val Arrays=new DataViewType[ArrayChunk,ArraySelection]("array")
  val Images=new DataViewType[ImageTile,ImageSelection]("image")
  val Sequences=new DataViewType[SequenceChunk,SequenceSelection]("sequence")
}
