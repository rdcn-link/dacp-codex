package xstream.data.scientific

import _root_.xstream.data._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.{ByteBuffer,ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

private[scientific] object Json {
  def obj():ObjectNode=Data.mapper.createObjectNode()
  def fields(n:JsonNode,allowed:Set[String]):Unit=require(n!=null && n.isObject && n.fieldNames.asScala.forall(allowed),"invalid view selection")
  def number(n:JsonNode,key:String):Long={val x=n.get(key);require(x!=null && x.isIntegralNumber && x.canConvertToLong,"expected integer: "+key);x.longValue()}
  def longs(n:JsonNode,key:String):Vector[Long]={val x=n.get(key);require(x!=null && x.isArray,"expected array: "+key);x.elements.asScala.map(v=>{require(v.isIntegralNumber && v.canConvertToLong,"expected integer array");v.longValue()}).toVector}
  def put(n:ObjectNode,key:String,values:Iterable[Long]):Unit={val a=n.putArray(key);values.foreach(a.add)}
  def attrs(n:JsonNode):Map[String,JsonNode]=Option(n.get("attributes")).map { x=>require(x.isObject,"invalid attributes");x.fields.asScala.map(e=>e.getKey->e.getValue).toMap }.getOrElse(Map.empty)
  def addAttrs(n:ObjectNode,attrs:Map[String,JsonNode]):Unit={val a=n.putObject("attributes");attrs.foreach{case(k,v)=>a.set[JsonNode](k,v)}}
  def encoding(n:JsonNode):NumericEncoding={val order=n.path("byte_order").asText;require(Set("little","big")(order),"invalid byte order");NumericEncoding(n.path("data_type").asText,if(order=="little")ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN)}
  def addEncoding(n:ObjectNode,e:NumericEncoding):Unit={n.put("data_type",e.dataType);n.put("byte_order",if(e.byteOrder==ByteOrder.LITTLE_ENDIAN)"little"else"big")}
  def bytes(b:ByteBuffer):Array[Byte]={require(b.remaining<=DataViewCodecs.MaxBlockBytes,"data block too large");val out=new Array[Byte](b.remaining);b.duplicate().get(out);out}
  def base(n:JsonNode,view:String):Unit=require(n.path("version").asInt==1 && n.path("view").asText==view,"unsupported view descriptor")
  def base(view:String):ObjectNode=obj().put("version",1).put("view",view)
  def section(n:JsonNode):ArraySection={fields(n,Set("origin","shape","stride"));val shape=longs(n,"shape");ArraySection(longs(n,"origin"),shape,if(n.has("stride"))longs(n,"stride")else Vector.fill(shape.size)(1L))}
  def section(s:ArraySection):ObjectNode={val n=obj();put(n,"origin",s.origin);put(n,"shape",s.shape);put(n,"stride",s.stride);n}
  def region(n:JsonNode):ImageRegion={fields(n,Set("x","y","width","height"));ImageRegion(number(n,"x"),number(n,"y"),number(n,"width"),number(n,"height"))}
  def region(r:ImageRegion):ObjectNode=obj().put("x",r.x).put("y",r.y).put("width",r.width).put("height",r.height)
}
private[scientific] object Authoring {
  val integer="""{"type":"integer","minimum":0}"""
  def schema(properties:String):JsonNode=Data.mapper.readTree("""{"type":"object","properties":{REPLACE},"additionalProperties":false}""".replace("REPLACE",properties))
  def numeric(buffer:ByteBuffer,e:NumericEncoding,max:Int):JsonNode={
    val a=Data.mapper.createArrayNode();val b=buffer.duplicate().order(e.byteOrder)
    var count=0
    while(b.hasRemaining && count<max){
      e.dataType match {
        case "int8"=>a.add(b.get().toInt);case "uint8"=>a.add(b.get().toInt & 255)
        case "int16"=>a.add(b.getShort().toInt);case "uint16"=>a.add(b.getShort().toInt & 65535)
        case "int32"=>a.add(b.getInt());case "uint32"=>a.add(java.lang.Integer.toUnsignedLong(b.getInt()))
        case "int64"=>a.add(b.getLong());case "uint64"=>a.add(new java.math.BigInteger(java.lang.Long.toUnsignedString(b.getLong())))
        case "float32"=>val v=b.getFloat();if(v.isNaN||v.isInfinite)a.add(v.toString)else a.add(v)
        case "float64"=>val v=b.getDouble();if(v.isNaN||v.isInfinite)a.add(v.toString)else a.add(v)
      };count+=1
    };a
  }
}
final class ArrayCodec extends DataViewCodec {
  type Block=ArrayChunk;type Selection=ArraySelection
  val id="array-v1";val kind=ScientificViews.Arrays
  override def selectionSchema:JsonNode=Authoring.schema(""""origin":{"type":"array","items":{"type":"integer","minimum":0}},"shape":{"type":"array","items":{"type":"integer","minimum":0}},"stride":{"type":"array","items":{"type":"integer","minimum":1}}""")
  override def validateSelection(s:ArraySelection,d:DataDescriptor[ArrayChunk]):Unit=s.section.foreach(_.validate(d.asInstanceOf[ArrayDescriptor].dimensions))
  override def preview(b:ArrayChunk,max:Int):JsonNode={val n=Json.section(b.section);n.set[JsonNode]("values",Authoring.numeric(b.values,b.encoding,max));n.put("values_truncated",b.values.remaining()/b.encoding.bytesPerValue>max);n}
  def selection(n:JsonNode):ArraySelection={Json.fields(n,Set("origin","shape","stride"));ArraySelection(if(n.size==0)None else Some(Json.section(n)))}
  def selectionJson(s:ArraySelection):JsonNode=s.section.map(Json.section).getOrElse(Json.obj())
  def descriptor(d:DataDescriptor[ArrayChunk]):JsonNode={
    val a=d.asInstanceOf[ArrayDescriptor];val n=Json.base("array");Json.addEncoding(n,a.encoding);Json.addAttrs(n,a.attributes)
    val dims=n.putArray("dimensions");a.dimensions.foreach{d=>val x=dims.addObject().put("name",d.name).put("length",d.length);d.coordinateItem.foreach(x.put("coordinate_item",_));d.unit.foreach(x.put("unit",_))};n
  }
  def descriptor(n:JsonNode):DataDescriptor[ArrayChunk]={
    Json.base(n,"array");require(n.path("dimensions").isArray,"missing dimensions")
    ArrayDescriptor(n.path("dimensions").elements.asScala.map(d=>Dimension(d.path("name").asText,Json.number(d,"length"),Option(d.get("coordinate_item")).map(_.asText),Option(d.get("unit")).map(_.asText))).toVector,Json.encoding(n),Json.attrs(n))
  }
  def encode(b:ArrayChunk):EncodedDataBlock=EncodedDataBlock(Json.section(b.section),Json.bytes(b.values))
  def decode(b:EncodedDataBlock,d:DataDescriptor[ArrayChunk]):ArrayChunk={val a=d.asInstanceOf[ArrayDescriptor];val section=Json.section(b.metadata);section.validate(a.dimensions);ArrayChunk(section,a.encoding,ByteBuffer.wrap(b.data).order(a.encoding.byteOrder))}
}
final class ImageCodec extends DataViewCodec {
  type Block=ImageTile;type Selection=ImageSelection
  val id="image-v1";val kind=ScientificViews.Images
  override def selectionSchema:JsonNode=Authoring.schema(""""x":{"type":"integer","minimum":0},"y":{"type":"integer","minimum":0},"width":{"type":"integer","minimum":0},"height":{"type":"integer","minimum":0}""")
  override def validateSelection(s:ImageSelection,d:DataDescriptor[ImageTile]):Unit=s.region.foreach{r=>val a=d.asInstanceOf[ImageDescriptor];require(BigInt(r.x)+r.width<=a.width && BigInt(r.y)+r.height<=a.height,"image region out of bounds")}
  override def preview(b:ImageTile,max:Int):JsonNode={val n=Json.region(b.region);n.put("channels",b.channels);n.set[JsonNode]("values",Authoring.numeric(b.values,b.encoding,max));n.put("values_truncated",b.values.remaining()/b.encoding.bytesPerValue>max);n}
  def selection(n:JsonNode):ImageSelection={Json.fields(n,Set("x","y","width","height"));ImageSelection(if(n.size==0)None else Some(Json.region(n)))}
  def selectionJson(s:ImageSelection):JsonNode=s.region.map(Json.region).getOrElse(Json.obj())
  def descriptor(d:DataDescriptor[ImageTile]):JsonNode={val a=d.asInstanceOf[ImageDescriptor];val n=Json.base("image").put("width",a.width).put("height",a.height).put("channels",a.channels);Json.addEncoding(n,a.encoding);Json.addAttrs(n,a.attributes);n}
  def descriptor(n:JsonNode):DataDescriptor[ImageTile]={Json.base(n,"image");ImageDescriptor(Json.number(n,"width"),Json.number(n,"height"),Math.toIntExact(Json.number(n,"channels")),Json.encoding(n),Json.attrs(n))}
  def encode(b:ImageTile):EncodedDataBlock=EncodedDataBlock(Json.region(b.region),Json.bytes(b.values))
  def decode(b:EncodedDataBlock,d:DataDescriptor[ImageTile]):ImageTile={val a=d.asInstanceOf[ImageDescriptor];val r=Json.region(b.metadata);require(BigInt(r.x)+r.width<=a.width && BigInt(r.y)+r.height<=a.height,"image region out of bounds");ImageTile(r,a.channels,a.encoding,ByteBuffer.wrap(b.data).order(a.encoding.byteOrder))}
}
final class SequenceCodec extends DataViewCodec {
  type Block=SequenceChunk;type Selection=SequenceSelection
  val id="sequence-v1";val kind=ScientificViews.Sequences
  override def selectionSchema:JsonNode=Authoring.schema(""""ids":{"type":"array","items":{"type":"string"}},"offset":{"type":"integer","minimum":0},"length":{"type":"integer","minimum":0}""")
  override def preview(b:SequenceChunk,max:Int):JsonNode={val n=Json.obj().put("id",b.id).put("offset",b.offset).put("last",b.last).put("symbols",b.symbols.toString.take(max)).put("symbols_truncated",b.symbols.length>max);b.description.foreach(n.put("description",_));n}
  def selection(n:JsonNode):SequenceSelection={Json.fields(n,Set("ids","offset","length"));val ids=Option(n.get("ids")).map{x=>require(x.isArray && x.elements.asScala.forall(_.isTextual),"invalid sequence IDs");x.elements.asScala.map(_.asText).toSet}.getOrElse(Set.empty[String]);SequenceSelection(ids,if(n.has("offset"))Json.number(n,"offset")else 0,if(n.has("length"))Some(Json.number(n,"length"))else None)}
  def selectionJson(s:SequenceSelection):JsonNode={val n=Json.obj().put("offset",s.offset);val ids=n.putArray("ids");s.ids.toVector.sorted.foreach(ids.add);s.length.foreach(n.put("length",_));n}
  def descriptor(d:DataDescriptor[SequenceChunk]):JsonNode={val a=d.asInstanceOf[SequenceDescriptor];val n=Json.base("sequence").put("alphabet",a.alphabet);Json.addAttrs(n,a.attributes);n}
  def descriptor(n:JsonNode):DataDescriptor[SequenceChunk]={Json.base(n,"sequence");SequenceDescriptor(n.path("alphabet").asText,Json.attrs(n))}
  def encode(b:SequenceChunk):EncodedDataBlock={val n=Json.obj().put("id",b.id).put("offset",b.offset).put("last",b.last);b.description.foreach(n.put("description",_));val bytes=b.symbols.toString.getBytes(UTF_8);require(bytes.length<=DataViewCodecs.MaxBlockBytes,"sequence chunk too large");EncodedDataBlock(n,bytes)}
  def decode(b:EncodedDataBlock,d:DataDescriptor[SequenceChunk]):SequenceChunk={val n=b.metadata;require(n.path("last").isBoolean,"invalid sequence terminator");val decoder=UTF_8.newDecoder();SequenceChunk(n.path("id").asText,Json.number(n,"offset"),decoder.decode(ByteBuffer.wrap(b.data)).toString,n.path("last").asBoolean,Option(n.get("description")).map(_.asText))}
}
