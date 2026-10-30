package xstream.data

import com.fasterxml.jackson.databind.JsonNode

final case class EncodedDataBlock(metadata:JsonNode,data:Array[Byte])
trait EncodedDataConsumer {
  def start(descriptor:JsonNode):Unit
  def accept(block:EncodedDataBlock):Unit
}
/** Wire encoding is an extension independent of file formats and transport implementations. */
trait DataViewCodec extends Plugin {
  type Block
  type Selection
  def kind:DataViewType[Block,Selection]
  def selection(json:JsonNode):Selection
  def selectionJson(value:Selection):JsonNode
  def descriptor(value:DataDescriptor[Block]):JsonNode
  def descriptor(json:JsonNode):DataDescriptor[Block]
  /** Authoring hints and bounded block presentation belong to the representation plugin. */
  def selectionSchema:JsonNode=Data.mapper.readTree("""{"type":"object","properties":{},"additionalProperties":false}""")
  def validateSelection(value:Selection,description:DataDescriptor[Block]):Unit=()
  def preview(value:Block,maxValues:Int):JsonNode={
    val b=encode(value);val n=Data.mapper.createObjectNode();n.set[JsonNode]("block",b.metadata)
    n.put("bytes",b.data.length);n
  }
  def encode(value:Block):EncodedDataBlock
  def decode(value:EncodedDataBlock,description:DataDescriptor[Block]):Block
  final def send(access:DatasetAccess,path:String,request:JsonNode,context:StreamContext,out:EncodedDataConsumer):Unit={
    val stream=access.requireView(path,kind).open(selection(request),context)
    try stream.consume(new DataConsumer[Block] {
      def start(d:DataDescriptor[Block]):Unit={validateSelection(selection(request),d);out.start(descriptor(d))}
      def accept(b:Block):Unit={context.check();out.accept(encode(b))}
    })finally stream.close()
  }
  final def remoteView[K,S](requested:DataViewType[K,S])(read:(JsonNode,EncodedDataConsumer,StreamContext)=>Unit):Option[DataView[K,S]] =
    DataViews.when(requested,kind)(DataViews[Block,Selection]((s,c,out)=>{
      var description:DataDescriptor[Block]=null
      read(selectionJson(s),new EncodedDataConsumer {
        def start(d:JsonNode):Unit={require(description==null,"descriptor repeated");description=descriptor(d);out.start(description)}
        def accept(b:EncodedDataBlock):Unit={c.check();require(description!=null,"missing descriptor");out.accept(decode(b,description))}
      },c)
    }))
}
object DataViewCodecs {
  val MaxMetadataBytes=1024*1024
  val MaxBlockBytes=16*1024*1024
  def all(environment:PluginEnvironment):Vector[DataViewCodec]={
    val codecs=environment.services(classOf[DataViewCodec])
    require(codecs.map(_.kind.name).distinct.size==codecs.size,"duplicate data view codec")
    codecs
  }
  def named(environment:PluginEnvironment,name:String):DataViewCodec=all(environment).find(_.kind.name==name)
    .getOrElse(throw new IllegalArgumentException("unsupported stream view: "+name))
  def forType[K,S](environment:PluginEnvironment,kind:DataViewType[K,S]):Option[DataViewCodec]=all(environment).find(_.kind eq kind)
}
