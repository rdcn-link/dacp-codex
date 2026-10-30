package dacp.union

import _root_.xstream.data._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.flight.{FlightStream,FlightProducer}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector,VarCharVector}
import org.apache.arrow.vector.types.pojo.{Schema,Field,FieldType,ArrowType}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

/** Versioned scientific block envelope shared by services, SDKs and Union proxies. */
object ScientificWire {
  private val fields=Seq(new Field("metadata",FieldType.notNullable(new ArrowType.Utf8()),null),new Field("data",FieldType.notNullable(new ArrowType.Binary()),null)).asJava
  def envelopeSchema(view:String):Schema=new Schema(fields,Map("xstream.view"->view).asJava)
  def json(bytes:Array[Byte]):JsonNode={require(bytes.length<=DataViewCodecs.MaxMetadataBytes,"view metadata too large");val n=Data.mapper.reader().`with`(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(bytes);require(n!=null && n.isObject,"invalid view metadata");n}
  private def bytes(n:JsonNode):Array[Byte]={val b=Data.mapper.writeValueAsBytes(n);require(b.length<=DataViewCodecs.MaxMetadataBytes,"view metadata too large");b}
  def send(codec:DataViewCodec,access:DatasetAccess,path:String,selection:JsonNode,a:BufferAllocator,out:FlightProducer.ServerStreamListener,check:()=>Unit):Unit={
    var root:VectorSchemaRoot=null
    try {
      codec.send(access,path,selection,StreamContext(a,check),new EncodedDataConsumer {
        def start(d:JsonNode):Unit={
          require(root==null,"repeated descriptor");check()
          val schema=new Schema(fields,Map("xstream.view"->codec.kind.name,"xstream.descriptor"->new String(bytes(d),UTF_8)).asJava)
          root=VectorSchemaRoot.create(schema,a);out.start(root)
        }
        def accept(b:EncodedDataBlock):Unit={
          check();require(root!=null && b.data.length<=DataViewCodecs.MaxBlockBytes,"invalid scientific block")
          root.clear();root.allocateNew()
          root.getVector(0).asInstanceOf[VarCharVector].setSafe(0,bytes(b.metadata))
          root.getVector(1).asInstanceOf[VarBinaryVector].setSafe(0,b.data)
          root.setRowCount(1);out.putNext()
        }
      })
      require(root!=null,"missing descriptor");check();out.completed()
    }finally if(root!=null)root.close()
  }
  def consume(reader:FlightStream,view:String,out:EncodedDataConsumer,check:()=>Unit):Unit={
    check();val schema=reader.getSchema
    require(schema.getFields==fields && schema.getCustomMetadata.get("xstream.view")==view,"invalid scientific envelope")
    val text=schema.getCustomMetadata.get("xstream.descriptor");require(text!=null,"missing scientific descriptor")
    out.start(json(text.getBytes(UTF_8)))
    while({check();reader.next()}){
      val root=reader.getRoot;require(root.getRowCount<=1024,"too many scientific blocks in batch")
      val meta=root.getVector(0).asInstanceOf[VarCharVector];val data=root.getVector(1).asInstanceOf[VarBinaryVector]
      for(i<-0 until root.getRowCount){check();require(!meta.isNull(i) && !data.isNull(i) && meta.getValueLength(i)<=DataViewCodecs.MaxMetadataBytes && data.getValueLength(i)<=DataViewCodecs.MaxBlockBytes,"invalid scientific block");out.accept(EncodedDataBlock(json(meta.get(i)),data.get(i)))}
    }
  }
}
