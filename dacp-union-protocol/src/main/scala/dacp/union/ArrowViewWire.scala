package dacp.union

import _root_.xstream.data._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.flight.{FlightStream,FlightProducer}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.IpcOption

/** All views are ordinary Arrow schemas and batches over Flight. */
object ArrowViewWire {
  def json(bytes:Array[Byte]):JsonNode={
    require(bytes.length<=DataViewSupports.MaxMetadataBytes,"view metadata too large")
    val n=Data.mapper.reader().`with`(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(bytes)
    require(n!=null && n.isObject,"invalid view metadata");n
  }
  def schema(support:DataViewSupport,access:DatasetAccess,path:String,a:BufferAllocator):org.apache.arrow.vector.types.pojo.Schema={
    object Complete extends scala.util.control.ControlThrowable
    var result:org.apache.arrow.vector.types.pojo.Schema=null
    val stream=access.requireView(path,support.kind).open(support.selection(Data.mapper.createObjectNode()),StreamContext(a))
    try stream.consume(new ArrowConsumer {
      def start(s:org.apache.arrow.vector.types.pojo.Schema,d:DictionaryProvider):Unit={result=s;throw Complete}
      def accept(b:ArrowBatch):Unit=throw new IllegalStateException("missing schema")
    })catch{case Complete=>()}finally stream.close()
    require(result!=null,"missing schema");result
  }
  def send(support:DataViewSupport,access:DatasetAccess,path:String,selection:JsonNode,a:BufferAllocator,out:FlightProducer.ServerStreamListener,check:()=>Unit):Unit={
    val stream=access.requireView(path,support.kind).open(support.selection(selection),StreamContext(a,check))
    BatchStreams.consume(stream,a,new BatchReceiver {
      def start(root:VectorSchemaRoot,d:DictionaryProvider,option:IpcOption):Unit=out.start(root,d,option)
      def putNext():Unit={check();out.putNext()}
      override def completed():Unit={check();out.completed()}
    },check)
  }
  def consume(reader:FlightStream,view:String,out:ArrowConsumer,check:()=>Unit):Unit={
    check();val schema=reader.getSchema
    require(ArrowStreamMetadata.view(schema)==view,"unexpected stream view")
    out.start(schema,reader.getDictionaryProvider)
    while({check();reader.next()})out.accept(ArrowBatch(reader.getRoot,reader.getDictionaryProvider))
  }
}
