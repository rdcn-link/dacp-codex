package dacp

import _root_.xstream.data._

import java.io.ByteArrayOutputStream
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.{ArrowBuf,RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector,StructVector}
import org.apache.arrow.vector.dictionary.{Dictionary,DictionaryProvider}
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.types.pojo.{ArrowType,Field,FieldType,Schema,DictionaryEncoding}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class RecordJsonlSpec extends AnyFunSuite {
  private class Output extends DataConsumer[ByteChunk] {
    val data=new ByteArrayOutputStream();var completedCount=0;var chunks=0
    var cancelled=false
    var onChunk:()=>Unit=()=>()
    def text:String=data.toString("UTF-8")
    def start(d:DataDescriptor[ByteChunk]):Unit=()
    def accept(b:ByteChunk):Unit={assert(b.length<=7);data.write(b.bytes,b.offset,b.length);chunks+=1;onChunk()}
  }
  private def encode(a:org.apache.arrow.memory.BufferAllocator,out:Output,check:()=>Unit,chunk:Int)(read:BatchReceiver=>Unit):Unit={
    def active():Unit={check();if(out.cancelled)throw new java.util.concurrent.CancellationException()}
    RecordJsonl.encode(out,()=>active(),chunk)(c=>read(BatchStreams.records(c,()=>active())))
    out.completedCount+=1
  }
  test("UTF-8 JSONL preserves columns, escapes, numeric types and nulls across bounded chunks and batches") {
    val a=new RootAllocator();val out=new Output()
    val root=VectorSchemaRoot.create(new Schema(Seq(Records.field("name","utf8"),Records.field("value","int64"),Records.field("flag","bool")).asJava),a)
    try {
      encode(a,out,()=>(),7) { l=>
        l.start(root)
        Records.fill(root,Seq(Seq("北京\n\"\\",9007199254740993L,true)));l.putNext()
        assert(out.chunks>0 && out.completedCount==0)
        Records.fill(root,Seq(Seq(null,0L,false)));l.putNext();l.completed()
      }
      assert(out.text=="{\"name\":\"北京\\n\\\"\\\\\",\"value\":9007199254740993,\"flag\":true}\n{\"name\":null,\"value\":0,\"flag\":false}\n")
      assert(out.completedCount==1 && out.chunks>2)
    }finally {root.close();assert(a.getAllocatedMemory==0);a.close()}
  }
  test("nested arrays and structs stay nested; binary values use base64 and decimals remain numbers") {
    val a=new RootAllocator();val out=new Output()
    val fields=Seq(
      new Field("samples",FieldType.nullable(new ArrowType.List()),Seq(Records.field("$data$","int64")).asJava),
      new Field("station",FieldType.nullable(new ArrowType.Struct()),Seq(Records.field("name","utf8")).asJava),
      new Field("raw",FieldType.nullable(new ArrowType.Binary()),null),
      new Field("decimal",FieldType.nullable(new ArrowType.Decimal(12,2,128)),null))
    val root=VectorSchemaRoot.create(new Schema(fields.asJava),a)
    try {
      root.allocateNew()
      val list=root.getVector("samples").asInstanceOf[ListVector];list.startNewValue(0)
      val values=list.getDataVector.asInstanceOf[BigIntVector];values.setSafe(0,2);values.setSafe(1,3);list.endValue(0,2)
      val struct=root.getVector("station").asInstanceOf[StructVector];struct.setIndexDefined(0);struct.getChild("name").asInstanceOf[VarCharVector].setSafe(0,"A".getBytes("UTF-8"))
      root.getVector("raw").asInstanceOf[VarBinaryVector].setSafe(0,Array[Byte](0,1,-1))
      root.getVector("decimal").asInstanceOf[DecimalVector].setSafe(0,new java.math.BigDecimal("12.30"));root.setRowCount(1)
      encode(a,out,()=>(),7)(l=>{l.start(root);l.putNext();l.completed()})
      val json=Data.mapper.readTree(out.text)
      assert(json.path("samples").toString=="[2,3]" && json.path("station").path("name").asText=="A")
      assert(json.path("raw").asText=="AAH/" && json.path("decimal").decimalValue.compareTo(new java.math.BigDecimal("12.30"))==0)
    }finally {root.close();assert(a.getAllocatedMemory==0);a.close()}
  }
  test("dictionary records output values rather than dictionary indexes") {
    val a=new RootAllocator();val out=new Output()
    val encoding=new DictionaryEncoding(7,false,new ArrowType.Int(32,true))
    val root=VectorSchemaRoot.create(new Schema(Seq(new Field("label",new FieldType(true,new ArrowType.Int(32,true),encoding),null)).asJava),a)
    val values=new VarCharVector("dictionary",a);values.allocateNew();values.setSafe(0,"站点".getBytes("UTF-8"));values.setValueCount(1)
    val dict=new DictionaryProvider.MapDictionaryProvider(new Dictionary(values,encoding))
    try {
      root.allocateNew();root.getVector(0).asInstanceOf[IntVector].setSafe(0,0);root.setRowCount(1)
      encode(a,out,()=>(),7)(l=>{l.start(root,dict,IpcOption.DEFAULT);l.putNext();l.completed()})
      assert(out.text=="{\"label\":\"站点\"}\n")
    }finally {root.close();dict.close();assert(a.getAllocatedMemory==0);a.close()}
  }
  test("empty records produce empty bytes and errors never complete a partial result") {
    val a=new RootAllocator();val root=VectorSchemaRoot.create(new Schema(Seq(Records.field("text","utf8")).asJava),a)
    try {
      val empty=new Output();encode(a,empty,()=>(),7)(l=>{l.start(root);l.completed()})
      assert(empty.text.isEmpty && empty.completedCount==1)
      val failed=new Output()
      intercept[java.io.IOException] {
        encode(a,failed,()=>(),7)(l=>{l.start(root);Records.fill(root,Seq(Seq("partial")));l.putNext();throw new java.io.IOException("backend failed")})
      }
      assert(failed.completedCount==0)
      val cancelled=new Output();cancelled.onChunk=()=>cancelled.cancelled=true
      intercept[java.util.concurrent.CancellationException] {
        encode(a,cancelled,()=>(),7)(l=>{l.start(root);Records.fill(root,Seq(Seq("a long row to cancel")));l.putNext();l.completed()})
      }
      assert(cancelled.completedCount==0)
    }finally {root.close();assert(a.getAllocatedMemory==0);a.close()}
  }
  test("the common dataset adapter preserves native bytes and only synthesizes missing bytes") {
    val a=new RootAllocator();val out=new Output();var nativeCalls=0;var recordCalls=0;var closes=0
    val source=new BatchDatasetAccess {
      def describe(path:String):Item=Item(path,"custom",None,totalSizeBytes=Some(123),views=Some(if(path=="/records")Seq("records")else Seq("records","bytes")))
      def list():Seq[Item]=Seq(describe("/records"),describe("/native"))
      def list(path:String):Seq[Item]=list()
      def count(path:String):Option[Long]=Some(0)
      def read(path:String,view:String,allocator:org.apache.arrow.memory.BufferAllocator,l: BatchReceiver,ready:()=>Unit):Unit={
        if(path=="/native"){assert(view=="bytes");nativeCalls+=1;val r=VectorSchemaRoot.create(PluginSchemas.bytes,allocator);try l.start(r)finally r.close()}
        else {assert(view=="records");recordCalls+=1;val r=VectorSchemaRoot.create(new Schema(Seq.empty[Field].asJava),allocator);try {l.start(r);l.completed()}finally r.close()}
      }
      override def close():Unit={closes+=1}
    }
    val wrapped=ItemByteViews.wrap(source,_.views.get,7)
    try {
      assert(wrapped.describe("/records").views.contains(Seq("records","bytes")))
      assert(wrapped.list().head.totalSizeBytes.isEmpty && wrapped.list("/").last.totalSizeBytes.contains(123L))
      for(path<-Seq("/records","/native")){val stream=wrapped.requireView(path,DataViews.Bytes).open(AllBytes,StreamContext(a));try stream.consume(out)finally stream.close()}
      assert(nativeCalls==1 && recordCalls==1 && out.completedCount==0)
    }finally {wrapped.close();assert(closes==1);a.close()}
  }
}
