package dacp

import _root_.xstream.data._

import java.nio.{ByteBuffer,ByteOrder}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class DataStreamSpec extends AnyFunSuite {
  test("empty streams announce a descriptor, are single-use, and do not acquire resources before consumption") {
    val a=new RootAllocator();var acquired=0;var released=0;var descriptors=0
    val v=DataViews[ByteChunk,AllBytes.type]((_,c,o)=>{acquired+=1;try o.start(BytesDescriptor())finally released+=1})
    val unused=v.open(AllBytes,StreamContext(a));unused.close();assert(acquired==0)
    val s=v.open(AllBytes,StreamContext(a))
    val out=new DataConsumer[ByteChunk]{def start(d:DataDescriptor[ByteChunk]):Unit={descriptors+=1};def accept(b:ByteChunk):Unit=fail("unexpected data")}
    try {s.consume(out);assert(descriptors==1 && acquired==1 && released==1);intercept[IllegalArgumentException](s.consume(out))}finally {s.close();a.close()}
    intercept[IllegalArgumentException](DataStream[ByteChunk](_=>()).consume(out))
    intercept[IllegalArgumentException](DataStream[ByteChunk](o=>{o.start(BytesDescriptor());o.start(BytesDescriptor())}).consume(out))
    intercept[IllegalArgumentException](DataStream[ByteChunk](o=>o.accept(ByteChunk(Array.emptyByteArray,0,0))).consume(out))
  }
  test("byte slices are borrowed and consumer failure unwinds producer resources without more reads") {
    val a=new RootAllocator();val bytes=Array[Byte](9,1,2,9);var reads=0;var closed=false
    val v=DataViews[ByteChunk,AllBytes.type]((_,c,out)=>{
      try {out.start(BytesDescriptor());reads+=1;out.accept(ByteChunk(bytes,1,2));reads+=1}
      finally closed=true
    })
    val s=v.open(AllBytes,StreamContext(a))
    try intercept[java.io.IOException](s.consume(new DataConsumer[ByteChunk]{
      def start(d:DataDescriptor[ByteChunk]):Unit=()
      def accept(b:ByteChunk):Unit={assert((b.bytes eq bytes) && b.offset==1 && b.length==2);throw new java.io.IOException("consumer stopped")}
    }))finally {s.close();a.close()}
    assert(reads==1 && closed)
  }
  test("a third-party typed view passes through catalog wrappers without a core registry or records conversion") {
    final case class Spectrum(values:Vector[Double])
    final case class SpectrumDescriptor(unit:String) extends DataDescriptor[Spectrum]
    val token=new DataViewType[Spectrum,Int]("spectrum")
    val other=new DataViewType[Spectrum,Int]("spectrum")
    val a=new RootAllocator()
    val source=new DatasetAccess {
      def list():Seq[Item]=Seq(describe("/spectrum"))
      def list(p:String):Seq[Item]=list()
      def describe(p:String):Item=Item(p,"science",None,views=Some(Seq("spectrum")))
      def count(p:String):Option[Long]=None
      def view[K,S](p:String,t:DataViewType[K,S]):Option[DataView[K,S]]=if(t eq token)
        Some(DataViews[Spectrum,Int]((n,c,o)=>{o.start(SpectrumDescriptor("Hz"));o.accept(Spectrum(Vector.fill(n)(1d)))}).asInstanceOf[DataView[K,S]])else None
    }
    val access=ItemByteViews.wrap(source,_.views.get,16)
    try {
      assert(access.view("/spectrum",other).isEmpty && access.view("/spectrum",DataViews.Records).isEmpty)
      val stream=access.requireView("/spectrum",token).open(3,StreamContext(a))
      try stream.consume(new DataConsumer[Spectrum]{def start(d:DataDescriptor[Spectrum]):Unit=assert(d==SpectrumDescriptor("Hz"));def accept(s:Spectrum):Unit=assert(s.values.size==3)})finally stream.close()
    }finally {access.close();a.close()}
  }
  test("record batches preserve the producer root and empty schema while failures release Arrow allocations") {
    val a=new RootAllocator();val schema=new Schema(Seq(Records.field("temperature","float64")).asJava)
    var root:VectorSchemaRoot=null;var closed=false
    val v=DataViews[RecordBatch,Scan]((_,c,o)=>{
      root=VectorSchemaRoot.create(schema,c.allocator)
      try {o.start(RecordsDescriptor(schema));Records.fill(root,Seq(Seq(12.5)));o.accept(RecordBatch(root))}finally {root.close();closed=true}
    })
    val stream=v.open(Scan(),StreamContext(a))
    try intercept[java.util.concurrent.CancellationException](stream.consume(new DataConsumer[RecordBatch]{
      def start(d:DataDescriptor[RecordBatch]):Unit=assert(d.asInstanceOf[RecordsDescriptor].schema==schema)
      def accept(b:RecordBatch):Unit={assert(b.root eq root);throw new java.util.concurrent.CancellationException()}
    }))finally {stream.close();assert(closed && a.getAllocatedMemory==0);a.close()}
  }
  test("closing a consuming view cancels before the next source read and releases resources") {
    val a=new RootAllocator();var closed=false;var reads=0;var stream:DataStream[ByteChunk]=null
    val v=DataViews[ByteChunk,AllBytes.type]((_,context,out)=>{
      try {out.start(BytesDescriptor());reads+=1;out.accept(ByteChunk(Array[Byte](1),0,1));context.check();reads+=1}finally closed=true
    })
    stream=v.open(AllBytes,StreamContext(a))
    try intercept[java.util.concurrent.CancellationException](stream.consume(new DataConsumer[ByteChunk]{
      def start(d:DataDescriptor[ByteChunk]):Unit=()
      def accept(b:ByteChunk):Unit=stream.close()
    }))finally {stream.close();a.close()}
    assert(closed && reads==1)
  }
  test("Arrow transport adapts unrelated allocator roots with bounded buffer copies and releases both") {
    val producer=new RootAllocator();val receiver=new RootAllocator()
    val schema=new Schema(Seq(Records.field("temperature","int64")).asJava)
    val source=new BatchDatasetAccess {
      def list():Seq[Item]=Seq(describe("/table"))
      def list(p:String):Seq[Item]=list()
      def describe(p:String):Item=Item(p,"table",Some(schema),views=Some(Seq("records")))
      def count(p:String):Option[Long]=None
      def read(p:String,v:String,a:org.apache.arrow.memory.BufferAllocator,out:BatchReceiver,check:()=>Unit):Unit={
        val root=VectorSchemaRoot.create(schema,producer)
        try {out.start(root);for(i<-1 to 3){check();Records.fill(root,Seq(Seq(i.toLong)));out.putNext()}}finally root.close()
      }
    }
    var rows=0;var output:VectorSchemaRoot=null
    try BatchStreams.read(source,"/table","records",receiver,new BatchReceiver {
      def start(r:VectorSchemaRoot,d:org.apache.arrow.vector.dictionary.DictionaryProvider,o:org.apache.arrow.vector.ipc.message.IpcOption):Unit={output=r;assert(r.getSchema==schema)}
      def putNext():Unit={rows+=output.getRowCount;assert(output.getVector(0).getObject(0)==Long.box(rows.toLong))}
    },()=>())finally {assert(producer.getAllocatedMemory==0 && receiver.getAllocatedMemory==0);producer.close();receiver.close()}
    assert(rows==3)
  }
  test("scientific blocks preserve axes, coordinate references, image channels and chunked sequence identity") {
    val encoding=NumericEncoding("float32",ByteOrder.LITTLE_ENDIAN)
    val dims=Vector(Dimension("time",10,Some("/time"),Some("s")),Dimension("lat",3),Dimension("lon",4))
    val descriptor=ArrayDescriptor(dims,encoding)
    val section=ArraySection(Vector(2,0,0),Vector(1,3,4),Vector(1,1,1));section.validate(dims)
    val chunk=ArrayChunk(section,encoding,ByteBuffer.allocate(48))
    assert(descriptor.dimensions.head.coordinateItem.contains("/time") && chunk.section.origin.head==2)
    intercept[IllegalArgumentException](section.copy(origin=Vector(10,0,0)).validate(dims))
    intercept[IllegalArgumentException](ArrayChunk(section,encoding,ByteBuffer.allocate(12)))
    val image=ImageTile(ImageRegion(10,20,2,3),3,NumericEncoding("uint8",ByteOrder.BIG_ENDIAN),ByteBuffer.allocate(18))
    assert(image.channels==3 && image.region.y==20)
    val parts=Seq(SequenceChunk("chr1",0,"ACGT",false),SequenceChunk("chr1",4,"NN",true))
    assert(parts.last.offset==4 && parts.map(_.symbols.toString).mkString=="ACGTNN" && parts.last.last)
    assert(SequenceChunk("empty",0,"",true).last)
  }
}
