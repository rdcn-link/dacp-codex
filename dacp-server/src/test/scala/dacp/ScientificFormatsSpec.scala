package dacp

import _root_.xstream.data._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import java.nio.file.Files
import java.nio.{ByteBuffer,ByteOrder}
import scala.jdk.CollectionConverters._

class ScientificFormatsSpec extends AnyFunSuite {
  private def using(f:(DatasetContext,RootAllocator)=>Unit):Unit={
    val environment=new PluginEnvironment();val allocator=new RootAllocator()
    val context=new DatasetContext {
      val batchSize=16;val inferSample=16;val byteChunk=16;val countFileRecordsOnStartup=false
      val plugins=environment;val sources=Map.empty[String,SourceDefinition]
      def createDataset(d:DatasetDefinition):PublishedDataset=throw new UnsupportedOperationException()
    }
    try f(context,allocator)finally {allocator.close();environment.close()}
  }
  test("FASTA selection preserves sequence IDs, offsets, final markers and bounded blocks") {using { (c,a)=>
    val p=Files.createTempFile("sequences-",".fa")
    Files.writeString(p,">chr1 description\nACGTACGT\n>empty\n>chr2\nNNNN\n")
    val access=new _root_.xstream.data.fasta.FastaFormat().openStructure(ByteSource.local(p),FileOptions(parameters=Map("chunk_symbols"->Int.box(3))),c,()=>())
    val chunks=scala.collection.mutable.ArrayBuffer.empty[SequenceChunk]
    try {
      assert(access.list("/").map(_.path)==Seq("/sequences"))
      val s=access.requireView("/sequences",ScientificViews.Sequences).open(SequenceSelection(Set("chr1"),2,Some(5)),StreamContext(a))
      s.consume(new DataConsumer[SequenceChunk]{def start(d:DataDescriptor[SequenceChunk]):Unit=assert(d.isInstanceOf[SequenceDescriptor]);def accept(b:SequenceChunk):Unit=chunks+=b})
      assert(chunks.map(_.symbols.toString).mkString=="GTACG")
      assert(chunks.head.offset==2 && chunks.last.last && chunks.forall(_.symbols.length<=3))
    }finally {access.close();Files.delete(p)}
  }}
  test("FITS image sections retain axes and raw values without flattening to records") {using { (c,a)=>
    val p=Files.createTempFile("image-",".fits");Files.delete(p)
    val file=new nom.tam.fits.Fits();file.addHDU(nom.tam.fits.Fits.makeHDU(Array(Array[Short](1,2,3),Array[Short](4,5,6))));file.write(p.toFile);file.close()
    val access=new _root_.xstream.data.fits.FitsFormat().openStructure(ByteSource.local(p),FileOptions(),c,()=>())
    try {
      assert(access.describe("/hdu0").views.get==Seq("array","image"))
      val s=access.requireView("/hdu0",ScientificViews.Arrays).open(ArraySelection(Some(ArraySection(Vector(0,1),Vector(2,1),Vector(1,1)))),StreamContext(a))
      val values=scala.collection.mutable.ArrayBuffer.empty[Short]
      s.consume(new DataConsumer[ArrayChunk]{def start(d:DataDescriptor[ArrayChunk]):Unit=assert(d.asInstanceOf[ArrayDescriptor].dimensions.map(_.length)==Vector(2,3));def accept(b:ArrayChunk):Unit={val v=b.values.duplicate().order(b.encoding.byteOrder);while(v.hasRemaining)values+=v.getShort()}})
      assert(values.toVector==Vector[Short](2,5))
    }finally {access.close();Files.delete(p)}
  }}
  test("NetCDF variables expose dimensions and strided bounded array chunks") {using { (c,a)=>
    val p=Files.createTempFile("climate-",".nc")
    val builder=ucar.nc2.write.NetcdfFormatWriter.createNewNetcdf3(p.toString)
    builder.addDimension("time",4);builder.addVariable("temperature",ucar.ma2.DataType.INT,"time")
    val writer=builder.build()
    try writer.write("temperature",ucar.ma2.Array.factory(ucar.ma2.DataType.INT,Array(4),Array(10,20,30,40)))finally writer.close()
    val access=new _root_.xstream.data.netcdf.NetcdfFormat().openStructure(ByteSource.local(p),FileOptions(),c,()=>())
    try {
      val s=access.requireView("/temperature",ScientificViews.Arrays).open(ArraySelection(Some(ArraySection(Vector(1),Vector(2),Vector(2)))),StreamContext(a))
      val values=scala.collection.mutable.ArrayBuffer.empty[Int]
      s.consume(new DataConsumer[ArrayChunk]{def start(d:DataDescriptor[ArrayChunk]):Unit=assert(d.asInstanceOf[ArrayDescriptor].dimensions.head.name=="time");def accept(b:ArrayChunk):Unit={val v=b.values.duplicate().order(b.encoding.byteOrder);while(v.hasRemaining)values+=v.getInt()}})
      assert(values.toVector==Vector(20,40))
    }finally {access.close();Files.delete(p)}
  }}
  test("published scientific files remain browsable and typed views survive namespace routing") {using { (c,a)=>
    val p=Files.createTempFile("published-",".fa");Files.writeString(p,">s\nACGT\n")
    val access=new NamespaceAdapter(Seq(PublishedItem("/genome",file=Some(p),options=FileOptions(format=Some("fasta")))),c)
    try {
      assert(access.describe("/genome").kind=="folder")
      assert(access.list("/genome").map(_.path)==Seq("/genome/sequences"))
      assert(access.describe("/genome/sequences").views.contains(Seq("sequence")))
      var count=0
      val stream=access.requireView("/genome/sequences",ScientificViews.Sequences).open(SequenceSelection(),StreamContext(a))
      stream.consume(new DataConsumer[SequenceChunk]{def start(d:DataDescriptor[SequenceChunk]):Unit=();def accept(b:SequenceChunk):Unit=count+=b.symbols.length})
      assert(count==4)
    }finally {access.close();Files.delete(p)}
  }}
  test("scientific codecs are discovered and preserve descriptors and payloads") {using { (c,a)=>
    val codec=new _root_.xstream.data.scientific.ArrayCodec()
    assert(DataViewCodecs.all(c.plugins).map(_.kind.name).toSet==Set("array","image","sequence"))
    val encoding=NumericEncoding("int32",ByteOrder.LITTLE_ENDIAN)
    val d=ArrayDescriptor(Vector(Dimension("x",3)),encoding)
    assert(codec.descriptor(codec.descriptor(d))==d)
    val b=ArrayChunk(ArraySection(Vector(1),Vector(2),Vector(1)),encoding,ByteBuffer.allocate(8).order(encoding.byteOrder).putInt(20).putInt(30).flip())
    val decoded=codec.decode(codec.encode(b),d)
    assert(decoded.section==b.section && decoded.values==b.values)
    intercept[IllegalArgumentException](codec.decode(EncodedDataBlock(codec.encode(b).metadata,Array[Byte](1)),d))
  }}
}
