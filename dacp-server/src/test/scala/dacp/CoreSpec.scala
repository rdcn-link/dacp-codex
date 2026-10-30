package dacp

import _root_.xstream.data._
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.apache.arrow.vector.VectorSchemaRoot
import dacp.protocol.Metadata
class CoreSpec extends AnyFunSuite {
  private val plugins=new PluginEnvironment()
  private val Formats=plugins.formats
  private val Containers=plugins.containers

  def temp(test: Path => Unit): Unit = { val p = Files.createTempDirectory("dacp-test"); try test(p) finally Data.deleteTree(p) }
  test("directory recursively flattens files and rejects escapes") { temp { p =>
    Files.createDirectories(p.resolve("a/b")); Files.writeString(p.resolve("a/b/x.csv"),"id\n1\n2\n")
    val a = new DirectoryAdapter(p,2,1,plugins=plugins)
    assert(a.list().map(_.path) == Seq("a/b/x.csv"))
    Files.writeString(p.resolve("_schema.json"),"{}")
    assert(a.list().map(_.path).contains("_schema.json"))
    assert(!new DirectoryAdapter(p,2,1,excludeMetadata=true,plugins=plugins).list().exists(_.path == "_schema.json"))
    intercept[IllegalArgumentException](a.resolve("../secret"))
    intercept[IllegalArgumentException](a.resolve("/etc/passwd"))
  }}
  test("schema sampling fixes types and rejects later conflicts") { temp { p =>
    val f = p.resolve("x.csv"); Files.writeString(f,"id,value\n1,2.5\n2,wrong\n")
    val s = Formats.detect(ByteSource.local(f)).schema(ByteSource.local(f),FileOptions(),1,1)
    val a = new RootAllocator(); val root = VectorSchemaRoot.create(s,a)
    try { Records.fill(root,Seq(Seq("1","2.5"))); intercept[IllegalArgumentException](Records.fill(root,Seq(Seq("2","wrong")))) }
    finally {root.close(); a.close()}
    intercept[IllegalArgumentException](Formats.detect(ByteSource.local(f)).schema(ByteSource.local(f),FileOptions(),2,1))
    val explicit = new org.apache.arrow.vector.types.pojo.Schema(List(Records.field("id","int64"),Records.field("value","utf8")).asJava)
    assert(Formats.detect(ByteSource.local(f)).schema(ByteSource.local(f),FileOptions(columns=Some(explicit)),2,1) == explicit)
  }}
  test("protobuf metadata and descriptor namespaces round-trip") { temp { p =>
    Files.writeString(p.resolve("x.csv"),"id\n1\n")
    val a = new RootAllocator()
    val c = Config("localhost",8815,"localhost",Map("d" -> FileDatasets.mixed(Seq(PublishedItem("/records", file = Some(p.resolve("x.csv")))))),p,p,p)
    val producer = new Producer(c,a)
    try {
      val info = producer.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","d"))
      val meta = Metadata.DataSet.parseFrom(info.getAppMetadata)
      assert(meta.getItems(0).getPath == "/records")
      val item = producer.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","d","/records"))
      val ticket = Metadata.Ticket.parseFrom(item.getEndpoints.get(0).getTicket.getBytes)
      assert(ticket.getDataset == "d" && ticket.getItem == "/records")
      intercept[FlightRuntimeException](producer.getFlightInfo(TestCalls.context,FlightDescriptor.path("wrong","d")))
      assert(c.hasDataset("d") && !c.hasDataset("d2"))
    } finally {a.close()}
  }}
  test("Parquet range reads preserve schema and record batches") { temp { p =>
    val path=p.resolve("sample.parquet")
    val schema=new org.apache.arrow.vector.types.pojo.Schema(List(Records.field("id","int64"),Records.field("amount","float64")).asJava)
    val avro=Parquet.avro(schema)
    val writer=org.apache.parquet.avro.AvroParquetWriter.builder[org.apache.avro.generic.GenericRecord](new org.apache.parquet.io.LocalOutputFile(path))
      .withConf(new org.apache.parquet.conf.PlainParquetConfiguration()).withSchema(avro)
      .withExtraMetaData(Map("dacp.arrow.schema" -> schema.toJson).asJava).build()
    try (1 to 2).foreach { id => val row=new org.apache.avro.generic.GenericData.Record(avro);row.put("id",id.toLong);row.put("amount",id+0.5);writer.write(row) } finally writer.close()
    var ranges=0
    val local=ByteSource.local(path)
    val remote=new ByteSource {
      val name="remote.parquet"
      def size=local.size
      def open(check:()=>Unit)=local.open(check)
      override val supportsRanges=true
      override def readRange(offset:Long,length:Long,check:()=>Unit)={ranges+=1;local.readRange(offset,length,check)}
    }
    assert(remote.localPath.isEmpty && Parquet.schema(remote)==schema && Parquet.recordCount(remote)==2L && ranges>0)
    val allocator=new RootAllocator();var rows=0
    try Parquet.stream(remote,allocator,1)(_=>(),r=>rows+=r.getRowCount,()=>()) finally allocator.close()
    assert(rows==2)
  }}
}
