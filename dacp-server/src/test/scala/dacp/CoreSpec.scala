package dacp
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.apache.arrow.vector.VectorSchemaRoot
import dacp.protocol.Metadata
class CoreSpec extends AnyFunSuite {
  def temp(test: Path => Unit): Unit = { val p = Files.createTempDirectory("dacp-test"); try test(p) finally Data.deleteTree(p) }
  test("directory recursively flattens files and rejects escapes") { temp { p =>
    Files.createDirectories(p.resolve("a/b")); Files.writeString(p.resolve("a/b/x.csv"),"id\n1\n2\n")
    val a = new DirectoryAdapter(p,2,1)
    assert(a.list().map(_.path) == Seq("a/b/x.csv"))
    Files.writeString(p.resolve("_schema.json"),"{}")
    assert(a.list().map(_.path).contains("_schema.json"))
    assert(!new DirectoryAdapter(p,2,1,excludeMetadata=true).list().exists(_.path == "_schema.json"))
    intercept[IllegalArgumentException](a.resolve("../secret"))
    intercept[IllegalArgumentException](a.resolve("/etc/passwd"))
  }}
  test("expiry deletes output and retains explicit expired status") { temp { p =>
    val dir = Files.createDirectory(p.resolve("out")); val r = new OutputRegistry(20)
    try { r.register("s1",dir); assert(r.lookup("s1").contains(dir)); Thread.sleep(50)
      intercept[Expired](r.lookup("s1")); assert(!Files.exists(dir))
    } finally r.close()
  }}
  test("schema sampling fixes types and rejects later conflicts") { temp { p =>
    val f = p.resolve("x.csv"); Files.writeString(f,"id,value\n1,2.5\n2,wrong\n")
    val s = Data.schema(f,1,1)
    val a = new RootAllocator(); val root = VectorSchemaRoot.create(s,a)
    try { Data.fill(root,Seq(Seq("1","2.5"))); intercept[IllegalArgumentException](Data.fill(root,Seq(Seq("2","wrong")))) }
    finally {root.close(); a.close()}
    intercept[IllegalArgumentException](Data.schema(f,2,1))
    val explicit = new org.apache.arrow.vector.types.pojo.Schema(List(Data.field("id","int64"),Data.field("value","utf8")).asJava)
    assert(Data.schema(f,2,1,Some(explicit)) == explicit)
  }}
  test("protobuf metadata and descriptor namespaces round-trip") { temp { p =>
    Files.writeString(p.resolve("x.csv"),"id\n1\n")
    val a = new RootAllocator(); val r = new OutputRegistry(10000)
    val c = Config("localhost",8815,"localhost",Map("d" -> DatasetDefinition.custom(Seq(PublishedItem("/records", file = Some(p.resolve("x.csv")))))),Seq("dacp://localhost:8815/d"),p,p,p)
    val producer = new Producer(c,a,r)
    try {
      val info = producer.getFlightInfo(null,FlightDescriptor.path("datasets","d"))
      val meta = Metadata.DataSet.parseFrom(info.getAppMetadata)
      assert(meta.getItems(0).getPath == "/records")
      val item = producer.getFlightInfo(null,FlightDescriptor.path("datasets","d","/records"))
      val ticket = Metadata.Ticket.parseFrom(item.getEndpoints.get(0).getTicket.getBytes)
      assert(ticket.getDataset == "d" && ticket.getItem == "/records")
      intercept[FlightRuntimeException](producer.getFlightInfo(null,FlightDescriptor.path("wrong","d")))
      intercept[IllegalArgumentException](c.authorize("d2","x"))
      val o = Files.createTempDirectory("output-test"); Files.writeString(o.resolve("x.csv"),"id\n1\n")
      r.register("s1",o)
      intercept[FlightRuntimeException](producer.getFlightInfo(null,FlightDescriptor.path("datasets","s1")))
    } finally {r.close();a.close()}
  }}
  test("DAG rejects cycles and orders dependencies") { temp { p =>
    val f=p.resolve("job.yaml")
    Files.writeString(f,"job: test\ntasks:\n - {id: b, input: s1, operator: x@1, output: s2}\n - {id: a, input: 'dacp://localhost/d', operator: x@1, output: s1}\n")
    assert(Jobs.load(f).map(_.id) == Seq("a","b"))
    Files.writeString(f,"job: test\ntasks:\n - {id: b, input: s1, operator: x@1, output: s2}\n - {id: a, input: s2, operator: x@1, output: s1}\n")
    intercept[IllegalArgumentException](Jobs.load(f))
  }}
  test("converter streams binary Arrow input to parquet with schema metadata") { temp { p =>
    val input=Files.createDirectory(p.resolve("in")); val output=p.resolve("out")
    Files.writeString(input.resolve("manifest.json"),"""[{"file":"0.arrow","format":"csv"}]""")
    Files.writeString(input.resolve("params.json"),"""{"to":"parquet","compression":"none","schema":"infer","row_group_size":"1MB"}""")
    val a=new RootAllocator(); val root=VectorSchemaRoot.create(Data.bytesSchema,a)
    val writer=new org.apache.arrow.vector.ipc.ArrowStreamWriter(root,null,Files.newOutputStream(input.resolve("0.arrow")))
    try {
      writer.start()
      Seq("id,amount\n1,2.","5\n2,3.5\n").foreach { text => root.clear();root.allocateNew()
        root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector].setSafe(0,text.getBytes(java.nio.charset.StandardCharsets.UTF_8));root.setRowCount(1);writer.writeBatch() }
      writer.end()
    } finally {writer.close();root.close();a.close()}
    FormatConvert.run(input,output,input.resolve("params.json"))
    val boundary = new RootAllocator()
    try { OperatorOutput.encode(output,boundary); assert(!Files.exists(output.resolve("part-0.parquet"))); OperatorOutput.decode(output,boundary) }
    finally boundary.close()
    assert(Files.exists(output.resolve("_schema.json")))
    assert(Parquet.schema(output.resolve("part-0.parquet")).getFields.get(0).getName == "id")
    assert(Parquet.recordCount(output.resolve("part-0.parquet")) == 2L)
    val readAllocator=new RootAllocator(); var rows=0
    try Parquet.stream(output.resolve("part-0.parquet"),readAllocator,1)(_ => (),r => rows+=r.getRowCount,() => ()) finally readAllocator.close()
    assert(rows == 2)
    Files.writeString(input.resolve("params.json"),"""{"to":"parquet","schema":{"id":"bool","amount":"float64"}}""")
    intercept[IllegalArgumentException](FormatConvert.run(input,p.resolve("invalid-output"),input.resolve("params.json")))
  }}

  test("Arrow byte adapter keeps EOF terminal without reading beyond IPC EOS") {
    val a = new RootAllocator(); val root = VectorSchemaRoot.create(Data.bytesSchema,a)
    val bytes = new java.io.ByteArrayOutputStream()
    val writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root,null,bytes)
    try {
      writer.start(); root.allocateNew()
      root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector].setSafe(0,Array[Byte](1,2))
      root.setRowCount(1);writer.writeBatch();writer.end();writer.close()
      val input = new java.io.ByteArrayInputStream(bytes.toByteArray) {
        override def read(b: Array[Byte],off: Int,len: Int): Int = {
          assert(available() > 0,"attempted read after Arrow EOS")
          super.read(b,off,len)
        }
      }
      val reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(input,a)
      try { val view = new FormatConvert.BinaryInput(reader)
        assert(view.readAllBytes().toSeq == Seq[Byte](1,2)); assert(view.read() == -1); assert(view.read() == -1)
      } finally reader.close()
    } finally {root.close();a.close()}
  }

}
