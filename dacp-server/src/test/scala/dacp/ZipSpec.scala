package dacp
import java.nio.file.{Files,Path}
import java.util.zip.{ZipEntry,ZipOutputStream}
import scala.jdk.CollectionConverters._
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata
class ZipSpec extends AnyFunSuite {
  def write(p: Path, entries: Seq[(String,String)]): Unit = {
    val out = new ZipOutputStream(Files.newOutputStream(p))
    try entries.foreach { case (n,v) => out.putNextEntry(new ZipEntry(n)); out.write(v.getBytes("UTF-8")); out.closeEntry() } finally out.close()
  }
  test("ZIP Flight hierarchy, records and original bytes with unknown folder properties") {
    val dir = Files.createTempDirectory("zip-test-"); val allocator = new RootAllocator()
    try {
      val file = dir.resolve("private.ZIP")
      write(file,Seq("reports/orders.csv" -> "id\n1\n2\n","empty/" -> "","nested.zip" -> "opaque"))
      val config = Config("localhost",8815,"localhost",Map("public" -> DatasetDefinition.custom(Seq(PublishedItem("/archive",file=Some(file))))),Seq("dacp://localhost:8815/public"),dir,dir,dir)
      val ns = config.openDataset("public")
      val folder = ns.list("/").head
      assert(folder.kind == "folder" && folder.format == "zip" && folder.totalSizeBytes.isEmpty && folder.lastModifiedTimeMs.isEmpty && folder.recordCount.isEmpty)
      assert(ns.list("/archive").map(_.path) == Seq("/archive/empty","/archive/nested.zip","/archive/reports"))
      assert(ns.count("/archive/reports/orders.csv").contains(2L))
      assert(ns.describe("/archive/nested.zip").format == "bin")
      assert(ns.count("/archive").isEmpty && ns.list("/archive/empty").isEmpty)
      val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(config,allocator)).build().start()
      val client = FlightClient.builder(allocator,server.getLocation).build()
      try {
        val info = client.getInfo(FlightDescriptor.path("datasets","public","/archive"))
        assert(Metadata.Item.parseFrom(info.getAppMetadata).getViewsList.asScala == Seq("bytes"))
        val bytes = new java.io.ByteArrayOutputStream(); val stream = client.getStream(info.getEndpoints.get(0).getTicket)
        try while(stream.next()) { val root = stream.getRoot; (0 until root.getRowCount).foreach(i => bytes.write(root.getVector(0).asInstanceOf[VarBinaryVector].get(i))) } finally stream.close()
        assert(bytes.toByteArray.sameElements(Files.readAllBytes(file)))
        val records = client.getInfo(FlightDescriptor.path("datasets","public","/archive/reports/orders.csv"))
        val rows = client.getStream(records.getEndpoints.get(0).getTicket)
        try { var n = 0; while(rows.next()) n += rows.getRoot.getRowCount; assert(n == 2) } finally rows.close()
        intercept[FlightRuntimeException](client.getInfo(FlightDescriptor.path("datasets","public","/archive/empty")))
      } finally { client.close(); server.close() }
    } finally { allocator.close(); Data.deleteTree(dir) }
  }
  test("unsafe ZIP names, file parents and alias collisions are rejected; staged files are cleaned") {
    val dir = Files.createTempDirectory("zip-invalid-")
    try {
      val file = dir.resolve("a.zip")
      for(n <- Seq("../escape","/absolute","a//b","a/./b","a\\b")) { write(file,Seq(n -> "x")); intercept[IllegalArgumentException](ZipArchive.index(file)) }
      write(file,Seq("a" -> "x","a/b" -> "y")); intercept[IllegalArgumentException](ZipArchive.index(file))
      write(file,Seq("a.csv" -> "id\n1\n"))
      val raw = Files.readAllBytes(file)
      val central = raw.indices.find(i => i+4 <= raw.length && raw.slice(i,i+4).sameElements(Array[Byte](80,75,1,2))).get
      java.nio.ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(central+24,(ZipArchive.MaxEntryBytes+1).toInt)
      Files.write(file,raw)
      intercept[IllegalArgumentException](ZipArchive.index(file))
      write(file,Seq("a.csv" -> "id\n1\n"))
      val config = Config("localhost",8815,"localhost",Map.empty,Seq.empty,dir,dir,dir)
      intercept[IllegalArgumentException] { new NamespaceAdapter(Seq(PublishedItem("/zip",file=Some(file)),PublishedItem("/zip/a.csv",file=Some(file))),config) }
      var staged: Path = null
      ZipArchive.withFile(file,"a.csv",() => ()) { p => staged=p; assert(Files.exists(p)) }; assert(!Files.exists(staged))
      intercept[java.util.concurrent.CancellationException] { ZipArchive.withFile(file,"a.csv",() => throw new java.util.concurrent.CancellationException())(_ => fail("must cancel")) }
    } finally Data.deleteTree(dir)
  }
}
