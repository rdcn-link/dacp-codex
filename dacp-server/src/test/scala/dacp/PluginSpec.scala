package dacp

import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.apache.arrow.vector.VarBinaryVector
import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

class PluginSpec extends AnyFunSuite {
  test("independent example plugins and registered containers work through the Flight namespace") {
    val dir = Files.createTempDirectory("plugin-contract-")
    val allocator = new RootAllocator()
    Files.writeString(dir.resolve("private.message"),"native child")
    Files.writeString(dir.resolve("private.testpack"),"id\n1\n2\n")
    val c = Config("localhost",8815,"localhost",Map(
      "native" -> DatasetDefinition("example-message",Map("message" -> "native dataset")),
      "files" -> DatasetDefinition.custom(Seq(
        PublishedItem("/science",file=Some(dir.resolve("private.message")),options=FileOptions(parameters=Map("prefix" -> "hello "))),
        PublishedItem("/archive",file=Some(dir.resolve("private.testpack"))))),
      "directory" -> DatasetDefinition("directory",Map("root" -> dir.toString))),
      Seq("dacp://localhost:8815/"),dir,dir,dir)
    val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(c,allocator)).build().start()
    val client = FlightClient.builder(allocator,server.getLocation).build()
    def read(ds: String, path: String): String = {
      val info = client.getInfo(FlightDescriptor.path("datasets",ds,path))
      val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder.setView("bytes").build()
      val stream = client.getStream(new Ticket(ticket.toByteArray))
      try {
        val out = new java.io.ByteArrayOutputStream()
        while(stream.next()) {
          val v = stream.getRoot.getVector(0).asInstanceOf[VarBinaryVector]
          (0 until stream.getRoot.getRowCount).foreach(i => out.write(v.get(i)))
        }
        out.toString("UTF-8")
      } finally stream.close()
    }
    try {
      assert(read("native","/message") == "native dataset")
      assert(read("files","/science/message") == "hello native child")
      assert(read("files","/science") == "native child")
      assert(read("directory","/private.message/message") == "native child")
      assert(read("files","/archive/nested/entry.csv") == "id\n1\n2\n")
      val summaryInfo = client.getInfo(FlightDescriptor.path("datasets","files","/science/summary"))
      val summaryQuery = Metadata.Scan.newBuilder().setLimit(1)
        .setProjection(Metadata.Scan.Projection.newBuilder().addColumns("characters"))
        .setPredicate(Metadata.Predicate.newBuilder().setOp("gt").setColumn("characters")
          .addValues(Metadata.Literal.newBuilder().setType("integer").setValue("1")))
      val summaryTicket = Metadata.Ticket.parseFrom(summaryInfo.getEndpoints.get(0).getTicket.getBytes).toBuilder
        .setView("records").setScan(summaryQuery).build()
      val summaryStream = client.getStream(new Ticket(summaryTicket.toByteArray))
      try {
        assert(summaryStream.next())
        assert(summaryStream.getRoot.getVector("characters").asInstanceOf[org.apache.arrow.vector.BigIntVector].get(0) == "hello native child".length.toLong)
        assert(!summaryStream.next())
      } finally summaryStream.close()
      val ds = c.openDataset("files")
      try {
        PluginChecks.listing(ds)
        assert(ds.list().map(_.path) == Seq("/archive","/archive/nested","/archive/nested/entry.csv","/science","/science/message","/science/summary"))
        assert(ds.list("/science").map(_.path) == Seq("/science/message","/science/summary"))
        assert(ds.describe("/science").kind == "folder")
        assert(ds.describe("/science").recordCount.isEmpty)
        assert(ds.count("/science").isEmpty)
        assert(ds.count("/archive/nested/entry.csv").contains(2L))
        intercept[java.io.FileNotFoundException](ds.describe("/science/missing"))
      } finally ds.close()
      val listing = Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets","files")).getAppMetadata)
      assert(!listing.toString.contains("private.message") && !listing.toString.contains("hello "))
    } finally { client.close(); server.close(); c.close(); allocator.close(); Data.deleteTree(dir) }
  }
  test("native child access closes on success, failure, and cancellation") {
    val dir = Files.createTempDirectory("plugin-lifecycle-")
    try {
      val file = dir.resolve("private.tracked"); Files.writeString(file,"message")
      val c = Config("localhost",8815,"localhost",Map.empty,Seq.empty,dir,dir,dir)
      var cancel = false
      val ns = new NamespaceAdapter(Seq(PublishedItem("/science",file=Some(file))),c,
        () => if(cancel) throw new java.util.concurrent.CancellationException())
      val before = TestTrackedHandler.opened.get()
      assert(ns.describe("/science/message").path == "/science/message")
      intercept[java.io.FileNotFoundException](ns.describe("/science/missing"))
      assert(TestTrackedHandler.opened.get() == before + 2)
      assert(TestTrackedHandler.opened.get() == TestTrackedHandler.closed.get())
      val handler = Formats.byId("test-tracked").asInstanceOf[StructuredFileHandler]
      val access = handler.openStructure(file,FileOptions(),c,() => ())
      val allocator = new RootAllocator()
      try {
        intercept[java.util.concurrent.CancellationException] {
          access.stream("/message","bytes",allocator,null,() => throw new java.util.concurrent.CancellationException())
        }
      } finally { access.close(); allocator.close() }
      cancel = true
      intercept[java.util.concurrent.CancellationException](ns.list("/science"))
      assert(TestTrackedHandler.opened.get() == TestTrackedHandler.closed.get())
    } finally Data.deleteTree(dir)
  }
  test("handler-owned parameters are validated without exposing their values") {
    val handler = Formats.byId("example-message-file")
    handler.validateOptions(FileOptions(parameters=Map("prefix" -> "hello")))
    intercept[IllegalArgumentException](handler.validateOptions(FileOptions(parameters=Map("unknown" -> "secret"))))
    intercept[IllegalArgumentException](handler.validateOptions(FileOptions(parameters=Map("prefix" -> Int.box(2)))))
    assert(!FileOptions(parameters=Map("prefix" -> "secret")).toString.contains("secret"))
    assert(Formats.byId("test-container").id == "test-container")
  }
}
