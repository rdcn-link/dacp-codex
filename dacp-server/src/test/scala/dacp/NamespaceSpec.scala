package dacp

import java.nio.file.{Files, Path}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

class NamespaceSpec extends AnyFunSuite {
  def fixture(test: (Config, Producer, OutputRegistry) => Unit): Unit = {
    val dir = Files.createTempDirectory("private-storage-")
    val file = dir.resolve("secret-orders.csv")
    Files.writeString(file,"id\n1\n")
    Files.writeString(dir.resolve("unpublished.csv"),"hidden\n1\n")
    val entries = Seq(PublishedItem("/events",file=Some(file)), PublishedItem("/nested/orders",file=Some(file)),
      PublishedItem("/nested/archive/old",file=Some(file)), PublishedItem("/empty",kind="folder"))
    val config = Config("localhost",8815,"localhost",Map("public"->entries),Seq("dacp://localhost:8815/public"),dir,dir,dir)
    val allocator = new RootAllocator(); val outputs = new OutputRegistry(10000)
    try test(config,new Producer(config,allocator,outputs),outputs)
    finally { outputs.close(); allocator.close(); Data.deleteTree(dir) }
  }
  def list(producer: Producer, parts: String*): Seq[Metadata.Item] =
    Metadata.DataSet.parseFrom(producer.getFlightInfo(null,FlightDescriptor.path(parts:_*)).getAppMetadata).getItemsList.asScala.toVector

  test("unknown datasets report a wrong name while existing datasets retain authorization") { fixture { (c,p,outputs) =>
    val requests = Seq(Seq("datasets", "missing"), Seq("datasets", "missing", "list", "/"),
      Seq("datasets", "missing", "/orders"), Seq("datasets", "missing", "count", "/orders"))
    requests.foreach { parts =>
      val error = intercept[FlightRuntimeException](p.getFlightInfo(null,FlightDescriptor.path(parts:_*)))
      assert(error.status().code() == FlightStatusCode.NOT_FOUND)
      assert(error.getMessage.contains("dataset not found") && error.getMessage.contains("dataset name"))
    }
    val allocator = new RootAllocator()
    try {
      val restricted = new Producer(c.copy(prefixes=Seq("dacp://localhost:8815/other")),allocator,outputs)
      val error = intercept[FlightRuntimeException](restricted.getFlightInfo(null,FlightDescriptor.path("datasets","public")))
      assert(error.status().code() == FlightStatusCode.UNAUTHORIZED)
    } finally allocator.close()
  }}

  test("recursive listing and immediate folder children use absolute logical paths") { fixture { (_,p,_) =>
    val all = list(p,"datasets","public")
    assert(all.map(_.getPath) == Seq("/empty","/events","/nested","/nested/archive","/nested/archive/old","/nested/orders"))
    val root = list(p,"datasets","public","list","/")
    assert(root.map(i => i.getPath -> i.getKind) == Seq("/empty"->"folder","/events"->"data","/nested"->"folder"))
    assert(list(p,"datasets","public","list","/nested").map(_.getPath) == Seq("/nested/archive","/nested/orders"))
    assert(list(p,"datasets","public","list","/empty").isEmpty)
    assert(all.find(_.getKind == "folder").get.getViewsCount == 0)
    assert(all.find(_.getKind == "data").get.getViewsList.asScala == Seq("records","bytes"))
  }}
  test("folder streams, data listings, relative paths and traversal are rejected") { fixture { (_,p,_) =>
    for (parts <- Seq(Seq("datasets","public","/"), Seq("datasets","public","/nested"), Seq("datasets","public","list","/events"),
      Seq("datasets","public","list","/missing"), Seq("datasets","public","events"),
      Seq("datasets","public","/../events"), Seq("datasets","public","/nested//orders"))) {
      intercept[FlightRuntimeException](p.getFlightInfo(null,FlightDescriptor.path(parts:_*)))
    }
  }}
  test("metadata and tickets do not disclose physical names; unpublished resources are inaccessible") { fixture { (_,p,_) =>
    val info = p.getFlightInfo(null,FlightDescriptor.path("datasets","public","/nested/orders"))
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    assert(metadata.getPath == "/nested/orders" && metadata.getKind == "data")
    val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes)
    assert(ticket.getItem == "/nested/orders")
    assert(!metadata.toString.contains("secret-orders") && !ticket.toString.contains("secret-orders"))
    for(path <- Seq("/secret-orders.csv","/unpublished.csv")) {
      val error = intercept[FlightRuntimeException](p.getFlightInfo(null,FlightDescriptor.path("datasets","public",path)))
      assert(!error.getMessage.contains("private-storage") && !error.getMessage.contains(path))
    }
  }}
  test("invalid mappings reject duplicate aliases, data parents, and unknown sources") { fixture { (c,_,_) =>
    val data = c.datasets("public").head
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(data,data),c))
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(data,PublishedItem("/events/child",kind="folder")),c))
    intercept[IllegalArgumentException](PublishedItem("relative",kind="folder"))
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(PublishedItem("/data",sql=Some("unknown"),table=Some("SECRET"))),c))
  }}
  test("missing private files produce sanitized client errors") { fixture { (c,_,outputs) =>
    val missing = c.datasets("public").head.file.get.resolveSibling("private-missing.csv")
    val config = c.copy(datasets=Map("public" -> Seq(PublishedItem("/published",file=Some(missing)))))
    val a = new RootAllocator()
    try {
      val p = new Producer(config,a,outputs)
      val error = intercept[FlightRuntimeException](p.getFlightInfo(null,FlightDescriptor.path("datasets","public","/published")))
      assert(!error.getMessage.contains("private-missing") && !error.getMessage.contains(missing.getParent.toString))
    } finally a.close()
  }}
  test("task outputs expose generated aliases rather than operator filenames") { fixture { (c,_,_) =>
    val output = Files.createTempDirectory("opaque-output-")
    try {
      Files.writeString(output.resolve("private-result.csv"),"id\n1\n")
      val adapter = NamespaceAdapter.output(output,c)
      assert(adapter.list().map(_.path) == Seq("/result-1"))
      assert(adapter.resolve("/result-1")._2 == "private-result.csv")
    } finally Data.deleteTree(output)
  }}
  test("file statistics preserve zero timestamps; folder statistics remain unknown") { fixture { (c,p,_) =>
    val file = c.datasets("public").head.file.get
    Files.setLastModifiedTime(file,java.nio.file.attribute.FileTime.fromMillis(0))
    val all = list(p,"datasets","public").map(i => i.getPath -> i).toMap
    val item = all("/events")
    assert(item.hasLastModifiedTimeMs && item.getLastModifiedTimeMs == 0L)
    assert(item.hasTotalSizeBytes && item.getTotalSizeBytes == Files.size(file))
    assert(!item.hasRecordCount)
    Seq("/nested", "/empty").foreach { path =>
      val folder = all(path)
      assert(!folder.hasTotalSizeBytes && !folder.hasRecordCount && !folder.hasLastModifiedTimeMs)
    }
    // Folder metadata must succeed without touching an inaccessible descendant source.
    val inaccessible = new NamespaceAdapter(Seq(PublishedItem("/parent/child", file=Some(file.resolveSibling("missing.csv")))), c)
    assert(inaccessible.list("/").head.kind == "folder")
    assert(inaccessible.describe("/parent").recordCount.isEmpty)
    val adapter = new NamespaceAdapter(c.datasets("public"), c)
    Seq("/", "/nested", "/empty").foreach { path =>
      val folder = adapter.describe(path)
      assert(folder.kind == "folder")
      assert(folder.totalSizeBytes.isEmpty && folder.recordCount.isEmpty && folder.lastModifiedTimeMs.isEmpty)
    }
    val info = p.getFlightInfo(null,FlightDescriptor.path("datasets","public","/events"))
    assert(!Metadata.Item.parseFrom(info.getAppMetadata).hasRecordCount)
    assert(adapter.count("/events").contains(1L))
  }}
  test("CSV counts records rather than lines; NDJSON skips blanks; binary record count is unknown") {
    val dir = Files.createTempDirectory("statistics-")
    try {
      val csv = dir.resolve("rows.csv"); Files.writeString(csv,"id,note\n1,\"first\nsecond\"\n2,last\n")
      val json = dir.resolve("rows.ndjson"); Files.writeString(json,"{\"id\":1}\n\n{\"id\":2}\n")
      val empty = dir.resolve("empty.csv"); Files.writeString(empty,"id\n")
      val binary = dir.resolve("empty.bin"); Files.write(binary,Array.emptyByteArray)
      def stat(path: Path) = ItemProperties.file(Item("/data",Data.format(path),None),path)
      assert(stat(csv).recordCount.isEmpty)
      assert(stat(json).recordCount.isEmpty)
      assert(stat(empty).recordCount.isEmpty)
      assert(Formats.detect(csv).count(csv,FileOptions()).contains(2L))
      assert(Formats.detect(json).count(json,FileOptions()).contains(2L))
      assert(Formats.detect(empty).count(empty,FileOptions()).contains(0L))
      assert(stat(binary).totalSizeBytes.contains(0L) && stat(binary).recordCount.isEmpty)
    } finally Data.deleteTree(dir)
  }

}
