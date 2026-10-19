package dacp

import java.nio.file.{Files, Path}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

class NamespaceSpec extends AnyFunSuite {
  def fixture(test: (Config, Producer) => Unit): Unit = {
    val dir = Files.createTempDirectory("private-storage-")
    val file = dir.resolve("secret-orders.csv")
    Files.writeString(file,"id\n1\n")
    Files.writeString(dir.resolve("unpublished.csv"),"hidden\n1\n")
    val entries = Seq(PublishedItem("/events",file=Some(file)), PublishedItem("/nested/orders",file=Some(file)),
      PublishedItem("/nested/archive/old",file=Some(file)), PublishedItem("/empty",kind="folder"))
    val config = Config("localhost",8815,"localhost",Map("public"->DatasetDefinition.mixed(entries)),Seq("dacp://localhost:8815/public"),dir,dir,dir)
    val allocator = new RootAllocator()
    try test(config,new Producer(config,allocator))
    finally { allocator.close(); Data.deleteTree(dir) }
  }
  def list(producer: Producer, parts: String*): Seq[Metadata.Item] =
    Metadata.DataSet.parseFrom(producer.getFlightInfo(TestCalls.context,FlightDescriptor.path(parts:_*)).getAppMetadata).getItemsList.asScala.toVector

  test("unknown authorized datasets report a wrong name while other datasets retain authorization") { fixture { (c,_) =>
    val allocator = new RootAllocator()
    val p = new Producer(c.copy(prefixes=c.prefixes:+c.uri("missing")),allocator)
    val requests = Seq(Seq("datasets", "missing"), Seq("datasets", "missing", "list", "/"),
      Seq("datasets", "missing", "/orders"), Seq("datasets", "missing", "count", "/orders"))
    requests.foreach { parts =>
      val error = intercept[FlightRuntimeException](p.getFlightInfo(TestCalls.context,FlightDescriptor.path(parts:_*)))
      assert(error.status().code() == FlightStatusCode.NOT_FOUND)
      assert(error.getMessage.contains("dataset not found") && error.getMessage.contains("dataset name"))
    }
    try {
      val restricted = new Producer(c.copy(prefixes=Seq("dacp://localhost:8815/other")),allocator)
      val error = intercept[FlightRuntimeException](restricted.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","public")))
      assert(error.status().code() == FlightStatusCode.UNAUTHORIZED)
    } finally allocator.close()
  }}

  test("recursive listing and immediate folder children use absolute logical paths") { fixture { (_,p) =>
    val all = list(p,"datasets","public")
    assert(all.map(_.getPath) == Seq("/empty","/events","/nested","/nested/archive","/nested/archive/old","/nested/orders"))
    val root = list(p,"datasets","public","list","/")
    assert(root.map(i => i.getPath -> i.getKind) == Seq("/empty"->"folder","/events"->"data","/nested"->"folder"))
    assert(list(p,"datasets","public","list","/nested").map(_.getPath) == Seq("/nested/archive","/nested/orders"))
    assert(list(p,"datasets","public","list","/empty").isEmpty)
    assert(all.find(_.getKind == "folder").get.getViewsCount == 0)
    assert(all.find(_.getKind == "data").get.getViewsList.asScala == Seq("records","bytes"))
  }}
  test("folder streams, data listings, relative paths and traversal are rejected") { fixture { (_,p) =>
    for (parts <- Seq(Seq("datasets","public","/"), Seq("datasets","public","/nested"), Seq("datasets","public","list","/events"),
      Seq("datasets","public","list","/missing"), Seq("datasets","public","events"),
      Seq("datasets","public","/../events"), Seq("datasets","public","/nested//orders"))) {
      intercept[FlightRuntimeException](p.getFlightInfo(TestCalls.context,FlightDescriptor.path(parts:_*)))
    }
  }}
  test("metadata and tickets do not disclose physical names; unpublished resources are inaccessible") { fixture { (_,p) =>
    val info = p.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","public","/nested/orders"))
    val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
    assert(metadata.getPath == "/nested/orders" && metadata.getKind == "data")
    val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes)
    assert(ticket.getItem == "/nested/orders")
    assert(!metadata.toString.contains("secret-orders") && !ticket.toString.contains("secret-orders"))
    for(path <- Seq("/secret-orders.csv","/unpublished.csv")) {
      val error = intercept[FlightRuntimeException](p.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","public",path)))
      assert(!error.getMessage.contains("private-storage") && !error.getMessage.contains(path))
    }
  }}
  test("invalid mappings reject duplicate aliases, data parents, and unknown sources") { fixture { (c,_) =>
    val data = DatasetProviders.mixedBindings(c.datasets("public")).head
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(data,data),c))
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(data,PublishedItem("/events/child",kind="folder")),c))
    intercept[IllegalArgumentException](PublishedItem("relative",kind="folder"))
    intercept[IllegalArgumentException](new NamespaceAdapter(Seq(PublishedItem("/data",sql=Some("unknown"),table=Some("SECRET"))),c))
  }}
  test("missing private files produce sanitized client errors") { fixture { (c,_) =>
    val missing = DatasetProviders.mixedBindings(c.datasets("public")).head.file.get.resolveSibling("private-missing.csv")
    val config = c.copy(datasets=Map("public" -> DatasetDefinition.mixed(Seq(PublishedItem("/published",file=Some(missing))))))
    val a = new RootAllocator()
    try {
      val p = new Producer(config,a)
      val error = intercept[FlightRuntimeException](p.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","public","/published")))
      assert(!error.getMessage.contains("private-missing") && !error.getMessage.contains(missing.getParent.toString))
    } finally a.close()
  }}
  test("file statistics preserve zero timestamps; folder statistics remain unknown") { fixture { (c,p) =>
    val file = DatasetProviders.mixedBindings(c.datasets("public")).head.file.get
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
    val adapter = c.openDataset("public")
    Seq("/", "/nested", "/empty").foreach { path =>
      val folder = adapter.describe(path)
      assert(folder.kind == "folder")
      assert(folder.totalSizeBytes.isEmpty && folder.recordCount.isEmpty && folder.lastModifiedTimeMs.isEmpty)
    }
    val info = p.getFlightInfo(TestCalls.context,FlightDescriptor.path("datasets","public","/events"))
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

  test("publication counts are included without recounting; file changes invalidate them and folders remain unknown") {
    val dir = Files.createTempDirectory("published-count-")
    try {
      val csv = dir.resolve("orders.csv"); Files.writeString(csv,"id\n1\n2\n")
      val empty = dir.resolve("empty.csv"); Files.writeString(empty,"id\n")
      val config = Config("localhost",8815,"localhost",Map.empty,Seq.empty,dir,dir,dir)
      val saved = FileRecordCounts.capture(csv,FileOptions()).get
      assert(saved.value == 2L)
      val ns = new NamespaceAdapter(Seq(
        PublishedItem("/reports/orders",file=Some(csv),recordCount=Some(saved)),
        PublishedItem("/empty",file=Some(empty),recordCount=FileRecordCounts.capture(empty,FileOptions()))),config)
      assert(ns.list("/reports").head.recordCount.contains(2L))
      assert(ns.describe("/reports/orders").recordCount.contains(2L))
      assert(ns.list().find(_.path == "/empty").get.recordCount.contains(0L))
      assert(ns.describe("/reports").recordCount.isEmpty)
      Files.setLastModifiedTime(csv,java.nio.file.attribute.FileTime.fromMillis(saved.modified.toMillis+1000))
      assert(ns.list("/reports").head.recordCount.isEmpty)
      Files.writeString(csv,"id\n1\n2\n3\n")
      assert(ns.list("/reports").head.recordCount.isEmpty)
      assert(ns.count("/reports/orders").contains(3L))
      assert(ns.list("/reports").head.recordCount.isEmpty)
      val binary = dir.resolve("image.bin"); Files.write(binary,Array[Byte](1,2))
      assert(FileRecordCounts.capture(binary,FileOptions()).isEmpty)
    } finally Data.deleteTree(dir)
  }
  test("example config fills direct file counts at publication without counting container children") {
    val config = Config.load(Path.of("../examples/server.yaml"))
    val ns = config.openDataset("data1")
    assert(ns.list("/reports").head.recordCount.contains(2L))
    val root = ns.list("/").map(i => i.path -> i).toMap
    assert(root("/events").recordCount.contains(2L))
    assert(root("/image").recordCount.isEmpty && root("/archive").recordCount.isEmpty)
    assert(ns.list("/archive/reports").head.recordCount.isEmpty)
  }

  test("startup counting is opt in and ordinary configuration loading does not open record files") {
    val dir = Files.createTempDirectory("count-config-")
    try {
      val yaml = dir.resolve("server.yaml")
      val text = s"""host: localhost
public_host: localhost
tls_cert: cert.pem
tls_key: key.pem
tls_ca: ca.pem
allowed_uri_prefixes: [dacp://localhost:8815/public]
datasets:
  public:
    provider: mixed
    items:
      /missing: {file: ${dir.resolve("missing.csv")}}
"""
      Files.writeString(yaml,text)
      assert(DatasetProviders.mixedBindings(Config.load(yaml).datasets("public")).head.recordCount.isEmpty)
      Files.writeString(yaml,text + "count_file_records_on_startup: true\n")
      intercept[java.nio.file.NoSuchFileException](Config.load(yaml))
    } finally Data.deleteTree(dir)
  }

}
