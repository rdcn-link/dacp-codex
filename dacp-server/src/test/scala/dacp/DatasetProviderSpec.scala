package dacp

import java.nio.file.{Files,Path}
import java.sql.DriverManager
import java.util.zip.{ZipEntry,ZipOutputStream}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

class DatasetProviderSpec extends AnyFunSuite {
  private def config(dir: Path, definitions: Map[String,DatasetDefinition], counts: Boolean = false): Config =
    Config("localhost",8815,"localhost",definitions,Seq("dacp://localhost:8815/"),dir,dir,dir,countFileRecordsOnStartup=counts)
  private def directory(root: Path, overrides: Map[String,Map[String,Object]] = Map.empty): DatasetDefinition =
    DatasetDefinition("directory",Map("root" -> root.toString),overrides)
  private def zip(path: Path): Unit = {
    val out = new ZipOutputStream(Files.newOutputStream(path))
    try { out.putNextEntry(new ZipEntry("orders.csv")); out.write("id\n1\n2\n".getBytes("UTF-8")); out.closeEntry() } finally out.close()
  }
  test("directory provider discovers nested and empty folders, format overrides, and live filesystem changes") {
    val dir = Files.createTempDirectory("dataset-directory-")
    try {
      Files.createDirectories(dir.resolve("reports")); Files.createDirectories(dir.resolve("empty"))
      Files.writeString(dir.resolve("reports/orders.csv"),"id\n1\n2\n")
      Files.writeString(dir.resolve("events.json"),"{\"id\":1}\n{\"id\":2}\n")
      zip(dir.resolve("archive.ZIP"))
      val c = config(dir,Map("files" -> directory(dir,Map("/events.json" -> Map("format" -> "ndjson")))),true)
      val ds = c.openDataset("files")
      assert(ds.list("/").map(_.path) == Seq("/archive.ZIP","/empty","/events.json","/reports"))
      assert(ds.list("/empty").isEmpty)
      assert(ds.list("/reports").head.recordCount.contains(2L))
      assert(ds.list("/").find(_.path == "/events.json").get.recordCount.contains(2L))
      assert(ds.list("/archive.ZIP").head.path == "/archive.ZIP/orders.csv")
      assert(ds.count("/archive.ZIP/orders.csv").contains(2L))
      assert(ds.describe("/reports").recordCount.isEmpty)
      Files.writeString(dir.resolve("new.csv"),"id\n1\n")
      assert(ds.list("/").find(_.path == "/new.csv").get.recordCount.isEmpty)
      Files.writeString(dir.resolve("reports/orders.csv"),"id\n1\n2\n3\n")
      assert(ds.list("/reports").head.recordCount.isEmpty && ds.count("/reports/orders.csv").contains(3L))
      Files.delete(dir.resolve("new.csv")); assert(!ds.list().exists(_.path == "/new.csv"))
    } finally Data.deleteTree(dir)
  }
  test("folder metadata does not inspect descendants; symlinks and invalid logical paths are inaccessible") {
    val dir = Files.createTempDirectory("dataset-boundary-"); val outside = Files.createTempDirectory("dataset-private-")
    try {
      Files.createDirectories(dir.resolve("folder")); Files.writeString(dir.resolve("folder/bad.zip"),"not a zip")
      Files.writeString(outside.resolve("secret.csv"),"id\n1\n")
      Files.createSymbolicLink(dir.resolve("outside"),outside)
      Files.createSymbolicLink(dir.resolve("secret.csv"),outside.resolve("secret.csv"))
      Files.createSymbolicLink(dir.resolve("internal"),dir.resolve("folder"))
      val ds = config(dir,Map("files" -> directory(dir))).openDataset("files")
      val item = ds.describe("/folder")
      assert(item.kind == "folder" && item.lastModifiedTimeMs.isEmpty && item.totalSizeBytes.isEmpty && item.recordCount.isEmpty)
      assert(ds.list("/").map(_.path) == Seq("/folder"))
      for(path <- Seq("/outside/secret.csv","/secret.csv","/internal")) intercept[java.io.FileNotFoundException](ds.describe(path))
      for(path <- Seq("relative","/../secret.csv","/a//b")) intercept[IllegalArgumentException](ds.describe(path))
      assert(ds.list("/folder").head.kind == "folder") // Listing a container itself need not enumerate invalid children.
      intercept[FormatFailure](ds.list("/folder/bad.zip"))
    } finally { Data.deleteTree(dir); Data.deleteTree(outside) }
  }
  test("provider configuration uses provider-specific keys and rejects obsolete names and mixed modes") {
    val dir = Files.createTempDirectory("dataset-config-")
    try {
      Files.writeString(dir.resolve("orders.csv"),"id\n1\n")
      val file = dir.resolve("server.yaml")
      val base = s"""host: localhost
public_host: localhost
tls_cert: cert.pem
tls_key: key.pem
tls_ca: ca.pem
output_root: output
allowed_uri_prefixes: [dacp://localhost:8815/]
"""
      val definitions = s"""sql_sources:
  warehouse:
    url: jdbc:h2:mem:config_provider
datasets:
  directory:
    provider: directory
    source: {root: $dir}
    reader_overrides:
      /orders.csv: {options: {}}
  sales:
    provider: sqldb
    source: {connection: warehouse}
    tables:
      /orders: {table: PRIVATE_ORDERS}
  plugin:
    provider: test-dataset
    source: {message: configured payload}
    entries:
      /message: {label: configured item}
  custom:
    provider: custom
    items:
      /orders: {file: $dir/orders.csv}
"""
      Files.writeString(file,base+definitions)
      val c = Config.load(file)
      assert(c.datasets("sales").items("/orders")("table") == "PRIVATE_ORDERS")
      assert(c.datasets("plugin").source("message") == "configured payload")
      assert(c.datasets("plugin").items("/message")("label") == "configured item")
      assert(c.openDataset("plugin").describe("/message").views.contains(Seq("bytes")))
      assert(c.openDataset("directory").list("/").exists(_.path == "/orders.csv"))
      assert(c.openDataset("custom").list().map(_.path) == Seq("/orders"))
      for (bad <- Seq(
        "datasets: {bad: {provider: unknown}}",
        "datasets: {bad: {items: {}}}",
        "sql_datasets: {}",
        "datasets: {bad: /private/directory}",
        s"datasets: {bad: {provider: directory, source: {root: $dir}, items: {}}}",
        "datasets: {bad: {provider: sqldb, source: {connection: warehouse}, items: {}}}",
        "datasets: {bad: {provider: test-dataset, items: {}}}",
        s"datasets: {bad: {provider: directory, source: {root: $dir}, reader_overrides: {}, items: {}}}",
        "datasets: {bad: {provider: sqldb, tables: {}, items: {}}}",
        s"datasets: {bad: {provider: directory, source: {root: $dir}, tables: {}}}",
        "datasets: {bad: {provider: custom, reader_overrides: {}}}",
        s"datasets: {bad: {provider: directory, source: {root: $dir}, reader_overrides: {/x: {file: /private}}}}",
        s"datasets: {bad: {provider: custom, source: {root: $dir}}}",
        s"datasets: {bad: {provider: directory, source: {root: $dir}, items: {/x: {file: /private}}}}",
        "datasets: {bad: {provider: sqldb, source: {connection: missing}}}",
        s"datasets: {bad: {provider: directory, directory: $dir, source: {root: $dir}}}")) {
        Files.writeString(file,base+bad+"\n"); intercept[IllegalArgumentException](Config.load(file))
      }
    } finally Data.deleteTree(dir)
  }
  test("directory, sqldb, custom mixed sources, and external providers share the Flight interface") {
    val dir = Files.createTempDirectory("dataset-flight-"); val allocator = new RootAllocator(); val outputs = new OutputRegistry(10000)
    val url = "jdbc:h2:mem:dataset_provider_"+System.nanoTime()+";DB_CLOSE_DELAY=-1"
    val db = DriverManager.getConnection(url,"sa","")
    try {
      val st = db.createStatement()
      try { st.execute("CREATE TABLE PRIVATE_CUSTOMERS(ID BIGINT)"); st.execute("INSERT INTO PRIVATE_CUSTOMERS VALUES(1),(2)") } finally st.close()
      Files.writeString(dir.resolve("orders.csv"),"id\n1\n2\n")
      zip(dir.resolve("archive.zip"))
      val defs = Map(
        "files" -> directory(dir),
        "sales" -> DatasetDefinition("sqldb",Map("connection" -> "warehouse"),Map("/customers" -> Map("table" -> "PRIVATE_CUSTOMERS"))),
        "custom" -> DatasetDefinition.custom(Seq(PublishedItem("/orders",file=Some(dir.resolve("orders.csv"))),
          PublishedItem("/customers",sql=Some("warehouse"),table=Some("PRIVATE_CUSTOMERS")))),
        "plugin" -> DatasetDefinition("test-dataset",Map("message" -> "external provider")))
      val c = config(dir,defs).copy(sqlSources=Map("warehouse" -> SqlConfig(url,Some("sa"),Some(""),Some("PUBLIC"))))
      assert(!defs("sales").toString.contains("warehouse"))
      val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(c,allocator,outputs)).build().start()
      val client = FlightClient.builder(allocator,server.getLocation).build()
      try {
        for ((dataset,path) <- Seq("files" -> "/orders.csv","files" -> "/archive.zip/orders.csv","sales" -> "/customers","custom" -> "/orders","custom" -> "/customers")) {
          val listing = Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets",dataset)).getAppMetadata)
          assert(!listing.toString.contains("PRIVATE_CUSTOMERS") && !listing.toString.contains(dir.toString))
          assert(!listing.getItemsList.asScala.find(_.getPath == path).get.hasRecordCount)
          val counted = client.getInfo(FlightDescriptor.path("datasets",dataset,"count",path))
          assert(Metadata.Item.parseFrom(counted.getAppMetadata).getRecordCount == 2L)
          val info = client.getInfo(FlightDescriptor.path("datasets",dataset,path))
          val stream = client.getStream(info.getEndpoints.get(0).getTicket)
          try { var n=0; while(stream.next()) n += stream.getRoot.getRowCount; assert(n == 2) } finally stream.close()
        }
        val info = client.getInfo(FlightDescriptor.path("datasets","plugin","/message"))
        val stream = client.getStream(info.getEndpoints.get(0).getTicket)
        try { assert(stream.next()); assert(new String(stream.getRoot.getVector(0).asInstanceOf[VarBinaryVector].get(0),"UTF-8") == "external provider") } finally stream.close()
      } finally { client.close(); server.close() }
    } finally { db.close(); outputs.close(); allocator.close(); Data.deleteTree(dir) }
  }
}
