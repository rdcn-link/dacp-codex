package dacp

import java.nio.file.{Files,Path}
import java.sql.DriverManager
import java.util.UUID
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import dacp.protocol.Metadata

class SqlSpec extends AnyFunSuite {
  def database(test: SqlConfig => Unit): Unit = {
    val url = "jdbc:h2:mem:"+UUID.randomUUID().toString
    val c = DriverManager.getConnection(url,"sa","")
    try {
      val st = c.createStatement()
      try {
        st.execute("CREATE TABLE orders (id BIGINT NOT NULL, name VARCHAR, amount DECIMAL(12,2), active BOOLEAN, payload VARBINARY, event_day DATE, clock TIME(6), stamp TIMESTAMP(6), zoned TIMESTAMP(6) WITH TIME ZONE, score DOUBLE)")
        st.execute("INSERT INTO orders VALUES (1,'one',12.50,TRUE,X'0102',DATE '2026-01-02',TIME '12:34:56.123456',TIMESTAMP '2026-01-02 12:34:56.123456',TIMESTAMP WITH TIME ZONE '2026-01-02 12:34:56.123456+02:00',1.25),(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(3,'three',0,FALSE,X'',DATE '1970-01-01',TIME '00:00:00',TIMESTAMP '1970-01-01 00:00:00',TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00:00',0)")
        st.execute("CREATE TABLE empty_table (id INTEGER)")
        st.execute("CREATE SCHEMA private_data")
        st.execute("CREATE TABLE private_data.secret (id INTEGER)")
        st.execute("CREATE TABLE \"odd\"\"name\" (id INTEGER)")
      } finally st.close()
      test(SqlConfig(url,Some("sa"),Some(""),Some("PUBLIC"),fetchSize=1))
    } finally c.close()
  }
  test("SQL tables are flat Items with typed schemas, exact schema scoping and allowlists") { database { conf =>
    val a = new SqlAdapter(conf,2)
    assert(a.list().map(_.path) == Seq("EMPTY_TABLE","ORDERS","odd\"name"))
    val item = a.describe("ORDERS")
    assert(item.format == "sql" && item.schema.get.getFields.size == 10)
    assert(item.recordCount.isEmpty && item.totalSizeBytes.isEmpty && item.lastModifiedTimeMs.isEmpty)
    assert(a.describe("EMPTY_TABLE").recordCount.isEmpty)
    assert(a.count("EMPTY_TABLE") == 0L && a.count("ORDERS") == 3L)
    assert(!item.schema.get.getFields.get(0).isNullable)
    assert(new SqlAdapter(conf.copy(tables=Some(Seq("ORDERS"))),2).list().map(_.path) == Seq("ORDERS"))
    intercept[java.io.FileNotFoundException](new SqlAdapter(conf.copy(tables=Some(Seq("ORDERS"))),2).describe("EMPTY_TABLE"))
    for (name <- Seq("../ORDERS","PUBLIC/ORDERS","/ORDERS")) intercept[IllegalArgumentException](a.describe(name))
    intercept[java.io.FileNotFoundException](a.describe("ORDERS; DROP TABLE ORDERS"))
    assert(a.describe("odd\"name").schema.get.getFields.size == 1)
  }}
  test("SQL streaming preserves types/nulls and bounds batches; empty and cancelled streams release resources") { database { conf =>
    val a = new SqlAdapter(conf,2); val allocator = new RootAllocator()
    try {
      var sizes = Vector.empty[Int]; var rows = 0; var readyCalls = 0
      a.stream("ORDERS",allocator)(_ => (),root => {
        sizes :+= root.getRowCount
        if(rows == 0) {
          assert(root.getVector("AMOUNT").getObject(0).toString == "12.50")
          assert(root.getVector("NAME").isNull(1))
          assert(root.getVector("PAYLOAD").getObject(0).asInstanceOf[Array[Byte]].toSeq == Seq[Byte](1,2))
          assert(root.getVector("CLOCK").getObject(0).toString.nonEmpty)
        }
        rows += root.getRowCount
      },() => readyCalls += 1)
      assert(rows == 3 && sizes == Vector(2,1) && readyCalls >= 3)
      var started = false
      a.stream("EMPTY_TABLE",allocator)(_ => started=true,_ => fail("empty table emitted rows"),() => ())
      assert(started)
      intercept[java.util.concurrent.CancellationException] {
        a.stream("ORDERS",allocator)(_ => (),_ => (),() => throw new java.util.concurrent.CancellationException())
      }
      intercept[IllegalStateException] {
        a.stream("ORDERS",allocator)(_ => (),_ => throw new IllegalStateException("consumer failed"),() => ())
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(a.list().nonEmpty)
    } finally allocator.close()
  }}
  test("Flight serves SQL records and byte-view IPC alongside directory datasets") { database { conf =>
    val allocator = new RootAllocator(); val outputs = new OutputRegistry(10000)
    val dir = Files.createTempDirectory("sql-files-"); Files.writeString(dir.resolve("x.csv"),"id\n1\n")
    val config = Config("localhost",8815,"localhost",Map("files" -> Seq(PublishedItem("/records", file=Some(dir.resolve("x.csv")))),
      "db" -> Seq(PublishedItem("/orders", sql=Some("warehouse"), table=Some("ORDERS")),
        PublishedItem("/reports/file",file=Some(dir.resolve("x.csv"))))),Seq("dacp://localhost:8815/files","dacp://localhost:8815/db"),dir,dir,dir, batchSize=2,byteChunk=32,sqlSources=Map("warehouse"->conf))
    val server = FlightServer.builder(allocator,Location.forGrpcInsecure("127.0.0.1",0),new Producer(config,allocator,outputs)).build().start()
    val client = FlightClient.builder(allocator,server.getLocation).build()
    try {
      def meta(ds: String) = Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets",ds)).getAppMetadata)
      assert(meta("files").getItems(0).getPath == "/records")
      assert(meta("db").getItemsList.asScala.exists(i => i.getPath == "/orders" && !i.getArrowSchema.isEmpty))
      val children = Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets","db","list","/reports")).getAppMetadata)
      assert(children.getItemsList.asScala.map(_.getPath) == Seq("/reports/file"))
      assert(TaskInputs.discover(client,"db",None).map(_._1).toSet == Set("/orders","/reports/file"))
      assert(TaskInputs.discover(client,"db",Some("/orders")) == Seq("/orders" -> "sql"))
      val publicMetadata = meta("db")
      val stats = publicMetadata.getItemsList.asScala.find(_.getPath == "/orders").get
      assert(!stats.hasRecordCount)
      val counted = client.getInfo(FlightDescriptor.path("datasets","db","count","/orders"))
      assert(Metadata.Item.parseFrom(counted.getAppMetadata).getRecordCount == 3L)
      assert(!stats.hasTotalSizeBytes && !stats.hasLastModifiedTimeMs)
      assert(!publicMetadata.toString.contains("ORDERS") && !publicMetadata.toString.contains("warehouse"))
      val rejected = intercept[FlightRuntimeException](client.getInfo(FlightDescriptor.path("datasets","db","/ORDERS")))
      assert(!rejected.getMessage.contains("ORDERS"))
      val info = client.getInfo(FlightDescriptor.path("datasets","db","/orders"))
      val records = client.getStream(info.getEndpoints.get(0).getTicket)
      try { var rows = 0; while(records.next()) rows += records.getRoot.getRowCount; assert(rows == 3) } finally records.close()
      val t = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder.setView("bytes").build()
      val bytes = client.getStream(new Ticket(t.toByteArray)); val out = new java.io.ByteArrayOutputStream()
      try while(bytes.next()) {
        val root = bytes.getRoot; val v = root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
        (0 until root.getRowCount).foreach(i => { assert(v.get(i).length <= 32); out.write(v.get(i)) })
      } finally bytes.close()
      val reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(out.toByteArray),allocator)
      try { var rows = 0; while(reader.loadNextBatch()) rows += reader.getVectorSchemaRoot.getRowCount; assert(rows == 3) } finally reader.close()
      val restricted = config.copy(prefixes=Seq("dacp://localhost:8815/files"))
      intercept[FlightRuntimeException](new Producer(restricted,allocator,outputs).getFlightInfo(null,FlightDescriptor.path("datasets","db")))
    } finally { client.close(); server.close(); outputs.close(); allocator.close(); Data.deleteTree(dir) }
  }}
  test("SQL unsupported types fail explicitly and config loads private sources and explicit aliases") { database { conf =>
    val c = DriverManager.getConnection(conf.url,"sa","")
    try { val st = c.createStatement(); try st.execute("CREATE TABLE unsupported (items INTEGER ARRAY)") finally st.close() } finally c.close()
    intercept[IllegalArgumentException](new SqlAdapter(conf,2).describe("UNSUPPORTED"))
    val old = Config.load(Path.of("../examples/server.yaml")); assert(old.sqlSources.isEmpty && old.datasets.contains("data1"))
    val p = Files.createTempFile("sql-config-",".yaml")
    try {
      Files.writeString(p,"""host: localhost
public_host: localhost
tls_cert: cert
tls_key: key
tls_ca: ca
output_root: output
allowed_uri_prefixes: ['dacp://localhost:8815/db']
sql_sources:
  db:
    url: 'jdbc:h2:mem:config'
    user: sa
    schema: PUBLIC
    tables: [ORDERS]
    fetch_size: 17
""")
      val loaded = Config.load(p)
      assert(loaded.datasets.isEmpty && loaded.sqlSources("db").fetchSize == 17 && !loaded.hasDataset("db"))
      intercept[IllegalArgumentException](loaded.copy(datasets=Map("bad/name"->Seq.empty)))
    } finally Files.delete(p)
  }}
}
