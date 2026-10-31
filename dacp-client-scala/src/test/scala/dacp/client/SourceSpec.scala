package dacp.client

import _root_.xstream.data._

import java.io.ByteArrayOutputStream
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._
import scala.util.Using

class SourceSpec extends AnyFunSuite {
  @volatile private var scanTicket: Option[Metadata.Ticket] = None
  test("dataset discovery retains a user-defined JSON description without fixed title or tags") {
    val description="""{"summary":"Orders","owner":{"team":"sales"},"fields":["id","amount"]}"""
    val metadata=Metadata.DataSet.newBuilder().setName("orders").setUri("dacp://localhost/orders").setDescription(description).setObservedAtMs(42).build()
    val info=new FlightInfo(bytesSchema,FlightDescriptor.path("datasets","orders"),java.util.Collections.emptyList[FlightEndpoint](),-1,-1,false,org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,metadata.toByteArray)
    val dataset=dacp.client.DatasetInfo.fromFlight(info)
    assert(dataset.name=="orders" && dataset.description==description && dataset.observedAtMs==42)
    assert(!metadata.getAllFields.keySet.asScala.exists(f=>Set("title","tags")(f.getName)))
  }
  test("transport diagnostics become public DACP errors without causes") {
    val cases = Seq(
      (CallStatus.NOT_FOUND.withDescription("dataset not found; private JDBC detail"), "NOT_FOUND", "Dataset not found."),
      (CallStatus.UNAUTHORIZED.withDescription("private URI prefix"), "ACCESS_DENIED", "Access denied."),
      (CallStatus.UNAVAILABLE.withDescription("private gRPC endpoint"), "UNAVAILABLE", "Cannot reach"),
      (CallStatus.TIMED_OUT.withDescription("private timeout"), "TIMEOUT", "timed out"),
      (CallStatus.INTERNAL.withDescription("private database table"), "REQUEST_FAILED", "request failed"))
    cases.foreach { case (status, code, message) =>
      val error = intercept[DacpException](DacpErrors.protect(throw status.toRuntimeException))
      assert(error.code == code && error.getMessage.contains(message))
      assert(!error.toString.contains("private") && error.getCause == null)
    }
  }

  test("records query travels in tickets and queried streams reject bytes") { server { ds =>
    import Expressions.col
    val predicate = (col("id") > 0) && !col("id").isNull
    Using.resource(ds.open("/records",predicate=Some(predicate),columns=Some(Seq("id")),limit=Some(1))) { item =>
      Using.resource(item.asRecords()) { records => assert(records.next()) }
      val query = scanTicket.get.getScan
      assert(query.getPredicate.getOp == "and")
      assert(query.getPredicate.getChildren(0).getValues(0).getType == "integer")
      assert(query.getProjection.getColumnsList.asScala == Seq("id"))
      assert(query.hasLimit && query.getLimit == 1)
      intercept[IllegalArgumentException](item.asBytes())
    }
    intercept[IllegalArgumentException](ds.open("/records",limit=Some(-1)))
    intercept[IllegalArgumentException](ds.open("/records",columns=Some(Seq.empty)))
    intercept[IllegalArgumentException](col("id") === null)
  }}

  val recordsSchema = new Schema(List(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)).asJava)
  val bytesSchema = new Schema(List(new Field("data", FieldType.nullable(new ArrowType.Binary()), null)).asJava)

  def server(test: Dataset => Unit): Unit = {
    val serverAllocator = new RootAllocator()
    val producer = new NoOpFlightProducer {
      override def getFlightInfo(ctx: FlightProducer.CallContext, desc: FlightDescriptor): FlightInfo = {
        val parts = desc.getPath.asScala
        if (parts(1) == "missing") throw CallStatus.NOT_FOUND.withDescription("missing dataset").toRuntimeException
        val meta = Metadata.DataSet.newBuilder().setUri("dacp://localhost/data1")
          .addItems(Metadata.Item.newBuilder().setPath("/records").setLastModifiedTimeMs(0L).setTotalSizeBytes(0L).setRecordCount(0L).setFormat("csv").setKind("data").addViews("records").addViews("bytes")
            .setArrowSchema(com.google.protobuf.ByteString.copyFrom(recordsSchema.serializeAsMessage())))
          .addItems(Metadata.Item.newBuilder().setPath("/unknown").setKind("data")).build()
        val ticket = Metadata.Ticket.newBuilder().setDataset(parts(1)).setItem(if(parts.size == 3) parts(2) else "/records").build()
        new FlightInfo(recordsSchema, desc, List(new FlightEndpoint(new Ticket(ticket.toByteArray))).asJava,
          -1, -1, false, org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT, if(parts.size == 3) {
            if (parts(2) == "/archive") Metadata.Item.newBuilder().setPath("/archive").setKind("folder").setFormat("tar-gzip").addViews("bytes").build().toByteArray
            else if (parts(2) == "/folder") Metadata.Item.newBuilder().setPath("/folder").setKind("folder").build().toByteArray
            else meta.getItems(0).toByteArray
          } else meta.toByteArray)
      }
      override def getStream(ctx: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        val t = Metadata.Ticket.parseFrom(ticket.getBytes)
        scanTicket = Some(t)
        val schema = if(t.getView == "bytes") bytesSchema else recordsSchema
        val root = VectorSchemaRoot.create(schema, serverAllocator)
        try {
          root.allocateNew()
          listener.start(root)
          if(t.getItem == "/fail") {
            listener.error(CallStatus.INTERNAL.withDescription("broken stream").toRuntimeException)
          } else {
            if(t.getView == "bytes") {
              val v = root.getVector(0).asInstanceOf[VarBinaryVector]
              v.setSafe(0, Array.emptyByteArray); v.setSafe(1, "ab".getBytes("UTF-8")); v.setSafe(2, "cd".getBytes("UTF-8"))
              root.setRowCount(3)
              listener.putNext()
              root.clear(); root.allocateNew()
              root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,"ef".getBytes("UTF-8"))
              root.setRowCount(1)
            } else {
              val v = root.getVector(0).asInstanceOf[IntVector]
              v.setSafe(0,1); v.setSafe(1,2); root.setRowCount(2)
            }
            listener.putNext(); listener.completed()
          }
        } finally root.close()
      }
    }
    val flightServer = FlightServer.builder(serverAllocator,Location.forGrpcInsecure("localhost",0),producer).build().start()
    val allocator = new RootAllocator()
    val ds = new Dataset(FlightClient.builder(allocator,flightServer.getLocation).build(),allocator,"data1",10)
    try test(ds)
    finally { ds.close(); assert(allocator.getAllocatedMemory == 0); flightServer.close(); serverAllocator.close() }
  }

  test("describe fetches only Item metadata without opening data streams") { server { ds=>
    scanTicket=None
    val item=ds.describe("/records")
    assert(item.path=="/records" && item.schema.contains(recordsSchema) && item.views==Seq("records","bytes"))
    assert(item.recordCount.contains(0L) && scanTicket.isEmpty)
    assert(ds.describe("/folder").kind=="folder" && scanTicket.isEmpty)
    intercept[IllegalArgumentException](ds.describe("/../secret"))
  }}
  test("dataset metadata includes paths, formats and Arrow schema") { server { ds =>
    val item = ds.list().head
    assert(item.path == "/records" && item.format == "csv" && item.kind == "data")
    assert(ds.list("/").map(_.path) == Seq("/records","/unknown"))
    val unknown = ds.list().last
    assert(unknown.lastModifiedTime.isEmpty && unknown.totalSizeBytes.isEmpty && unknown.recordCount.isEmpty)
    assert(item.schema.contains(recordsSchema))
    assert(item.totalSizeBytes.contains(0L) && item.recordCount.contains(0L))
    assert(item.lastModifiedTime.contains(java.time.Instant.EPOCH))
  }}
  test("records expose schema and borrowed batches; EOF remains terminal") { server { ds =>
    Using.resource(ds.open("/records")) { item =>
      Using.resource(item.asRecords()) { reader =>
        assert(reader.schema == recordsSchema)
        assert(reader.next())
        assert(reader.root.getRowCount == 2)
        assert(reader.root.getVector(0).getObject(1) == Integer.valueOf(2))
        assert(!reader.next()); assert(!reader.next())
      }
    }
  }}
  test("byte reads preserve chunks, partial reads, empty rows and EOF") { server { ds =>
    Using.resource(ds.open("/records")) { item =>
      Using.resource(item.asBytes()) { reader =>
        assert(reader.read() == 'a'.toInt)
        val bytes = new Array[Byte](3)
        assert(reader.read(bytes,1,1) == 1 && bytes(1) == 'b'.toByte)
        val out = new ByteArrayOutputStream()
        reader.transferTo(out)
        assert(out.toString("UTF-8") == "cdef")
        assert(reader.read() == -1 && reader.read() == -1)
        assert(reader.read(bytes,0,0) == 0)
      }
    }
  }}
  test("container folders open bytes by advertised capabilities; ordinary folders reject open") { server { ds =>
    Using.resource(ds.open("/archive")) { item =>
      assert(item.views == Seq("bytes"))
      Using.resource(item.asBytes()) { reader => assert(new String(reader.readAllBytes(),"UTF-8") == "abcdef") }
      intercept[IllegalArgumentException](item.asRecords())
    }
    intercept[IllegalArgumentException](ds.open("/folder"))
  }}
  test("closing an item cancels and closes its active readers") { server { ds =>
    val item = ds.open("/records")
    val records = item.asRecords(); assert(records.next())
    item.close(); item.close()
    intercept[IllegalArgumentException](records.next())
    intercept[IllegalArgumentException](item.asBytes())
  }}
  test("closing dataset closes outstanding readers and rejects reuse") { server { ds =>
    val reader = ds.open("/records").asBytes()
    assert(reader.read() == 'a'.toInt)
    ds.close()
    intercept[java.io.IOException](reader.read())
    intercept[IllegalArgumentException](ds.list())
  }}
  test("failed record and byte readers are terminal") { server { ds =>
    Using.resource(ds.open("/fail")) { item =>
      val records = item.asRecords()
      intercept[DacpException](records.next())
      intercept[IllegalArgumentException](records.next())
      val bytes = item.asBytes()
      intercept[DacpException](bytes.read())
      intercept[java.io.IOException](bytes.read())
    }
  }}
  test("invalid URIs and item paths fail before network access") {
    Seq("https://host/d", "dacp://host", "dacp://host/d/../a", "dacp://user@host/d",
      "dacp://host/d?x=1", "dacp://host/d#x", "dacp://host/d/%2e%2e/a", "dacp://host:0/d").foreach { uri =>
      intercept[IllegalArgumentException](Source.parse(uri))
    }
    server { ds => Seq("", "a", "../a", "a//b", "a/", "relative", "/a/", "/a//b", "/../a").foreach(p => intercept[IllegalArgumentException](ds.open(p))) }
  }
  test("direct item access requires item URI") {
    intercept[IllegalArgumentException](Source.openItem("dacp://localhost/data1"))
    intercept[IllegalArgumentException](Source.open("dacp://localhost/data1/rows.csv"))
  }

  test("optional real mTLS server: listing, records, bytes and direct item ownership") {
    sys.env.get("DACP_SCALA_TEST_URI") match {
      case None => cancel("set DACP_SCALA_TEST_URI and DACP_CA/CERT/KEY for real-server validation")
      case Some(uri) =>
        Using.resource(Source.open(uri)) { ds =>
          assert(ds.list().exists(_.path == "/reports/orders"))
          assert(ds.list("/").exists(i => i.path == "/reports" && i.kind == "folder"))
          sys.env.get("DACP_SCALA_EXCEL_PATH").foreach { path =>
            assert(ds.list().find(_.path == path).get.format == "excel")
            assert(ds.count(path).contains(2L))
            Using.resource(ds.open(path)) { item =>
              assert(item.views == Seq("records","bytes"))
              Using.resource(item.asRecords()) { reader =>
                var count = 0
                while (reader.next()) count += reader.root.getRowCount
                assert(count == 2)
              }
            }
          }
          sys.env.get("DACP_SCALA_CONTAINER_DIR").foreach { directory =>
            val sources = Seq("/archive" -> "reports.zip", "/images" -> "images.ZIP", "/bundle" -> "reports.tar",
              "/bundle_gzip" -> "reports.tar.gz", "/orders.csv.gz" -> "orders.csv.gz")
            sources.foreach { case (alias,filename) =>
              val metadata = ds.list("/").find(_.path == alias).get
              assert(metadata.kind == "folder" && metadata.views == Seq("bytes"))
              assert(metadata.totalSizeBytes.isEmpty && metadata.recordCount.isEmpty && metadata.lastModifiedTimeMs.isEmpty)
              Using.resource(ds.open(alias)) { item =>
                Using.resource(item.asBytes()) { reader =>
                  assert(reader.readAllBytes().sameElements(java.nio.file.Files.readAllBytes(java.nio.file.Path.of(directory,filename))))
                }
              }
            }
            for (path <- Seq("/bundle/reports/orders.csv", "/bundle_gzip/reports/orders.csv", "/orders.csv.gz/orders.csv")) {
              assert(ds.count(path).contains(2L))
              Using.resource(ds.open(path)) { item =>
                Using.resource(item.asRecords()) { reader =>
                  var count = 0; while (reader.next()) count += reader.root.getRowCount
                  assert(count == 2)
                }
              }
            }
          }
          import Expressions.col
          Using.resource(ds.open("/reports/orders",predicate=Some(col("id") > 1),
            columns=Some(Seq("amount")),limit=Some(1L))) { item =>
            Using.resource(item.asRecords()) { reader =>
              assert(reader.schema.getFields.asScala.map(_.getName) == Seq("amount"))
              assert(reader.next() && reader.root.getRowCount == 1)
              assert(reader.root.getVector(0).getObject(0).asInstanceOf[Number].doubleValue() == 30.0)
              assert(!reader.next())
            }
          }
          val listed = ds.list("/reports")
          assert(listed.map(_.path) == Seq("/reports/orders"))
          assert(listed.head.recordCount.forall(_ == 2L) && listed.head.totalSizeBytes.isDefined && listed.head.lastModifiedTime.isDefined)
          Using.resource(ds.open("/reports/orders")) { item =>
            Using.resource(item.asRecords()) { reader => assert(reader.next()); assert(reader.root.getRowCount == 2) }
          }
        }
        Using.resource(Source.openItem(uri + "/image")) { item =>
          Using.resource(item.asBytes()) { reader => assert(reader.readNBytes(8).toSeq == Seq[Byte](-119,80,78,71,13,10,26,10)) }
        }
    }
  }
}
