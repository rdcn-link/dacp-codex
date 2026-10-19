package dacp

import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType,Field,FieldType,Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.flight._
import scala.jdk.CollectionConverters._
import java.nio.file.Files
import dacp.protocol.Metadata

class ScanSpec extends AnyFunSuite {
  def atom(op: String,column: String,kind: String = "integer",value: String = "1"): Predicate =
    Predicate(op,column,if(op.startsWith("is_")) Seq.empty else Seq(Literal(kind,value)))
  test("three-valued boolean logic, NaN, null checks, membership, projection and validation") {
    val allocator = new RootAllocator()
    val schema = new Schema(Seq(
      new Field("n",FieldType.nullable(new ArrowType.Int(64,true)),null),
      new Field("f",FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),null)).asJava)
    val root = VectorSchemaRoot.create(schema,allocator)
    try {
      root.allocateNew(); root.getVector(0).asInstanceOf[BigIntVector].setSafe(0,1)
      root.getVector(0).setNull(1); root.getVector(1).asInstanceOf[Float8Vector].setSafe(0,Double.NaN)
      root.getVector(1).setNull(1); root.setRowCount(2)
      def matches(p: Predicate,r: Int) = Scan(Some(p)).bind(schema).matches(root,r)
      val eq = atom("eq","n")
      assert(matches(eq,0) && !matches(eq,1))
      assert(!matches(Predicate("not",children=Seq(eq)),1))
      assert(matches(Predicate("or",children=Seq(eq,atom("is_null","n"))),1))
      assert(!matches(Predicate("and",children=Seq(eq,atom("is_null","n"))),1))
      assert(!matches(atom("eq","f","float","0"),0))
      assert(matches(atom("ne","f","float","0"),0))
      assert(!matches(atom("gt","f","float","0"),0))
      assert(!matches(Predicate("in","n"),1))
      assert(matches(Predicate("in","n",Seq(Literal("integer","1"),Literal("integer","2"))),0))
      intercept[ScanFailure](Scan(Some(atom("eq","missing"))).bind(schema))
      intercept[ScanFailure](Scan(Some(atom("eq","n","string","1"))).bind(schema))
      intercept[ScanFailure](Scan(Some(atom("eq","n","integer","9223372036854775808"))).bind(schema))
      intercept[IllegalArgumentException](Scan(columns=Some(Seq("n","n"))))
      intercept[ScanFailure](Scan(columns=Some(Seq("missing"))).bind(schema))
      intercept[IllegalArgumentException](Scan(limit=Some(-1)))
    } finally { root.close(); allocator.close() }
  }
  test("Flight filters multiple batches before ordered projection and global limit, including zero and bytes rejection") {
    val dir = Files.createTempDirectory("scan-flight-")
    Files.writeString(dir.resolve("private.csv"),"id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n")
    val allocator = new RootAllocator()
    val parquetSchema = new Schema(Seq(Records.field("id","int64"),Records.field("name","string")).asJava)
    val writer = ParquetOutput.writer(dir.resolve("private.parquet"),parquetSchema,"none",1024)
    try {
      (1 to 6).foreach { n =>
        val record = new org.apache.avro.generic.GenericData.Record(Parquet.avro(parquetSchema))
        record.put("id",if(n == 6) null else Long.box(n.toLong)); record.put("name",('a'+n-1).toChar.toString)
        writer.write(record)
      }
    } finally writer.close()
    val config = Config("localhost",8815,"localhost",Map("data" -> DatasetDefinition.mixed(Seq(
      PublishedItem("/records",file=Some(dir.resolve("private.csv"))),
      PublishedItem("/parquet",file=Some(dir.resolve("private.parquet")))))),Seq("dacp://localhost:8815/"),dir,dir,dir,batchSize=2)
    val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(config,allocator)).build().start()
    val client = FlightClient.builder(allocator,server.getLocation).build()
    def read(scan: Metadata.Scan, view: String = "records", path: String = "/records"): (Seq[String],Seq[String]) = {
      val info = client.getInfo(FlightDescriptor.path("datasets","data",path))
      val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder.setView(view).setScan(scan).build()
      val stream = client.getStream(new Ticket(ticket.toByteArray))
      try {
        var values = Vector.empty[String]
        val names = stream.getSchema.getFields.asScala.map(_.getName).toVector
        while(stream.next()) (0 until stream.getRoot.getRowCount).foreach(i => values :+= stream.getRoot.getVector(0).getObject(i).toString)
        names -> values
      } finally stream.close()
    }
    val predicate = Metadata.Predicate.newBuilder().setOp("gt").setColumn("id")
      .addValues(Metadata.Literal.newBuilder().setType("integer").setValue("2"))
    val query = Metadata.Scan.newBuilder().setPredicate(predicate)
      .setProjection(Metadata.Scan.Projection.newBuilder().addColumns("name")).setLimit(2)
    try {
      assert(read(query.build()) == (Seq("name") -> Seq("c","d")))
      assert(read(query.build(),path="/parquet") == (Seq("name") -> Seq("c","d")))
      val ne = predicate.clone().setOp("ne")
      assert(read(query.clone().setPredicate(ne).clearLimit().build(),path="/parquet")._2 == Seq("a","c","d","e"))
      val text = Metadata.Predicate.newBuilder().setOp("eq").setColumn("name")
        .addValues(Metadata.Literal.newBuilder().setType("string").setValue("a"))
      val or = Metadata.Predicate.newBuilder().setOp("or").addChildren(predicate).addChildren(text)
      assert(read(query.clone().setPredicate(or).clearLimit().build(),path="/parquet")._2 == Seq("a","c","d","e"))
      assert(read(query.setLimit(0).build()) == (Seq("name") -> Seq.empty))
      val invalid = query.setProjection(Metadata.Scan.Projection.newBuilder().addColumns("missing")).build()
      val error = intercept[FlightRuntimeException](read(invalid))
      assert(error.getMessage.contains("unknown column: missing"))
      intercept[FlightRuntimeException](read(Metadata.Scan.getDefaultInstance,"bytes"))
    } finally { client.close(); server.close(); config.close(); allocator.close(); Data.deleteTree(dir) }
  }
  test("limit stops upstream production and releases roots on early completion") {
    val allocator = new RootAllocator()
    val schema = new Schema(Seq(Records.field("id","int64")).asJava)
    for(limit <- Seq(0L,3L)) {
      var sourceBatches = 0; var rows = 0; var closed = false
      var output: VectorSchemaRoot = null
      val listener = java.lang.reflect.Proxy.newProxyInstance(getClass.getClassLoader,
        Array(classOf[FlightProducer.ServerStreamListener]),new java.lang.reflect.InvocationHandler {
          def invoke(proxy: Any, method: java.lang.reflect.Method,args: Array[AnyRef]): AnyRef = {
            method.getName match {
              case "start" => output = args(0).asInstanceOf[VectorSchemaRoot]
              case "putNext" => rows += output.getRowCount
              case _ => ()
            }
            null
          }
        }).asInstanceOf[FlightProducer.ServerStreamListener]
      try {
        ScanStreams.stream(Scan(limit=Some(limit)),allocator,listener) { filtered =>
          val root = VectorSchemaRoot.create(schema,allocator)
          try {
            filtered.start(root)
            for(_ <- 1 to 100) {
              sourceBatches += 1; root.clear(); root.allocateNew()
              val vector = root.getVector(0).asInstanceOf[BigIntVector]
              vector.setSafe(0,1); vector.setSafe(1,2); root.setRowCount(2)
              filtered.putNext()
            }
          } finally { root.close(); closed = true }
        }
        assert(rows == limit && closed)
        assert(sourceBatches == (if(limit == 0) 0 else 2))
        assert(allocator.getAllocatedMemory == 0)
      } finally ()
    }
    allocator.close()
  }

  test("JDBC pushdown uses bound values, safe conjunctions and no premature limit") {
    new SqlSpec().database { config =>
      val sql = new SqlAdapter(config,1); val schema = sql.describe("ORDERS").schema.get
      val numeric = atom("gt","ID","integer","1")
      val text = atom("eq","NAME","string","three")
      val conjunction = Predicate("and",children=Seq(numeric,text))
      val scan = Scan(Some(conjunction),Some(Seq("AMOUNT")),Some(1))
      val plan = SqlScanPlan.build(scan,schema,s => "\""+s.replace("\"","\"\"")+"\"")
      assert(plan.where == " WHERE \"ID\" > ?")
      assert(plan.maxRows.isEmpty && !plan.where.contains("three"))
      assert(plan.columns == "\"AMOUNT\",\"ID\",\"NAME\"")
      val or = SqlScanPlan.build(scan.copy(predicate=Some(Predicate("or",children=Seq(numeric,text)))),schema,identity)
      assert(or.where.isEmpty && or.maxRows.isEmpty)
      val allocator = new RootAllocator()
      try {
        var rows = Vector.empty[String]
        sql.stream("ORDERS",allocator,Some(scan))(_ => (),root => {
          val bound = scan.bind(root.getSchema)
          (0 until root.getRowCount).filter(bound.matches(root,_)).foreach(i => rows :+= root.getVector("NAME").getObject(i).toString)
        },() => ())
        assert(rows == Seq("three"))
        assert(SqlScanPlan.build(Scan(Some(numeric),limit=Some(1)),schema,identity).maxRows.contains(1))
      } finally allocator.close()
    }
  }
}
