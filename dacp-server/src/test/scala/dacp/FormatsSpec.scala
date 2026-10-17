package dacp

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import org.scalatest.funsuite.AnyFunSuite
import dacp.protocol.Metadata

class FormatsSpec extends AnyFunSuite {
  def workbook(path: Path, multiple: Boolean = false, header: Int = 1, rows: Int = 2): Unit = {
    val book: Workbook = if (path.toString.endsWith(".xls")) new HSSFWorkbook() else new XSSFWorkbook()
    try {
      val sheet = book.createSheet("Orders")
      val headings = sheet.createRow(header-1)
      Seq("id", "", "id", "flag", "calculated").zipWithIndex.foreach { case (name,index) => headings.createCell(index).setCellValue(name) }
      (0 until rows).foreach { index =>
        val row = sheet.createRow(header+index)
        row.createCell(0).setCellValue((index+1).toDouble)
        row.createCell(2).setCellValue("value")
        row.createCell(3).setCellValue(true)
        row.createCell(4).setCellFormula(s"A${header+index+1}+1")
      }
      book.getCreationHelper.createFormulaEvaluator().evaluateAll()
      if (multiple) { val second = book.createSheet("Other"); second.createRow(0).createCell(0).setCellValue("different") }
      val out = Files.newOutputStream(path)
      try book.write(out) finally out.close()
    } finally book.close()
  }

  test("registry distinguishes ordinary JSON, NDJSON, overrides, and unknown bytes") {
    val dir = Files.createTempDirectory("formats-")
    try {
      val json = dir.resolve("value.json"); Files.writeString(json,"{\"id\":1}\n")
      assert(Formats.detect(json).id == "json" && Formats.views("json") == Seq("bytes"))
      assert(Formats.detect(json,FileOptions(format=Some("ndjson"))).id == "ndjson")
      val csv = dir.resolve("value.CSV"); Files.writeString(csv,"id\n1\n")
      assert(Formats.detect(csv).id == "csv")
      val binary = dir.resolve("value.unknown"); Files.write(binary,Array[Byte](0,1,2))
      assert(Formats.detect(binary).id == "bin" && Formats.detect(binary).count(binary,FileOptions()).isEmpty)
      assert(Formats.views("sql") == Seq("records","bytes"))
      intercept[IllegalArgumentException](Formats.byId("missing"))
    } finally Data.deleteTree(dir)
  }

  test("ServiceLoader providers supply detection, schema, counts and streaming without server changes") {
    val dir = Files.createTempDirectory("format-provider-")
    val allocator = new RootAllocator()
    try {
      val file = dir.resolve("new.dacptest"); Files.writeString(file,"id\n1\n2\n")
      val handler = Formats.detect(file)
      assert(handler.id == "test-format" && Formats.views(handler.id) == Seq("records","bytes"))
      assert(handler.count(file,FileOptions()).contains(2L))
      var rows = 0
      handler.stream(file,FileOptions(),allocator,1,1)(_ => (), root => rows += root.getRowCount, () => ())
      assert(rows == 2)
    } finally { allocator.close(); Data.deleteTree(dir) }
  }

  test("malformed Excel preserves bytes listing and returns a safe format failure for records") {
    val dir = Files.createTempDirectory("bad-excel-")
    try {
      val file = dir.resolve("private.xls"); Files.writeString(file,"private invalid contents")
      val handler = Formats.detect(file)
      assert(handler.id == "excel" && handler.cheapSchema(file,FileOptions()).isEmpty)
      val error = intercept[FormatFailure](handler.rows(file,FileOptions()))
      assert(error.getMessage == "Invalid Excel workbook.")
    } finally Data.deleteTree(dir)
  }

  test("YAML loads private Excel selection and ordered explicit schema") {
    val dir = Files.createTempDirectory("format-config-")
    try {
      val yaml = dir.resolve("server.yaml")
      Files.writeString(yaml,"""host: localhost
public_host: localhost
tls_cert: cert.pem
tls_key: key.pem
tls_ca: ca.pem
output_root: output
allowed_uri_prefixes: [dacp://localhost:8815/office]
datasets:
  office:
    items:
      /orders:
        file: private.xlsx
        format: excel
        sheet: Orders
        header_row: 2
        schema:
          - {name: id, type: int64}
          - {name: amount, type: float64}
""")
      val binding = Config.load(yaml).datasets("office").head
      assert(binding.options.format.contains("excel") && binding.options.sheet.contains("Orders"))
      assert(binding.options.headerRow == 2)
      assert(binding.options.columns.get.getFields.asScala.map(_.getName).toSeq == Seq("id","amount"))
      intercept[IllegalArgumentException](FileOptions(headerRow=0))
    } finally Data.deleteTree(dir)
  }

  test("ordinary file listings avoid record parsing even for malformed CSV content") {
    val dir = Files.createTempDirectory("listing-cost-")
    try {
      val file = dir.resolve("private.csv"); Files.writeString(file,"id,note\n1,\"unterminated")
      val config = Config("localhost",8815,"localhost",Map.empty,Seq.empty,dir,dir,dir)
      val adapter = new NamespaceAdapter(Seq(PublishedItem("/orders",file=Some(file))),config)
      val metadata = adapter.list().head
      assert(metadata.format == "csv" && metadata.recordCount.isEmpty && metadata.schema.isEmpty)
      intercept[Exception](adapter.count("/orders"))
    } finally Data.deleteTree(dir)
  }

  for (extension <- Seq("xls", "xlsx")) {
    test(s"$extension event reader handles header names, typed cells, nulls and cached formulas") {
      val dir = Files.createTempDirectory("excel-")
      try {
        val file = dir.resolve(s"private.$extension"); workbook(file,header=2)
        val options = FileOptions(headerRow=2)
        assert(Formats.detect(file).id == "excel")
        val reader = ExcelFormat.rows(file,options)
        try {
          assert(reader.names == Seq("id","column_2","id_2","flag","calculated"))
          val rows = reader.iterator.toVector
          assert(rows == Vector(Vector(1L,null,"value",true,2L),Vector(2L,null,"value",true,3L)))
        } finally reader.close()
        assert(ExcelFormat.count(file,options).contains(2L))
        val schema = ExcelFormat.schema(file,options,2,1)
        assert(schema.getFields.asScala.map(_.getType.toString).toSeq == Seq("Int(64, true)","Utf8","Utf8","Bool","Int(64, true)"))
        val explicit = new org.apache.arrow.vector.types.pojo.Schema(reader.names.map(name => Data.field(name,"utf8")).asJava)
        assert(ExcelFormat.schema(file,options.copy(columns=Some(explicit)),1,1) == explicit)
        assert(ExcelFormat.cheapSchema(file,options).isEmpty && ExcelFormat.cheapCount(file,options).isEmpty)
      } finally Data.deleteTree(dir)
    }
    test(s"$extension requires a sheet selection for multisheet workbooks and supports early close") {
      val dir = Files.createTempDirectory("excel-selection-")
      try {
        val file = dir.resolve(s"private.$extension"); workbook(file,multiple=true,rows=100)
        assert(intercept[FormatFailure](ExcelFormat.rows(file,FileOptions())).getMessage.contains("multiple sheets"))
        assert(intercept[FormatFailure](ExcelFormat.rows(file,FileOptions(sheet=Some("Missing")))).getMessage.contains("not found"))
        val reader = ExcelFormat.rows(file,FileOptions(sheet=Some("Orders")))
        assert(reader.iterator.next().head == 1L)
        reader.close()
        assert(!Thread.getAllStackTraces.keySet().asScala.exists(t => t.isAlive && t.getName == "dacp-excel-reader"))
        assert(ExcelFormat.count(file,FileOptions(sheet=Some("Other"))).contains(0L))
      } finally Data.deleteTree(dir)
    }
  }

  test("Flight exposes Excel records, original bytes, explicit counts and private sheet aliases") {
    val dir = Files.createTempDirectory("excel-flight-")
    val allocator = new RootAllocator()
    val outputs = new OutputRegistry(10000)
    try {
      val file = dir.resolve("private-workbook.xlsx"); workbook(file,multiple=true)
      val config = Config("localhost",8815,"localhost",Map("public" -> Seq(
        PublishedItem("/orders",file=Some(file),options=FileOptions(sheet=Some("Orders"))),
        PublishedItem("/other",file=Some(file),options=FileOptions(sheet=Some("Other"))))),
        Seq("dacp://localhost:8815/public"),dir,dir,dir,batchSize=1,inferSample=1)
      val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(config,allocator,outputs)).build().start()
      val client = FlightClient.builder(allocator,server.getLocation).build()
      try {
        val listing = Metadata.DataSet.parseFrom(client.getInfo(FlightDescriptor.path("datasets","public")).getAppMetadata)
        val item = listing.getItems(0)
        assert(item.getFormat == "excel" && item.getViewsList.asScala == Seq("records","bytes"))
        assert(!item.hasRecordCount && item.getArrowSchema.isEmpty)
        assert(!listing.toString.contains("private-workbook") && !listing.toString.contains("Orders"))
        val info = client.getInfo(FlightDescriptor.path("datasets","public","/orders"))
        val stream = client.getStream(info.getEndpoints.get(0).getTicket)
        try { var count = 0; while (stream.next()) count += stream.getRoot.getRowCount; assert(count == 2) } finally stream.close()
        val ticket = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder.setView("bytes").build()
        val bytes = client.getStream(new Ticket(ticket.toByteArray)); val data = new java.io.ByteArrayOutputStream()
        try { while (bytes.next()) {
          val root = bytes.getRoot
          (0 until root.getRowCount).foreach(i => data.write(root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector].get(i)))
        }} finally bytes.close()
        assert(data.toByteArray.sameElements(Files.readAllBytes(file)))
        val counted = client.getInfo(FlightDescriptor.path("datasets","public","count","/orders"))
        assert(Metadata.Item.parseFrom(counted.getAppMetadata).getRecordCount == 2L)
      } finally { client.close(); server.close() }
    } finally { outputs.close(); allocator.close(); Data.deleteTree(dir) }
  }
}
