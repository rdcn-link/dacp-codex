package dacp
import java.nio.file.{Files, Path}
import java.io.{InputStream, BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.avro.generic.GenericData
import org.apache.commons.csv.CSVFormat

// Operator contract: input manifest + Arrow IPC streams; stdout is logs, output contains Parquet + schema metadata.
object FormatConvert {
  class BinaryInput(reader: ArrowStreamReader) extends InputStream {
    private var bytes = Array.emptyByteArray; private var offset = 0; private var row = 0; private var ended = false
    override def read(): Int = { val b = new Array[Byte](1); if (read(b,0,1) < 0) -1 else b(0) & 255 }
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (len == 0) return 0
      if (ended) return -1
      while (offset == bytes.length) {
        val root = reader.getVectorSchemaRoot
        if (row >= root.getRowCount) { if (!reader.loadNextBatch()) { ended = true; return -1 }; row = 0 }
        if (root.getRowCount > 0) { bytes = root.getVector(0).asInstanceOf[VarBinaryVector].get(row); row += 1; offset = 0 }
      }
      val n = math.min(len,bytes.length-offset); System.arraycopy(bytes,offset,b,off,n); offset += n; n
    }
  }
  def parsed(in: InputStream, format: String): Records.Rows = format match {
    case "csv" => new Records.Rows {
      private val parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(new InputStreamReader(in,UTF_8))
      val names = parser.getHeaderNames.asScala.toSeq
      val iterator = parser.iterator.asScala.map(r => { require(r.size == names.size,"SCHEMA_CONFLICT: CSV width"); names.map(n => { val s = r.get(n); if (s.isEmpty) null else s }) })
      def close(): Unit = parser.close()
    }
    case "ndjson" => new Records.Rows {
      private val reader = new BufferedReader(new InputStreamReader(in,UTF_8))
      private val nodes = Iterator.continually(reader.readLine()).takeWhile(_ != null).filter(_.trim.nonEmpty).map(RuntimeJson.mapper.readTree).buffered
      val names = if (nodes.hasNext) nodes.head.fieldNames.asScala.toSeq else Seq.empty
      val iterator = nodes.map { n => require(n.isObject && n.fieldNames.asScala.toSet == names.toSet,"SCHEMA_CONFLICT: JSON fields"); names.map { k =>
        val v = n.get(k); if (v.isNull) null else if (v.isBoolean) v.booleanValue else if (v.isIntegralNumber) v.longValue else if (v.isFloatingPointNumber) v.doubleValue else if (v.isTextual) v.textValue else throw new IllegalArgumentException("nested JSON unsupported")
      }}
      def close(): Unit = reader.close()
    }
    case _ => throw new IllegalArgumentException("format_convert accepts csv/ndjson only")
  }
  def size(value: String): Long = {
    val pattern = "(?i)([0-9]+)(B|KB|MB|GB)?".r
    value match { case pattern(n,u) => val mult = Option(u).map(_.toUpperCase) match { case Some("KB") => 1024L; case Some("MB") => 1024L*1024; case Some("GB") => 1024L*1024*1024; case _ => 1L }; val v = Math.multiplyExact(n.toLong,mult); require(v > 0); v
      case _ => throw new IllegalArgumentException("invalid row_group_size") }
  }
  def run(input: Path, output: Path, paramsPath: Path): Unit = {
    val params = RuntimeJson.mapper.readTree(Files.readAllBytes(paramsPath))
    def option(k: String,d: String) = Option(params.get(k)).map(_.asText()).getOrElse(d)
    require(option("to", "parquet") == "parquet", "only parquet output supported")
    require(option("on_conflict","fail") == "fail", "only on_conflict=fail supported")
    val batchSize = 1024; val sample = option("infer_sample","4").toInt; require(sample > 0)
    val explicit = Option(params.get("schema")).filter(_.isObject).map { node =>
      new Schema(node.fields.asScala.map(e => Records.field(e.getKey,e.getValue.asText)).toList.asJava)
    }
    require(!params.has("schema") || params.get("schema").isObject || params.get("schema").asText == "infer", "schema must be infer or name:type mapping")
    Files.createDirectories(output)
    val manifest = RuntimeJson.mapper.readTree(Files.readAllBytes(input.resolve("manifest.json")))
    val allocator = new RootAllocator(Long.MaxValue)
    val metadata = RuntimeJson.mapper.createObjectNode(); val schemas = metadata.putObject("items")
    try manifest.elements.asScala.zipWithIndex.foreach { case (entry,index) =>
      val format = option("from","auto") match { case "auto" => entry.get("format").asText; case f => f }
      val ipc = new ArrowStreamReader(new java.io.FileInputStream(input.resolve(entry.get("file").asText).toFile),allocator)
      try {
        val rows = parsed(new BinaryInput(ipc),format)
        try {
          val sampled = rows.iterator.take(Math.multiplyExact(batchSize,sample)).toVector
          val schema = explicit.getOrElse {
            val types = rows.names.indices.map { i =>
              val values = sampled.map(_(i)).filter(_ != null)
              // Reuse strict inference with an in-memory sample only; no full-file materialization.
              val ts = values.map(DataKind.kind).distinct
              require(ts.size <= 1,s"SCHEMA_CONFLICT: ${rows.names(i)}; specify explicit schema")
              Records.field(rows.names(i),ts.headOption.getOrElse("utf8"))
            }
            new Schema(types.asJava)
          }
          require(schema.getFields.asScala.map(_.getName).toSeq == rows.names,"SCHEMA_CONFLICT: explicit columns")
          val target = output.resolve(s"part-$index.parquet")
          val writer = ParquetOutput.writer(target,schema,option("compression","zstd"),size(option("row_group_size","128MB")))
          val root = VectorSchemaRoot.create(schema,allocator); val avroSchema = ParquetOutput.avro(schema)
          try (sampled.iterator ++ rows.iterator).grouped(batchSize).foreach { batch =>
            Records.fill(root,batch)
            (0 until root.getRowCount).foreach { i => val record = new GenericData.Record(avroSchema)
              root.getFieldVectors.asScala.foreach { v => val raw = v.getObject(i); record.put(v.getName,raw match { case t: org.apache.arrow.vector.util.Text => t.toString; case x => x }) }
              writer.write(record)
            }
          } finally { root.close(); writer.close() }
          schemas.put(target.getFileName.toString,schema.toJson)
        } finally rows.close()
      } finally ipc.close()
    } finally allocator.close()
    Files.write(output.resolve("_schema.json"),RuntimeJson.mapper.writeValueAsBytes(metadata))
  }
}
object DataKind {
  def kind(v: Any): String = v match {
    case _: Boolean => "bool"; case _: Long => "int64"; case _: Double => "float64"
    case s: String => if (s == "true" || s == "false") "bool" else if (s.matches("[-+]?\\d+")) { try {s.toLong; "int64"} catch {case _: NumberFormatException => "utf8"} }
      else if (s.matches("[-+]?(?:\\d+\\.\\d*|\\d*\\.\\d+)(?:[eE][-+]?\\d+)?|[-+]?\\d+[eE][-+]?\\d+")) "float64" else "utf8"
    case _ => throw new IllegalArgumentException("unsupported type")
  }
}
