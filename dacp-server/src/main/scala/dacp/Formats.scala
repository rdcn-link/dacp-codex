package dacp

import java.nio.file.{Files, Path}
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

object Formats {
  private class Delimited(val id: String, val extensions: Set[String]) extends FormatHandler {
    override val records = true
    override def rows(path: Path, options: FileOptions): Records.Rows = Data.delimitedRows(path,id)
  }
  private object ParquetFormat extends FormatHandler {
    val id = "parquet"; val extensions = Set("parquet")
    override def signatureMatches(header: Array[Byte]): Boolean = header.take(4).sameElements(Array[Byte](80,65,82,49))
    override val records = true
    override def cheapSchema(p: Path, o: FileOptions) = Some(Parquet.schema(p))
    override def cheapCount(p: Path, o: FileOptions) = Some(Parquet.recordCount(p))
    override def schema(p: Path, o: FileOptions, b: Int, s: Int): Schema = {
      val schema = Parquet.schema(p)
      require(o.columns.forall(_ == schema), "SCHEMA_CONFLICT: configured schema differs from Parquet metadata")
      schema
    }
    override def scan(p: Path, o: FileOptions, request: Scan, a: BufferAllocator, b: Int, s: Int,
                      listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener, demand: () => Unit): Unit = {
      schema(p,o,b,s)
      ScanStreams.stream(request,a,listener) { filtered =>
        Parquet.stream(p,a,b,Some(request))(root => filtered.start(root),_ => filtered.putNext(),demand)
      }
    }
    override def count(p: Path, o: FileOptions) = cheapCount(p,o)
    override def stream(p: Path, o: FileOptions, a: BufferAllocator, b: Int, s: Int)(
        start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit =
      Parquet.stream(p,a,b)(start,emit,demand)
  }
  private object BinaryFormat extends FormatHandler { val id = "bin"; val extensions = Set.empty[String] }
  private class ContainerFormat(val id: String, val extensions: Set[String]) extends FormatHandler
  private object JsonFormat extends FormatHandler { val id = "json"; val extensions = Set("json") }
  private val builtins: Seq[FormatHandler] = Seq(new Delimited("csv",Set("csv")),
    new Delimited("ndjson",Set("jsonl","ndjson")), JsonFormat, ParquetFormat, ExcelFormat, BinaryFormat)
  // Additional Java/Scala providers use META-INF/services/dacp.FormatHandler; restart to load.
  private lazy val handlers: Seq[FormatHandler] = {
    val all = builtins ++ Containers.registered.map(r => new ContainerFormat(r.id,r.extensions)) ++ ServiceLoader.load(classOf[FormatHandler],PluginLoader.classLoader).iterator().asScala.toVector
    require(all.forall(h => h.id.matches("[a-z][a-z0-9-]*") && h.id != "sql"), "invalid or reserved format id")
    require(all.forall(_.extensions.forall(_.matches("[a-z0-9]+"))), "invalid format extension")
    require(all.map(_.id).distinct.size == all.size, "duplicate format handler id")
    require(all.flatMap(_.extensions).distinct.size == all.flatMap(_.extensions).size, "duplicate format extension")
    all
  }
  def initialize(): Unit = { handlers; () }
  def byId(id: String): FormatHandler = handlers.find(_.id == id)
    .getOrElse(throw new IllegalArgumentException("unsupported configured format"))
  def views(id: String): Seq[String] = if (id == "sql" || byId(id).records) Seq("records","bytes") else Seq("bytes")
  def extensionFormat(name: String): FormatHandler = {
    val ext = name.split("\\.").last.toLowerCase(java.util.Locale.ROOT)
    Containers.formatForName(name).map(byId).getOrElse(handlers.find(_.extensions(ext)).getOrElse(BinaryFormat))
  }
  def detect(path: Path, options: FileOptions = FileOptions()): FormatHandler = {
    val handler = options.format match {
      case Some(id) => byId(id)
      case None =>
        val in = Files.newInputStream(path)
        val signature = try in.readNBytes(8) finally in.close()
        val ext = path.getFileName.toString.split("\\.").last.toLowerCase(java.util.Locale.ROOT)
        Containers.formatForName(path.getFileName.toString,compoundOnly=true).map(byId).orElse(handlers.find(_.signatureMatches(signature)))
          .orElse(handlers.find(_.matches(signature,ext))).getOrElse(BinaryFormat)
    }
    handler.validateOptions(options)
    require(handler.records || options.columns.isEmpty, "schema requires a records format")
    handler
  }
}
