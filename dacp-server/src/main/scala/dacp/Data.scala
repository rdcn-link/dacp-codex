package dacp

import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.memory.BufferAllocator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.commons.csv.CSVFormat

case class Item(path: String, format: String, schema: Option[Schema])
trait DataSetAdapter { def list(): Seq[Item]; def describe(item: String): Item }
class DirectoryAdapter(base: Path, batchSize: Int, inferSample: Int, excludeMetadata: Boolean = false) extends DataSetAdapter {
  private val root = base.toRealPath()
  def resolve(item: String): Path = {
    require(item.nonEmpty && !Path.of(item).isAbsolute && !item.split("/", -1).exists(x => x == ".." || x == "." || x.isEmpty), "invalid item path")
    val p = root.resolve(item).toRealPath()
    require(p.startsWith(root) && Files.isRegularFile(p), "item escapes dataset or is not a file")
    p
  }
  def describe(item: String): Item = {
    val p = resolve(item); val f = Data.format(p)
    val schema = f match {
      case "csv" | "json" => Data.schema(p,batchSize,inferSample)
      case "parquet" => Parquet.schema(p)
      case _ => Data.bytesSchema
    }
    Item(item,f,Some(schema))
  }
  // Architecture: recurse and flatten files, never return directories or follow escaping symlinks.
  def list(): Seq[Item] = {
    val walk = Files.walk(root)
    try walk.iterator.asScala.filter(p => Files.isRegularFile(p) && (!excludeMetadata || p.getFileName.toString != "_schema.json") && p.toRealPath().startsWith(root))
      .map { p => val f = Data.format(p); Item(root.relativize(p).toString.replace('\\','/'), f,
        if (f == "parquet") Some(Parquet.schema(p)) else None) }.toSeq.sortBy(_.path)
    finally walk.close()
  }
}
class OutputRegistry(ttlMillis: Long) extends AutoCloseable {
  require(ttlMillis > 0)
  private case class Entry(path: Path, expires: Long)
  private val entries = new ConcurrentHashMap[String, Entry]()
  private val expired = ConcurrentHashMap.newKeySet[String]()
  private val timer = Executors.newSingleThreadScheduledExecutor((r: Runnable) => { val t = new Thread(r, "dacp-expiry"); t.setDaemon(true); t })
  timer.scheduleAtFixedRate(() => cleanup(), 50, 50, TimeUnit.MILLISECONDS)
  def register(id: String, path: Path): Unit = synchronized {
    require(!entries.containsKey(id) && !expired.contains(id), "output name already used")
    entries.put(id, Entry(path, System.currentTimeMillis() + ttlMillis))
  }
  def lookup(id: String): Option[Path] = synchronized {
    cleanup()
    if (expired.contains(id)) throw new Expired(id)
    Option(entries.get(id)).map(_.path)
  }
  def cleanup(): Unit = synchronized {
    val now = System.currentTimeMillis()
    entries.asScala.foreach { case (id, e) => if (now >= e.expires) {
      entries.remove(id); expired.add(id); Data.deleteTree(e.path)
    }}
  }
  def close(): Unit = { timer.shutdownNow(); entries.values.asScala.foreach(e => Data.deleteTree(e.path)); entries.clear() }
}
class Expired(id: String) extends RuntimeException(s"EXPIRED: output $id has expired; rerun job from source")

object Data {
  val mapper = new ObjectMapper()
  val bytesSchema = new Schema(List(new Field("data", FieldType.nullable(new ArrowType.Binary()), null)).asJava)
  def format(p: Path): String = p.getFileName.toString.split("\\.").last.toLowerCase match {
    case "csv" => "csv"; case "json" | "jsonl" | "ndjson" => "json"; case "parquet" => "parquet"; case _ => "bin"
  }
  def deleteTree(p: Path): Unit = if (Files.exists(p)) { val s = Files.walk(p); try s.iterator.asScala.toSeq.reverse.foreach(Files.deleteIfExists) finally s.close() }
  trait Rows extends AutoCloseable { def names: Seq[String]; def iterator: Iterator[Seq[Any]] }
  def rows(p: Path): Rows = format(p) match {
    case "csv" => new Rows {
      private val reader = Files.newBufferedReader(p, UTF_8)
      private val parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)
      val names = parser.getHeaderNames.asScala.toSeq
      val iterator = parser.iterator.asScala.map { row =>
        require(row.size() == names.size, "SCHEMA_CONFLICT: CSV column count")
        names.map(n => { val v = row.get(n); if (v.isEmpty) null else v })
      }
      def close(): Unit = parser.close()
    }
    case "json" => new Rows {
      private val reader = Files.newBufferedReader(p, UTF_8)
      private val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).filter(_.trim.nonEmpty).buffered
      private def parse(s: String): JsonNode = { val n = mapper.readTree(s); require(n.isObject, "JSON must be newline-delimited objects"); n }
      val names = if (lines.hasNext) parse(lines.head).fieldNames.asScala.toSeq else Seq.empty
      val iterator = lines.map { line => val n = parse(line)
        require(n.fieldNames.asScala.toSet == names.toSet, "SCHEMA_CONFLICT: JSON fields changed")
        names.map { k => val v = n.get(k); if (v.isNull) null else if (v.isBoolean) v.booleanValue else if (v.isIntegralNumber) v.longValue else if (v.isFloatingPointNumber) v.doubleValue else if (v.isTextual) v.textValue else throw new IllegalArgumentException("nested JSON is unsupported") }
      }
      def close(): Unit = reader.close()
    }
    case _ => throw new IllegalArgumentException("records supported for csv/json/parquet")
  }
  private def kind(v: Any): String = v match {
    case null => "null"
    case _: Boolean => "bool"
    case _: Long => "int64"
    case _: Double => "float64"
    case s: String => if (s == "true" || s == "false") "bool" else if (s.matches("[-+]?\\d+")) { try { s.toLong; "int64" } catch { case _: NumberFormatException => "utf8" } } else if (s.matches("[-+]?(?:\\d+\\.\\d*|\\d*\\.\\d+)(?:[eE][-+]?\\d+)?|[-+]?\\d+[eE][-+]?\\d+")) "float64" else "utf8"
  }
  def field(name: String, kind: String): Field = new Field(name, FieldType.nullable(kind match {
    case "int64" => new ArrowType.Int(64, true)
    case "float64" => new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
    case "bool" => new ArrowType.Bool()
    case "utf8" | "string" => new ArrowType.Utf8()
    case x => throw new IllegalArgumentException(s"unsupported schema type $x")
  }), null)
  def schema(p: Path, batchSize: Int, sample: Int, explicit: Option[Schema] = None): Schema = {
    require(batchSize > 0 && sample > 0)
    val r = rows(p)
    try {
      explicit match {
        case Some(s) => require(s.getFields.asScala.map(_.getName).toSeq == r.names, "SCHEMA_CONFLICT: explicit columns do not match"); s
        case None =>
          val types = Array.fill(r.names.size)("null")
          r.iterator.take(Math.multiplyExact(batchSize, sample)).foreach(row => row.zipWithIndex.foreach { case (v, i) =>
            val t = kind(v)
            if (types(i) == "null") types(i) = t
            else require(t == "null" || t == types(i), s"SCHEMA_CONFLICT: ${r.names(i)}; specify explicit schema")
          })
          new Schema(r.names.zip(types).map { case (n,t) => field(n, if (t == "null") "utf8" else t) }.asJava)
      }
    } finally r.close()
  }
  // Fixed schema after sampling: fail on conflicts, never silently widen or restart a stream.
  def set(v: FieldVector, i: Int, value: Any): Unit = {
    if (value == null) v.setNull(i)
    else try v match {
      case x: BigIntVector => require(kind(value) == "int64"); x.setSafe(i, value.toString.toLong)
      case x: Float8Vector => require(Set("int64", "float64").contains(kind(value))); x.setSafe(i, value.toString.toDouble)
      case x: BitVector => require(kind(value) == "bool"); x.setSafe(i, if (value.toString == "true") 1 else 0)
      case x: VarCharVector => x.setSafe(i, value.toString.getBytes(UTF_8))
      case _ => throw new IllegalArgumentException("unsupported Arrow type")
    } catch { case e: Exception => throw new IllegalArgumentException(s"SCHEMA_CONFLICT: ${v.getName}; specify explicit schema", e) }
  }
  def fill(root: VectorSchemaRoot, rows: Seq[Seq[Any]]): Unit = {
    root.clear(); root.allocateNew()
    rows.zipWithIndex.foreach { case (row,i) => root.getFieldVectors.asScala.zip(row).foreach { case (v,x) => set(v,i,x) } }
    root.setRowCount(rows.size)
  }
}
