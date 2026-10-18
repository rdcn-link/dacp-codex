package dacp

import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.memory.BufferAllocator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.commons.csv.CSVFormat

class DirectoryAdapter(base: Path, batchSize: Int, inferSample: Int, excludeMetadata: Boolean = false, val options: FileOptions = FileOptions()) extends DataSetAdapter {
  private val root = base.toRealPath()
  def resolve(item: String): Path = {
    require(item.nonEmpty && !Path.of(item).isAbsolute && !item.split("/", -1).exists(x => x == ".." || x == "." || x.isEmpty), "invalid item path")
    val p = root.resolve(item).toRealPath()
    require(p.startsWith(root) && Files.isRegularFile(p), "item escapes dataset or is not a file")
    p
  }
  def describe(item: String): Item = {
    val p = resolve(item); val handler = Formats.detect(p,options)
    Item(item,handler.id,Some(if (handler.isInstanceOf[StructuredFileHandler]) PluginSchemas.bytes else handler.schema(p,options,batchSize,inferSample)))
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
object Data {
  val mapper = new ObjectMapper()
  def format(p: Path): String = Formats.detect(p).id
  def deleteTree(p: Path): Unit = if (Files.exists(p)) { val s = Files.walk(p); try s.iterator.asScala.toSeq.reverse.foreach(Files.deleteIfExists) finally s.close() }
  def delimitedRows(p: Path, format: String): Records.Rows = format match {
    case "csv" => new Records.Rows {
      private val reader = Files.newBufferedReader(p, UTF_8)
      private val parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)
      val names = parser.getHeaderNames.asScala.toSeq
      val iterator = parser.iterator.asScala.map { row =>
        require(row.size() == names.size, "SCHEMA_CONFLICT: CSV column count")
        names.map(n => { val v = row.get(n); if (v.isEmpty) null else v })
      }
      def close(): Unit = parser.close()
    }
    case "ndjson" => new Records.Rows {
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
    case _ => throw new IllegalArgumentException("delimited records require csv or ndjson")
  }
}
