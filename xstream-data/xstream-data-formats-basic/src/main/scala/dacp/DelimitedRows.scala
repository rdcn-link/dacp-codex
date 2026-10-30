package dacp

import _root_.xstream.data._
import java.nio.file.{Files,Path}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.commons.csv.CSVFormat
object DelimitedRows {
  private val mapper = Data.mapper
  def read(p: ByteSource, format: String): Records.Rows = format match {
    case "csv" => new Records.Rows {
      private val reader = new java.io.BufferedReader(new java.io.InputStreamReader(p.open(()=>()),UTF_8))
      private val parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)
      val names = parser.getHeaderNames.asScala.toSeq
      val iterator = parser.iterator.asScala.map { row =>
        require(row.size() == names.size, "SCHEMA_CONFLICT: CSV column count")
        names.map(n => { val v = row.get(n); if (v.isEmpty) null else v })
      }
      def close(): Unit = parser.close()
    }
    case "ndjson" => new Records.Rows {
      private val reader = new java.io.BufferedReader(new java.io.InputStreamReader(p.open(()=>()),UTF_8))
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
