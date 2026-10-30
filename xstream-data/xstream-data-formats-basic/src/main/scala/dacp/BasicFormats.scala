package dacp

import _root_.xstream.data._
import java.nio.file.Path
class DelimitedFormat(val id: String, val extensions: Set[String]) extends FormatHandler {
    override val records = true
    override def rows(path: ByteSource, options: FileOptions): Records.Rows = DelimitedRows.read(path,id)
  }

class CsvFormat extends DelimitedFormat("csv",Set("csv"))
class NdjsonFormat extends DelimitedFormat("ndjson",Set("jsonl","ndjson"))
class JsonFormat extends FormatHandler { val id = "json"; val extensions = Set("json") }
class BinaryFormat extends FormatHandler { val id = "bin"; val extensions = Set.empty[String] }
