package dacp

import _root_.xstream.data._

import java.nio.file.Path

/** Test provider exercises the same classpath registration used by external Java/Scala JARs. */
class TestFormatHandler extends FormatHandler {
  val id = "test-format"
  val extensions = Set("dacptest")
  override val records = true
  override def rows(path: ByteSource, options: FileOptions): Records.Rows = DelimitedRows.read(path,"csv")
}
