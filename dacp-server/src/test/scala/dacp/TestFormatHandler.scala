package dacp

import java.nio.file.Path

/** Test provider exercises the same classpath registration used by external Java/Scala JARs. */
class TestFormatHandler extends FormatHandler {
  val id = "test-format"
  val extensions = Set("dacptest")
  override val records = true
  override def rows(path: Path, options: FileOptions): Records.Rows = Data.delimitedRows(path,"csv")
}
