package dacp
import java.nio.file.Path

/** Publisher-provided count tied to the file version observed during registration. */
case class FileRecordCount(value: Long, size: Long, modified: java.nio.file.attribute.FileTime) {
  require(value >= 0 && size >= 0,"file count and size must be nonnegative")
  def matches(attributes: java.nio.file.attribute.BasicFileAttributes): Boolean =
    size == attributes.size() && modified == attributes.lastModifiedTime()
}
case class PublishedItem(path: String, file: Option[Path] = None, sql: Option[String] = None,
                         table: Option[String] = None, kind: String = "data", options: FileOptions = FileOptions(), recordCount: Option[FileRecordCount] = None, fileRoot: Option[Path] = None) {
  LogicalPath.validate(path)
  require(recordCount.isEmpty || (file.isDefined && kind == "data"), "published counts require a file data source")
  require(path != "/", "root folder is implicit")
  require(kind == "data" || kind == "folder", "unknown item kind")
  require(if (kind == "folder") file.isEmpty && sql.isEmpty && table.isEmpty
    else (file.isDefined && sql.isEmpty && table.isEmpty) || (file.isEmpty && sql.isDefined && table.isDefined),
    "data requires a file or sql source/table mapping; folders have no source")
}


/** File/JDBC plugin convenience; native and mounted providers use generic DatasetDefinition. */
object FileDatasets {
  def mixed(items:Seq[PublishedItem]):DatasetDefinition={
    import scala.jdk.CollectionConverters._
    DatasetDefinition("mixed",source=Map("bindings"->items.asJava))
  }
}
