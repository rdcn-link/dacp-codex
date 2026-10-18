package dacp

import java.nio.file.Path
import org.apache.arrow.vector.types.pojo.Schema

case class Item(path: String, format: String, schema: Option[Schema], kind: String = "data",
                lastModifiedTimeMs: Option[Long] = None, totalSizeBytes: Option[Long] = None,
                recordCount: Option[Long] = None, views: Option[Seq[String]] = None) {
  require(views.forall(v => v.distinct.size == v.size && v.forall(Set("records","bytes"))), "invalid item views")
}
trait DataSetAdapter { def list(): Seq[Item]; def describe(item: String): Item }
/** Logical paths never encode storage paths. The root is an implicit folder. */
object LogicalPath {
  def validate(path: String): String = {
    require(path.startsWith("/") && !path.contains("\\") && !path.exists(_.isControl), "expected absolute logical path")
    require(path == "/" || path.substring(1).split("/", -1).forall(p => p.nonEmpty && p != "." && p != ".."), "invalid logical path")
    path
  }
  def parent(path: String): String = { val i = path.lastIndexOf('/'); if (i <= 0) "/" else path.take(i) }
}

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

