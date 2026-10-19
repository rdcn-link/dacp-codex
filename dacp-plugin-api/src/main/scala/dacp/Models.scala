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

