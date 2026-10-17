package dacp

import java.nio.file.{Files, Path}
import scala.collection.mutable

/** Logical paths never encode storage paths. The root is an implicit folder. */
object LogicalPath {
  def validate(path: String): String = {
    require(path.startsWith("/") && !path.contains("\\") && !path.exists(_.isControl), "expected absolute logical path")
    require(path == "/" || path.substring(1).split("/", -1).forall(p => p.nonEmpty && p != "." && p != ".."), "invalid logical path")
    path
  }
  def parent(path: String): String = { val i = path.lastIndexOf('/'); if (i <= 0) "/" else path.take(i) }
}

case class PublishedItem(path: String, file: Option[Path] = None, sql: Option[String] = None,
                         table: Option[String] = None, kind: String = "data") {
  LogicalPath.validate(path)
  require(path != "/", "root folder is implicit")
  require(kind == "data" || kind == "folder", "unknown item kind")
  require(if (kind == "folder") file.isEmpty && sql.isEmpty && table.isEmpty
    else (file.isDefined && sql.isEmpty && table.isEmpty) || (file.isEmpty && sql.isDefined && table.isDefined),
    "data requires a file or sql source/table mapping; folders have no source")
}

/** Resolves published aliases only; no automatic publication of private source names. */
class NamespaceAdapter(bindings: Seq[PublishedItem], config: Config) extends DataSetAdapter {
  private val entries = mutable.Map.empty[String, PublishedItem]
  require(bindings.map(_.path).distinct.size == bindings.size, "duplicate logical path")
  bindings.foreach(b => entries(b.path) = b)
  bindings.foreach { b =>
    var parent = LogicalPath.parent(b.path)
    while (parent != "/") {
      entries.get(parent) match {
        case Some(p) => require(p.kind == "folder", "data item cannot have children")
        case None => entries(parent) = PublishedItem(parent, kind = "folder")
      }
      parent = LogicalPath.parent(parent)
    }
    b.sql.foreach(source => require(config.sqlSources.contains(source), "unknown private SQL source"))
  }
  private def entry(path: String): PublishedItem = {
    LogicalPath.validate(path)
    entries.getOrElse(path, throw new java.io.FileNotFoundException("logical item not found"))
  }
  def isFolder(path: String): Boolean = path == "/" || entry(path).kind == "folder"
  def resolve(path: String): (DataSetAdapter, String) = {
    require(path != "/", "folder has no content stream")
    val b = entry(path)
    require(b.kind == "data", "folder has no content stream")
    b.file match {
      case Some(file) =>
        val absolute = file.toAbsolutePath.normalize()
        (new DirectoryAdapter(absolute.getParent, config.batchSize, config.inferSample), absolute.getFileName.toString)
      case None => (new SqlAdapter(config.sqlSources(b.sql.get), config.batchSize), b.table.get)
    }
  }
  private def metadata(b: PublishedItem, infer: Boolean): Item = {
    if (b.kind == "folder") Item(b.path, "", None, "folder")
    else {
      val (adapter, internal) = resolve(b.path)
      val description = adapter match {
        case d: DirectoryAdapter if !infer =>
          val p = d.resolve(internal)
          val format = Data.format(p)
          Item(b.path, format, if (format == "parquet") Some(Parquet.schema(p)) else None)
        case other => other.describe(internal).copy(path = b.path)
      }
      val enriched = adapter match {
        case d: DirectoryAdapter => ItemProperties.file(description, d.resolve(internal))
        case _ => description
      }
      enriched.copy(path = b.path)
    }
  }
  private def snapshot(selected: Seq[PublishedItem], infer: Boolean): Seq[Item] =
    selected.map(metadata(_, infer))

  def count(path: String): Option[Long] = {
    val (source, internal) = resolve(path)
    source match {
      case sql: SqlAdapter => Some(sql.count(internal))
      case _ => describe(path).recordCount
    }
  }
  def describe(path: String): Item = {
    if (path == "/") ItemProperties.folder("/")
    else metadata(entry(path), infer = true)
  }
  def list(): Seq[Item] = snapshot(entries.values.toVector.sortBy(_.path), infer = false)
  def list(path: String): Seq[Item] = {
    LogicalPath.validate(path)
    if (path != "/") require(entry(path).kind == "folder", "listing requires a folder")
    snapshot(entries.values.toVector.filter(b => LogicalPath.parent(b.path) == path).sortBy(_.path), infer = false)
  }
}

object NamespaceAdapter {
  /** Task outputs receive opaque logical aliases, independent of operator filenames. */
  def output(base: Path, config: Config): NamespaceAdapter = {
    val privateItems = new DirectoryAdapter(base, config.batchSize, config.inferSample, excludeMetadata = true).list()
    val bindings = privateItems.zipWithIndex.map { case (item, index) =>
      PublishedItem(s"/result-${index + 1}", file = Some(base.resolve(item.path)))
    }
    new NamespaceAdapter(bindings, config)
  }
  def views(item: Item): Seq[String] = if (item.kind == "folder") Seq.empty
    else if (Set("csv", "json", "parquet", "arrow")(item.format)) Seq("records", "bytes") else Seq("bytes")
}

/** Exact metadata, with unknown distinct from zero. Contents and metadata are not a shared snapshot. */
object ItemProperties {
  def file(item: Item, path: Path): Item = {
    val attributes = Files.readAttributes(path, classOf[java.nio.file.attribute.BasicFileAttributes])
    val records = item.format match {
      case "csv" | "json" =>
        val rows = Data.rows(path)
        try Some(rows.iterator.foldLeft(0L)((n, _) => Math.addExact(n, 1L))) finally rows.close()
      case "parquet" => Some(Parquet.recordCount(path))
      case _ => None
    }
    item.copy(lastModifiedTimeMs=Some(attributes.lastModifiedTime().toMillis),
      totalSizeBytes=Some(attributes.size()), recordCount=records)
  }
  def folder(path: String): Item = Item(path, "", None, "folder")
}
