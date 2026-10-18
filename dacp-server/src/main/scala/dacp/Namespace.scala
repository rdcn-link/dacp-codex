package dacp

import java.nio.file.{Files, Path}
import scala.collection.mutable

object FileRecordCounts {
  def capture(path: Path, options: FileOptions): Option[FileRecordCount] = {
    val handler = Formats.detect(path,options)
    if (!handler.records || handler.isInstanceOf[StructuredFileHandler]) None else {
      val before = Files.readAttributes(path,classOf[java.nio.file.attribute.BasicFileAttributes])
      handler.count(path,options).flatMap { value =>
        val snapshot = FileRecordCount(value,before.size(),before.lastModifiedTime())
        val after = Files.readAttributes(path,classOf[java.nio.file.attribute.BasicFileAttributes])
        if (snapshot.matches(after)) Some(snapshot) else None
      }
    }
  }
}

/** Resolves published aliases only; no automatic publication of private source names. */
class NamespaceAdapter(bindings: Seq[PublishedItem], config: Config, metadataDemand: () => Unit = () => ()) extends DatasetAccess {
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
    require(b.file.isDefined || b.options == FileOptions(), "format options require a file source")
    b.options.format.foreach(id => Formats.byId(id).validateOptions(b.options))
    b.sql.foreach(source => require(config.sqlSources.contains(source), "unknown private SQL source"))
  }
  private case class StructureSource(binding: PublishedItem, handler: StructuredFileHandler) {
    def use[A](check: () => Unit)(f: DatasetAccess => A): A = {
      check()
      val (adapter,internal) = resolve(binding.path)
      val file = adapter.asInstanceOf[DirectoryAdapter].resolve(internal)
      val access = handler.openStructure(file,binding.options,config,check)
      try f(access) finally access.close()
    }
  }
  private def structures(parent: Option[String]): Map[String,StructureSource] = bindings.filter { b =>
    b.file.nonEmpty && parent.forall(p => p == b.path || p.startsWith(b.path + "/"))
  }.flatMap { b =>
    val (adapter,internal) = resolve(b.path)
    val path = adapter.asInstanceOf[DirectoryAdapter].resolve(internal)
    Formats.detect(path,b.options) match {
      case handler: StructuredFileHandler => Some(b.path -> StructureSource(b,handler))
      case _ => None
    }
  }.toMap
  private def publicChild(root: String, item: Item): Item = {
    LogicalPath.validate(item.path)
    require(item.path != "/", "structured listing must not contain its root")
    val result = item.copy(path=root + item.path)
    if (item.kind == "folder") result.copy(lastModifiedTimeMs=None,totalSizeBytes=None,recordCount=None)
    else { require(item.kind == "data","invalid structured item kind"); result }
  }
  private def structuredListing(parent: Option[String]): Map[String,Item] = {
    val result = mutable.Map.empty[String,Item]
    structures(parent).foreach { case (root,source) =>
      source.use(metadataDemand) { access =>
        val children = parent match {
          case Some(p) if p == root || p.startsWith(root + "/") => access.list(p.substring(root.length) match { case "" => "/"; case path => path })
          case _ => access.list()
        }
        children.foreach { child =>
          metadataDemand()
          val item = publicChild(root,child)
          require(!result.contains(item.path) && !entries.contains(item.path),"structured item path conflict")
          result(item.path) = item
        }
      }
      result.keys.toVector.filter(_.startsWith(root + "/")).foreach { path =>
        var parent = LogicalPath.parent(path)
        while (parent != root) {
          require(!entries.contains(parent),"structured folder path conflict")
          require(result.get(parent).forall(_.kind == "folder"),"structured data item has children")
          result.getOrElseUpdate(parent,ItemProperties.folder(parent))
          parent = LogicalPath.parent(parent)
        }
      }
    }
    result.toMap
  }
  private def withStructure[A](path: String, check: () => Unit = metadataDemand)(f: (DatasetAccess,String) => A): Option[A] =
    structures(Some(path)).find { case (root,_) => path.startsWith(root + "/") }.map { case (root,source) =>
      source.use(check)(access => f(access,path.substring(root.length)))
    }
  private case class ContainerSource(file: Path, reader: ContainerReader, publicName: String) {
    lazy val index: Seq[ContainerEntry] = reader.index(file,publicName,metadataDemand)
  }
  private case class Archived(source: ContainerSource, entry: ContainerEntry)
  private lazy val archives: Map[String, ContainerSource] = bindings.flatMap { b =>
    b.file.flatMap { p =>
      val id = b.options.format.getOrElse(Formats.extensionFormat(p.getFileName.toString).id)
      if (Containers.supports(id)) Some(b.path -> ContainerSource(p,Containers.byId(id),b.path)) else None
    }
  }.toMap
  private def archived(roots: Map[String,ContainerSource] = archives): Map[String, Archived] = roots.toSeq.flatMap { case (alias,source) =>
    source.index.map(e => (alias + "/" + e.name) -> Archived(source,e))
  }.toMap
  private def virtual(parent: Option[String] = None): Map[String, Item] = {
    val result = mutable.Map.empty[String,Item]
    val roots = archives.filter { case (alias,_) => parent.forall(p => p == alias || p.startsWith(alias + "/")) }
    archived(roots).foreach { case (path,a) =>
      require(!entries.contains(path), "archive entry conflicts with published alias")
      result(path) = if (a.entry.directory) ItemProperties.folder(path) else
        Item(path,{ val id = Formats.extensionFormat(a.entry.name).id; if (Containers.supports(id)) "bin" else id },None,lastModifiedTimeMs=a.entry.modified,totalSizeBytes=a.entry.size)
      var parent = LogicalPath.parent(path)
      while (parent != "/" && !archives.contains(parent)) {
        require(!entries.contains(parent), "archive folder conflicts with published alias")
        result.getOrElseUpdate(parent,ItemProperties.folder(parent))
        parent = LogicalPath.parent(parent)
      }
    }
    val native = structuredListing(parent)
    require(result.keySet.intersect(native.keySet).isEmpty,"structured item path conflict")
    result.toMap ++ native
  }
  def withResolved[A](path: String, demand: () => Unit = () => ())(f: (DataSetAdapter,String) => A): A = {
    LogicalPath.validate(path)
    if (entries.contains(path)) { val (a,i) = resolve(path); f(a,i) }
    else {
      val (alias,source) = archives.find { case (alias,_) => path.startsWith(alias + "/") }
        .getOrElse(throw new java.io.FileNotFoundException("logical item not found"))
      val name = path.substring(alias.length + 1)
      source.reader.withFile(source.file,source.publicName,name,demand) { p =>
        // Nested containers remain binary data Items; no implicit recursive expansion.
        val options = if (Containers.supports(Formats.detect(p).id)) FileOptions(format=Some("bin")) else FileOptions()
        f(new DirectoryAdapter(p.getParent,config.batchSize,config.inferSample,options=options),p.getFileName.toString)
      }
    }
  }
  private def entry(path: String): PublishedItem = {
    LogicalPath.validate(path)
    entries.getOrElse(path, throw new java.io.FileNotFoundException("logical item not found"))
  }
  def isFolder(path: String): Boolean = path == "/" || describe(path).kind == "folder"
  def resolve(path: String): (DataSetAdapter, String) = {
    require(path != "/", "folder has no content stream")
    val b = entry(path)
    require(b.kind == "data", "folder has no content stream")
    b.file match {
      case Some(file) =>
        val absolute = file.toAbsolutePath.normalize()
        val base = b.fileRoot.getOrElse(absolute.getParent)
        b.fileRoot.foreach(root => require(absolute.toRealPath().startsWith(root),"file escapes dataset root"))
        (new DirectoryAdapter(base, config.batchSize, config.inferSample, options=b.options), base.relativize(absolute).toString.replace('\\','/'))
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
          val handler = Formats.detect(p,d.options)
          Item(b.path, handler.id, handler.cheapSchema(p,d.options))
        case other => other.describe(internal).copy(path = b.path)
      }
      val enriched = adapter match {
        case d: DirectoryAdapter => ItemProperties.file(description, d.resolve(internal), d.options, b.recordCount)
        case _ => description
      }
      if (Containers.supports(description.format) || (description.format != "sql" && Formats.byId(description.format).isInstanceOf[StructuredFileHandler]))
        Item(b.path,description.format,Some(PluginSchemas.bytes),"folder",views=Some(Seq("bytes")))
      else enriched.copy(path = b.path)
    }
  }
  private def snapshot(selected: Seq[PublishedItem], infer: Boolean): Seq[Item] =
    selected.map(metadata(_, infer))

  def count(path: String): Option[Long] = {
    LogicalPath.validate(path)
    withStructure(path)((access,child) => access.count(child)).getOrElse {
      val folder = path == "/" || entries.get(path).exists(_.kind == "folder") || archives.contains(path) || structures(Some(path)).contains(path) ||
        (!entries.contains(path) && virtual(Some(path)).get(path).exists(_.kind == "folder"))
      if (folder) None else withResolved(path) { (source,internal) =>
        source match {
          case sql: SqlAdapter => Some(sql.count(internal))
          case d: DirectoryAdapter => val p = d.resolve(internal); Formats.detect(p,d.options).count(p,d.options)
        }
      }
    }
  }
  def describe(path: String): Item = {
    LogicalPath.validate(path)
    if (path == "/") ItemProperties.folder("/")
    else if (entries.contains(path)) metadata(entry(path), infer = true)
    else withStructure(path) { (access,child) =>
      val item = access.describe(child)
      require(item.path == child,"structured describe returned a different path")
      publicChild(path.substring(0,path.length-child.length),item)
    }.getOrElse {
      val item = virtual(Some(path)).getOrElse(path,throw new java.io.FileNotFoundException("logical item not found"))
      if (item.kind == "folder") item else withResolved(path) { (a,i) => a.describe(i).copy(path=path,
        lastModifiedTimeMs=item.lastModifiedTimeMs,totalSizeBytes=item.totalSizeBytes) }
    }
  }
  def list(): Seq[Item] = (snapshot(entries.values.toVector,infer=false) ++ virtual().values).sortBy(_.path)
  def list(path: String): Seq[Item] = {
    LogicalPath.validate(path)
    require(isFolder(path), "listing requires a folder")
    (snapshot(entries.values.toVector.filter(b => LogicalPath.parent(b.path) == path),infer=false) ++
      virtual(Some(path)).values.filter(b => LogicalPath.parent(b.path) == path)).sortBy(_.path)
  }

  def stream(path: String, view: String, allocator: org.apache.arrow.memory.BufferAllocator,
             listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener, ready: () => Unit): Unit =
    {
      LogicalPath.validate(path)
      withStructure(path,ready)((access,child) => access.stream(child,view,allocator,listener,ready))
        .getOrElse(DatasetStreams.stream(this,path,view,allocator,config,listener,ready))
    }

}

object NamespaceAdapter {
  /** Task outputs receive opaque logical aliases, independent of operator filenames. */
  def output(base: Path, config: Config, metadataDemand: () => Unit = () => ()): NamespaceAdapter = {
    val privateItems = new DirectoryAdapter(base, config.batchSize, config.inferSample, excludeMetadata = true).list()
    val bindings = privateItems.zipWithIndex.map { case (item, index) =>
      PublishedItem(s"/result-${index + 1}", file = Some(base.resolve(item.path)))
    }
    new NamespaceAdapter(bindings, config, metadataDemand)
  }
  def views(item: Item): Seq[String] = item.views.getOrElse {
    if (item.format.isEmpty) Seq.empty else Formats.views(item.format)
  }
}

/** Exact metadata, with unknown distinct from zero. Contents and metadata are not a shared snapshot. */
object ItemProperties {
  def file(item: Item, path: Path, options: FileOptions = FileOptions(), knownCount: Option[FileRecordCount] = None): Item = {
    val attributes = Files.readAttributes(path, classOf[java.nio.file.attribute.BasicFileAttributes])
    item.copy(lastModifiedTimeMs=Some(attributes.lastModifiedTime().toMillis),
      totalSizeBytes=Some(attributes.size()), recordCount=knownCount.filter(_.matches(attributes)).map(_.value)
        .orElse(Formats.detect(path,options).cheapCount(path,options)))
  }
  def folder(path: String): Item = Item(path, "", None, "folder")
}
