package dacp

import java.nio.file.{Files, Path, FileVisitResult, SimpleFileVisitor, LinkOption}
import java.nio.file.attribute.BasicFileAttributes
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

object DatasetProviders {
  private lazy val providers: Seq[DatasetProvider] = {
    val all = Seq(new CustomDatasetProvider,new DirectoryDatasetProvider,new SqlDatasetProvider) ++
      ServiceLoader.load(classOf[DatasetProvider],PluginLoader.classLoader).iterator().asScala.toVector
    require(all.forall(_.id.matches("[a-z][a-z0-9-]*")),"invalid dataset provider id")
    require(all.forall(p => p.itemSettingsKey.matches("[a-z][a-z0-9_]*") &&
      !Set("provider","source").contains(p.itemSettingsKey)),"invalid dataset settings key")
    require(all.map(_.id).distinct.size == all.size,"duplicate dataset provider id")
    all
  }
  def byId(id: String): DatasetProvider = providers.find(_.id == id)
    .getOrElse(throw new IllegalArgumentException("unsupported dataset provider"))
  def fileOptions(v: Map[String,Object]): FileOptions = FileOptions(v.get("format").map(_.toString),
    v.get("schema").map { raw =>
      val fields = raw.asInstanceOf[java.util.List[java.util.Map[String,String]]].asScala
      new org.apache.arrow.vector.types.pojo.Schema(fields.map(c => Records.field(c.get("name"),c.get("type"))).asJava)
    }, v.get("options").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.toMap).getOrElse(Map.empty))
  def customBindings(d: DatasetDefinition): Seq[PublishedItem] = d.bindings.getOrElse {
    d.items.toVector.map { case (logical,v) =>
      require(v.keySet.subsetOf(Set("file","sql","table","kind","format","options","schema")),"unknown custom item option")
      PublishedItem(logical,v.get("file").map(x => Path.of(x.toString)),v.get("sql").map(_.toString),
        v.get("table").map(_.toString),v.get("kind").map(_.toString).getOrElse("data"),fileOptions(v))
    }
  }
  def publish(bindings: Seq[PublishedItem], config: Config): PublishedDataset = {
    val items = if (!config.countFileRecordsOnStartup) bindings else bindings.map { b =>
      b.file.map(p => b.copy(recordCount=FileRecordCounts.capture(p,b.options))).getOrElse(b)
    }
    new NamespaceAdapter(items,config) // Validate aliases and source references once at construction.
    new PublishedDataset {
      def open(check: () => Unit): DatasetAccess = new NamespaceAdapter(items,config,check)
    }
  }
}
class CustomDatasetProvider extends DatasetProvider {
  val id = "custom"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.source.isEmpty,"custom datasets do not accept source settings")
    config.publish(DatasetProviders.customBindings(d))
  }
}
class SqlDatasetProvider extends DatasetProvider {
  val id = "sqldb"
  override val itemSettingsKey = "tables"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.bindings.isEmpty && d.source.keySet == Set("connection"),"sqldb requires source.connection")
    val connection = d.source("connection").toString
    require(config.sqlSourceNames.contains(connection),"unknown private SQL source")
    val items = d.items.toVector.map { case (alias,v) =>
      require(v.keySet == Set("table"),"sqldb items require only a private table mapping")
      PublishedItem(alias,sql=Some(connection),table=Some(v("table").toString))
    }
    config.publish(items)
  }
}
class DirectoryDatasetProvider extends DatasetProvider {
  val id = "directory"
  override val itemSettingsKey = "reader_overrides"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.bindings.isEmpty && d.source.keySet == Set("root"),"directory requires source.root")
    val root = Path.of(d.source("root").toString).toRealPath()
    require(Files.isDirectory(root),"dataset root must be a directory")
    val overrides = d.items.map { case (path,v) =>
      LogicalPath.validate(path); require(path != "/","root cannot have reader options")
      require(v.nonEmpty && v.keySet.subsetOf(Set("format","options","schema")),"directory items accept reader options only")
      val options = DatasetProviders.fileOptions(v); options.format.foreach(id => Formats.byId(id).validateOptions(options))
      path -> options
    }
    new PublishedDataset {
      private def physical(logical: String): Path = {
        LogicalPath.validate(logical)
        require(root.toRealPath() == root,"dataset root changed")
        var p = root
        for (part <- logical.stripPrefix("/").split("/").filter(_.nonEmpty)) {
          p = p.resolve(part)
          if (Files.isSymbolicLink(p)) throw new java.io.FileNotFoundException("logical item not found")
        }
        p
      }
      private def binding(p: Path): Option[PublishedItem] = {
        if (p == root || Files.isSymbolicLink(p)) None else {
          val logical = "/" + root.relativize(p).iterator().asScala.mkString("/")
          if (scala.util.Try(LogicalPath.validate(logical)).isFailure) None
          else if (Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS)) Some(PublishedItem(logical,kind="folder"))
          else if (Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS) && p.toRealPath().startsWith(root))
            Some(PublishedItem(logical,file=Some(p),options=overrides.getOrElse(logical,FileOptions()),fileRoot=Some(root)))
          else None
        }
      }
      private def discover(check: () => Unit): Seq[PublishedItem] = {
        physical("/")
        val result = Vector.newBuilder[PublishedItem]
        Files.walkFileTree(root,new SimpleFileVisitor[Path] {
          override def preVisitDirectory(p: Path,a: BasicFileAttributes): FileVisitResult = {
            check(); binding(p).foreach(result += _); FileVisitResult.CONTINUE
          }
          override def visitFile(p: Path,a: BasicFileAttributes): FileVisitResult = {
            check(); binding(p).foreach(result += _); FileVisitResult.CONTINUE
          }
          override def visitFileFailed(p: Path,e: java.io.IOException): FileVisitResult = e match {
            case _: java.nio.file.NoSuchFileException => FileVisitResult.CONTINUE
            case _ => throw e
          }
        })
        result.result()
      }
      private val initialCounts = if (!config.countFileRecordsOnStartup) Map.empty[String,FileRecordCount] else
        discover(() => ()).flatMap(b => b.file.flatMap(p => FileRecordCounts.capture(p,b.options)).map(b.path -> _)).toMap
      private def counted(b: PublishedItem): PublishedItem = b.copy(recordCount=initialCounts.get(b.path))

      def open(check: () => Unit): DatasetAccess = new DatasetAccess {
        private def namespace(items: Seq[PublishedItem]): DatasetAccess = config.openNamespace(items.map(counted),check)
        // Resolve a physical file/folder or the first container encountered on the logical path.
        private def selected(path: String): DatasetAccess = {
          check(); LogicalPath.validate(path)
          if (path == "/") namespace(Seq.empty) else {
            var logical = ""
            val parts = path.drop(1).split("/")
            var selected: Option[PublishedItem] = None
            var i = 0
            while (i < parts.length && selected.isEmpty) {
              logical += "/" + parts(i)
              val p = physical(logical)
              if (Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS) || i == parts.length-1) selected = binding(p)
              if (selected.isEmpty && !Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS))
                throw new java.io.FileNotFoundException("logical item not found")
              i += 1
            }
            namespace(Seq(selected.getOrElse(throw new java.io.FileNotFoundException("logical item not found"))))
          }
        }
        def list(): Seq[Item] = namespace(discover(check)).list()
        def list(path: String): Seq[Item] = {
          val p = physical(path)
          if (Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS)) {
            check()
            val in = Files.newDirectoryStream(p)
            val children = try in.iterator().asScala.flatMap { child => check(); binding(child) }.toVector finally in.close()
            val folder = if (path == "/") Seq.empty else Seq(PublishedItem(path,kind="folder"))
            namespace(folder ++ children).list(path)
          } else selected(path).list(path)
        }
        def describe(path: String): Item = selected(path).describe(path)
        def count(path: String): Option[Long] = selected(path).count(path)
        override def scan(path: String, request: Scan, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit = {
          val access = selected(path)
          try access.scan(path,request,allocator,listener,ready) finally access.close()
        }
        def stream(path: String, view: String, allocator: BufferAllocator, listener: ServerStreamListener, ready: () => Unit): Unit =
          selected(path).stream(path,view,allocator,listener,ready)
      }
    }
  }
}
