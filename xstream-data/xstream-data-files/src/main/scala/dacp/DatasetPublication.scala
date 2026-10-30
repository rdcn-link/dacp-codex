package dacp

import _root_.xstream.data._
import java.nio.file.{Files,Path,FileVisitResult,SimpleFileVisitor,LinkOption}
import java.nio.file.attribute.BasicFileAttributes
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import _root_.xstream.data.BatchReceiver

object DatasetPublication {
  def fileOptions(v: Map[String,Object]): FileOptions = FileOptions(v.get("format").map(_.toString),
    v.get("schema").map { raw =>
      val fields = raw.asInstanceOf[java.util.List[java.util.Map[String,String]]].asScala
      new org.apache.arrow.vector.types.pojo.Schema(fields.map(c => Records.field(c.get("name"),c.get("type"))).asJava)
    }, v.get("options").map(_.asInstanceOf[java.util.Map[String,Object]].asScala.toMap).getOrElse(Map.empty))
  def mixedBindings(d: DatasetDefinition): Seq[PublishedItem] = d.source.get("bindings").map {
    case values: java.util.List[_] => values.asScala.map {case item:PublishedItem=>item;case _=>throw new IllegalArgumentException("invalid published binding")}.toVector
    case _=>throw new IllegalArgumentException("bindings must be a list")
  }.getOrElse {
    d.items.toVector.map { case (logical,v) =>
      require(v.keySet.subsetOf(Set("file","sql","table","kind","format","options","schema")),"unknown mixed item option")
      PublishedItem(logical,v.get("file").map(x => Path.of(x.toString)),v.get("sql").map(_.toString),
        v.get("table").map(_.toString),v.get("kind").map(_.toString).getOrElse("data"),fileOptions(v))
    }
  }
  def publish(bindings: Seq[PublishedItem], config: DatasetContext): PublishedDataset = {
    val items = if (!config.countFileRecordsOnStartup) bindings else bindings.map { b =>
      b.file.map(p => b.copy(recordCount=FileRecordCounts.capture(p,b.options,config.plugins))).getOrElse(b)
    }
    new NamespaceAdapter(items,config) // Validate aliases and source references once at construction.
    new PublishedDataset {
      def open(check: () => Unit): DatasetAccess = new NamespaceAdapter(items,config,check)
    }
  }
}
class MixedDatasetProvider extends DatasetProvider {
  val id = "mixed"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.source.keySet.subsetOf(Set("bindings")),"unknown mixed source settings")
    DatasetPublication.publish(DatasetPublication.mixedBindings(d),config)
  }
}
class SqlDatasetProvider extends DatasetProvider {
  val id = "sqldb"
  override val itemSettingsKey = "tables"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.source.keySet == Set("connection"),"sqldb requires source.connection")
    val connection = d.source("connection").toString
    require(config.sources.get(connection).exists(_.provider == "jdbc"),"unknown private SQL source")
    val items = d.items.toVector.map { case (alias,v) =>
      require(v.keySet == Set("table"),"sqldb items require only a private table mapping")
      PublishedItem(alias,sql=Some(connection),table=Some(v("table").toString))
    }
    DatasetPublication.publish(items,config)
  }
}
class DirectoryDatasetProvider extends DatasetProvider {
  val id = "directory"
  override val itemSettingsKey = "reader_overrides"
  def create(d: DatasetDefinition, config: DatasetContext): PublishedDataset = {
    require(d.source.keySet == Set("root"),"directory requires source.root")
    val root = Path.of(d.source("root").toString).toRealPath()
    require(Files.isDirectory(root),"dataset root must be a directory")
    val overrides = d.items.map { case (path,v) =>
      LogicalPath.validate(path); require(path != "/","root cannot have reader options")
      require(v.nonEmpty && v.keySet.subsetOf(Set("format","options","schema")),"directory items accept reader options only")
      val options = DatasetPublication.fileOptions(v); options.format.foreach(id => config.plugins.formats.byId(id).validateOptions(options))
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
        discover(() => ()).flatMap(b => b.file.flatMap(p => FileRecordCounts.capture(p,b.options,config.plugins)).map(b.path -> _)).toMap
      private def counted(b: PublishedItem): PublishedItem = b.copy(recordCount=initialCounts.get(b.path))

      def open(check: () => Unit): DatasetAccess = new DatasetAccess {
        private def namespace(items: Seq[PublishedItem]): DatasetAccess = new NamespaceAdapter(items.map(counted),config,check)
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
        private def use[A](access: DatasetAccess)(f: DatasetAccess => A): A =
          try f(access) finally access.close()
        def list(): Seq[Item] = use(namespace(discover(check)))(_.list())
        def list(path: String): Seq[Item] = {
          val p = physical(path)
          if (Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS)) {
            check()
            val in = Files.newDirectoryStream(p)
            val children = try in.iterator().asScala.flatMap { child => check(); binding(child) }.toVector finally in.close()
            val folder = if (path == "/") Seq.empty else Seq(PublishedItem(path,kind="folder"))
            use(namespace(folder ++ children))(_.list(path))
          } else use(selected(path))(_.list(path))
        }
        def describe(path: String): Item = use(selected(path))(_.describe(path))
        def count(path: String): Option[Long] = use(selected(path))(_.count(path))
        def view[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]=Some(DataViews[K,S]((selection,context,out)=>
          use(selected(path)){a=>val stream=a.requireView(path,kind).open(selection,context);try stream.consume(out)finally stream.close()}))

      }
    }
  }
}
