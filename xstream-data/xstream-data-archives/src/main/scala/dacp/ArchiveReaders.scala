package dacp

import _root_.xstream.data._

import java.io.{FilterInputStream, InputStream, IOException}
import java.nio.file.{Files, Path}
import java.util.zip.GZIPInputStream
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

object ArchiveReaders {
  val MaxEntries = ZipArchive.MaxEntries
  val MaxEntryBytes = ZipArchive.MaxEntryBytes
  val MaxTotalBytes = ZipArchive.MaxTotalBytes
  // Bound decompressed TAR headers, padding and extension records as well as declared payloads.
  private val MaxTarBytes = MaxTotalBytes + 16L * 1024 * 1024

  private def checked[A](f: => A): A = try f catch {
    case _: IOException => throw new FormatFailure("Invalid or unsupported archive or compressed stream.")
  }
  private def copy(in: InputStream, out: java.io.OutputStream, max: Long, demand: () => Unit): Long = {
    val buffer = new Array[Byte](65536)
    var total = 0L; var done = false
    while (!done) {
      demand()
      val n = in.read(buffer)
      if (n < 0) done = true else {
        total = Math.addExact(total,n.toLong)
        require(total <= max, "decompressed size limit exceeded")
        out.write(buffer,0,n)
      }
    }
    total
  }
  private def stage[A](name: String)(f: Path => A): A = {
    val dir = Files.createTempDirectory("dacp-container-")
    try f(dir.resolve(Path.of(name).getFileName.toString)) finally Data.deleteTree(dir)
  }
  private def compressed(path: Path): InputStream = {
    val raw = Files.newInputStream(path)
    try new GZIPInputStream(raw,65536) catch { case t: Throwable => raw.close(); throw t }
  }
  private class Limited(in: InputStream, demand: () => Unit) extends FilterInputStream(in) {
    private var total = 0L
    def bytesRead: Long = total
    override def read(): Int = { val b = new Array[Byte](1); if (read(b,0,1) < 0) -1 else b(0) & 255 }
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      demand()
      val n = super.read(b,off,len)
      if (n > 0) { total += n; require(total <= MaxTarBytes,"TAR decompressed size limit exceeded") }
      n
    }
    override def skip(n: Long): Long = {
      val b = new Array[Byte](65536); var left = n; var done = false
      while (left > 0 && !done) {
        val k = read(b,0,math.min(left,b.length.toLong).toInt)
        if (k < 0) done = true else left -= k
      }
      n-left
    }
  }
  private def validate(entries: Seq[ContainerEntry]): Seq[ContainerEntry] = {
    require(entries.size <= MaxEntries,"archive entry limit exceeded")
    require(entries.map(_.name).distinct.size == entries.size,"duplicate archive entry")
    val files = entries.filterNot(_.directory).map(e => "/" + e.name).toSet
    entries.foreach { e =>
      LogicalPath.validate("/" + e.name)
      var parent = LogicalPath.parent("/" + e.name)
      while (parent != "/") {
        require(!files(parent),"archive file has children")
        parent = LogicalPath.parent(parent)
      }
    }
    entries
  }
  class ZipReader extends ContainerReader {
    val id = "zip"
    val extensions = Set("zip")
    def index(path: Path, publicName: String, demand: () => Unit): Seq[ContainerEntry] = checked {
      demand(); ZipArchive.index(path).map(e => ContainerEntry(e.name,e.directory,e.size,e.modified))
    }
    def withFile[A](path: Path, publicName: String, name: String, demand: () => Unit)(f: Path => A): A =
      checked { ZipArchive.withFile(path,name,demand)(f) }
  }
  class GzipReader extends ContainerReader {
    val id = "gzip"
    val extensions = Set("gz")
    private def child(publicName: String): String = {
      val leaf = publicName.split("/").last
      val name = if (leaf.toLowerCase(java.util.Locale.ROOT).endsWith(".gz")) leaf.dropRight(3) else "content"
      if (name.isEmpty || name == "." || name == "..") "content" else name
    }
    def index(path: Path, publicName: String, demand: () => Unit): Seq[ContainerEntry] = checked {
      demand()
      // Ignore the optional private filename in the GZIP header. Size and time stay unknown.
      val in = compressed(path); in.close()
      Seq(ContainerEntry(child(publicName),false,None,None))
    }
    def withFile[A](path: Path, publicName: String, name: String, demand: () => Unit)(f: Path => A): A = checked {
      require(name == child(publicName),"unknown compressed-stream child")
      stage(name) { file =>
        val in = compressed(path)
        try {
          val out = Files.newOutputStream(file)
          try copy(in,out,MaxEntryBytes,demand) finally out.close()
        } finally in.close()
        f(file)
      }
    }
  }
  class TarReader(val id: String, gzip: Boolean) extends ContainerReader {
    val extensions = if (gzip) Set("tgz") else Set("tar")
    override def matchesName(name: String): Boolean = if (gzip) {
      val lower = name.toLowerCase(java.util.Locale.ROOT)
      lower.endsWith(".tar.gz") || lower.endsWith(".tgz")
    } else super.matchesName(name)
    private def open(path: Path, demand: () => Unit): Limited =
      new Limited(if (gzip) compressed(path) else Files.newInputStream(path),demand)

    private def scan[A](path: Path, selected: Option[(String,Path)], demand: () => Unit)(f: Seq[ContainerEntry] => A): A = checked {
      val raw = open(path,demand)
      val in = new TarArchiveInputStream(raw)
      try {
        val result = Vector.newBuilder[ContainerEntry]
        var count = 0; var total = 0L; var found = false
        var entry = in.getNextEntry()
        while (entry != null) {
          count += 1; require(count <= MaxEntries,"archive entry limit exceeded")
          require(entry.isCheckSumOK && in.canReadEntryData(entry),"unsupported TAR entry")
          require(!entry.isSymbolicLink && !entry.isLink && !entry.isSparse && (entry.isDirectory || entry.isFile),
            "TAR links, sparse files and special entries are unsupported")
          val raw = if (entry.isDirectory) entry.getName.stripSuffix("/") else entry.getName
          // Standard tar tools often prefix every entry with './' or include a root directory record.
          val name = Iterator.iterate(raw)(_.stripPrefix("./")).dropWhile(_.startsWith("./")).next()
          val root = entry.isDirectory && (name.isEmpty || name == ".")
          if (!root) {
            LogicalPath.validate("/" + name)
            require(entry.getSize >= 0 && entry.getSize <= MaxEntryBytes,"archive entry size limit exceeded")
            total = Math.addExact(total,entry.getSize); require(total <= MaxTotalBytes,"archive total size limit exceeded")
            result += ContainerEntry(name,entry.isDirectory,Some(entry.getSize),Some(entry.getLastModifiedTime.toMillis))
            selected.filter(_._1 == name).foreach { case (_,file) =>
              require(!entry.isDirectory,"folder has no content stream")
              val out = Files.newOutputStream(file)
              try copy(in,out,MaxEntryBytes,demand) finally out.close()
              found = true
            }
          }
          // Drain through the bounded stream: compressed TAR requires sequential decompression.
          copy(in,java.io.OutputStream.nullOutputStream(),MaxEntryBytes,demand)
          entry = in.getNextEntry()
        }
        // Consume the trailer too, checking GZIP checksums and limiting trailing decompressed content.
        copy(raw,java.io.OutputStream.nullOutputStream(),MaxTarBytes,demand)
        require(raw.bytesRead >= 1024,"invalid TAR archive")
        val entries = validate(result.result())
        if (selected.nonEmpty && !found) throw new java.io.FileNotFoundException("archive entry not found")
        f(entries)
      } finally in.close()
    }
    def index(path: Path, publicName: String, demand: () => Unit): Seq[ContainerEntry] = scan(path,None,demand)(identity)
    def withFile[A](path: Path, publicName: String, name: String, demand: () => Unit)(f: Path => A): A =
      stage(name) { file => scan(path,Some(name -> file),demand)(_ => ()); f(file) }
  }
}

class ZipContainerReader extends ArchiveReaders.ZipReader
class GzipContainerReader extends ArchiveReaders.GzipReader
class TarContainerReader extends ArchiveReaders.TarReader("tar",false)
class TarGzipContainerReader extends ArchiveReaders.TarReader("tar-gzip",true)
