package dacp

import _root_.xstream.data._

import java.nio.file.{Files, Path}
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters._

/** ZIP entries stay private; only validated logical names are published. */
object ZipArchive {
  val MaxEntries = 10000
  val MaxEntryBytes = 512L * 1024 * 1024
  val MaxTotalBytes = 2L * 1024 * 1024 * 1024
  case class Entry(name: String, directory: Boolean, size: Option[Long], modified: Option[Long])
  def index(path: Path): Seq[Entry] = {
    val zip = new ZipFile(path.toFile)
    try {
      val result = Vector.newBuilder[Entry]
      val names = scala.collection.mutable.Set.empty[String]
      var total = 0L; var count = 0
      val entries = zip.entries()
      while (entries.hasMoreElements) {
        val e = entries.nextElement(); count += 1
        require(count <= MaxEntries, "ZIP entry limit exceeded")
        val name = if (e.isDirectory) e.getName.stripSuffix("/") else e.getName
        LogicalPath.validate("/" + name)
        require(name.nonEmpty && names.add(name), "invalid or duplicate ZIP entry")
        require(e.getSize <= MaxEntryBytes, "ZIP entry size limit exceeded")
        total = Math.addExact(total, math.max(0L,e.getSize))
        require(total <= MaxTotalBytes, "ZIP total size limit exceeded")
        result += Entry(name,e.isDirectory,Option(e.getSize).filter(_ >= 0),Option(e.getTime).filter(_ >= 0))
      }
      val indexed = result.result()
      val files = indexed.filterNot(_.directory).map(e => "/" + e.name).toSet
      indexed.foreach(e => {
        var parent = LogicalPath.parent("/" + e.name)
        while (parent != "/") {
          require(!files(parent), "ZIP file has children")
          parent = LogicalPath.parent(parent)
        }
      })
      indexed
    } finally zip.close()
  }
  // Random-access readers (Excel and Parquet) need a seekable file. Stage only the requested entry.
  def withFile[A](archive: Path, name: String, demand: () => Unit)(f: Path => A): A = {
    index(archive)
    val zip = new ZipFile(archive.toFile)
    val temp = Files.createTempDirectory("dacp-zip-")
    try {
      val entry = Option(zip.getEntry(name)).getOrElse(throw new java.io.FileNotFoundException("ZIP entry not found"))
      require(!entry.isDirectory, "folder has no content stream")
      val file = temp.resolve(Path.of(name).getFileName.toString)
      val in = zip.getInputStream(entry)
      try {
        val out = Files.newOutputStream(file)
        try {
          val buffer = new Array[Byte](65536); var total = 0L; var done = false
          while (!done) {
            demand(); val n = in.read(buffer)
            if (n < 0) done = true else {
              total += n; require(total <= MaxEntryBytes, "ZIP entry size limit exceeded")
              out.write(buffer,0,n)
            }
          }
        } finally out.close()
      } finally in.close()
      f(file)
    } finally { Data.deleteTree(temp); zip.close() }
  }
}
