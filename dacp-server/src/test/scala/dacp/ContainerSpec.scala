package dacp

import java.nio.file.{Files,Path}
import java.io.{ByteArrayOutputStream,OutputStream}
import java.util.zip.GZIPOutputStream
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry,TarArchiveOutputStream,TarConstants}
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._
import dacp.protocol.Metadata

class ContainerSpec extends AnyFunSuite {
  private val csv = "id,name\n1,one\n2,two\n".getBytes("UTF-8")
  private def gzip(path: Path, bytes: Array[Byte]): Unit = {
    val out = new GZIPOutputStream(Files.newOutputStream(path))
    try out.write(bytes) finally out.close()
  }
  private def tar(entries: Seq[(String,Array[Byte])]): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new TarArchiveOutputStream(bytes)
    try entries.foreach { case (name,data) =>
      val e = new TarArchiveEntry(name); e.setSize(if (e.isDirectory) 0 else data.length); e.setModTime(0L)
      out.putArchiveEntry(e); if (!e.isDirectory) out.write(data); out.closeArchiveEntry()
    } finally out.close()
    bytes.toByteArray
  }
  private def config(dir: Path, file: Path, alias: String = "/archive"): Config =
    Config("localhost",8815,"localhost",Map("public" -> DatasetDefinition.custom(Seq(PublishedItem(alias,file=Some(file))))),
      Seq("dacp://localhost:8815/public"),dir,dir,dir)

  for ((suffix,format) <- Seq("tar" -> "tar","tar.gz" -> "tar-gzip","TGZ" -> "tar-gzip")) {
    test(s"$suffix exposes normalized TAR hierarchy, records, bytes, counts and safe folder properties") {
      val dir = Files.createTempDirectory("tar-test-")
      try {
        val file = dir.resolve("private."+suffix)
        val bytes = tar(Seq("./" -> Array.emptyByteArray,"./reports/orders.csv" -> csv,"empty/" -> Array.emptyByteArray,
          "nested.tar" -> tar(Seq.empty)))
        if (format == "tar") Files.write(file,bytes) else gzip(file,bytes)
        assert(Formats.detect(file).id == format)
        val c = config(dir,file); val ns = c.openDataset("public").asInstanceOf[NamespaceAdapter]
        val root = ns.list("/").head
        assert(root.kind == "folder" && root.format == format && NamespaceAdapter.views(root) == Seq("bytes"))
        assert(root.totalSizeBytes.isEmpty && root.recordCount.isEmpty && root.lastModifiedTimeMs.isEmpty)
        assert(ns.list("/archive").map(_.path) == Seq("/archive/empty","/archive/nested.tar","/archive/reports"))
        assert(ns.list("/archive/reports").head.format == "csv")
        assert(ns.describe("/archive/nested.tar").format == "bin")
        assert(ns.count("/archive/reports/orders.csv").contains(2L) && ns.count("/archive").isEmpty)
        ns.withResolved("/archive/reports/orders.csv") { (a,name) =>
          assert(Files.readAllBytes(a.asInstanceOf[DirectoryAdapter].resolve(name)).sameElements(csv))
        }
      } finally Data.deleteTree(dir)
    }
  }
  test("GZIP names its single child from the public alias, supports concatenated members, and leaves listing size unknown") {
    val dir = Files.createTempDirectory("gzip-test-")
    try {
      val file = dir.resolve("private-storage.gz"); gzip(file,csv)
      val c = config(dir,file,"/orders.csv.GZ"); val ns = c.openDataset("public").asInstanceOf[NamespaceAdapter]
      val child = ns.list("/orders.csv.GZ").head
      assert(child.path == "/orders.csv.GZ/orders.csv" && child.format == "csv")
      assert(child.totalSizeBytes.isEmpty && child.lastModifiedTimeMs.isEmpty && child.recordCount.isEmpty)
      assert(ns.count(child.path).contains(2L))
      assert(!ns.list().toString.contains("private-storage"))
      val opaque = new NamespaceAdapter(Seq(PublishedItem("/compressed",file=Some(file))),c)
      assert(opaque.list("/compressed").head.path == "/compressed/content")
      val second = dir.resolve("second.gz"); gzip(second,"extra\n".getBytes("UTF-8"))
      Files.write(file,Files.readAllBytes(file) ++ Files.readAllBytes(second))
      Containers.byId("gzip").withFile(file,"/orders.csv.gz","orders.csv",() => ()) { p =>
        assert(new String(Files.readAllBytes(p),"UTF-8").endsWith("extra\n"))
      }
    } finally Data.deleteTree(dir)
  }
  test("TAR rejects traversal, absolute paths, duplicates, file parents, links and oversized declarations") {
    val dir = Files.createTempDirectory("tar-invalid-")
    try {
      val file = dir.resolve("private.tar"); val reader = Containers.byId("tar")
      for (entries <- Seq(Seq("../escape" -> csv),Seq("a//b" -> csv),Seq("a" -> csv,"a/b" -> csv),Seq("a" -> csv,"a" -> csv))) {
        Files.write(file,tar(entries)); intercept[IllegalArgumentException](reader.index(file,"/archive",() => ()))
      }
      // Preserve an absolute name in a raw header (the writer normalizes absolute paths).
      for ((name,flag,size) <- Seq(("/absolute",TarConstants.LF_NORMAL,0L),("link",TarConstants.LF_SYMLINK,0L),
        ("hardlink",TarConstants.LF_LINK,0L),("huge",TarConstants.LF_NORMAL,Containers.MaxEntryBytes+1))) {
        val e = new TarArchiveEntry(name,flag,true); e.setSize(size); e.setLinkName("/private/target")
        val header = new Array[Byte](512); e.writeEntryHeader(header)
        Files.write(file,header ++ new Array[Byte](1024))
        intercept[IllegalArgumentException](reader.index(file,"/archive",() => ()))
      }
      Files.writeString(file,"invalid tar"); intercept[Exception](reader.index(file,"/archive",() => ()))
    } finally Data.deleteTree(dir)
  }
  test("TAR and GZIP staging clean up on failures and honor cancellation; corrupt gzip fails safely") {
    val dir = Files.createTempDirectory("container-cleanup-")
    try {
      for (format <- Seq("tar","gzip","tar-gzip")) {
        val file = dir.resolve("private."+format)
        if (format == "tar") Files.write(file,tar(Seq("orders.csv" -> csv)))
        else gzip(file,if (format == "gzip") csv else tar(Seq("orders.csv" -> csv)))
        val reader = Containers.byId(format)
        var staged: Path = null
        intercept[IllegalStateException] {
          reader.withFile(file,"/orders.csv.gz","orders.csv",() => ()) { p => staged=p; throw new IllegalStateException("stop") }
        }
        assert(staged != null && !Files.exists(staged))
        intercept[java.util.concurrent.CancellationException] {
          reader.withFile(file,"/orders.csv.gz","orders.csv",() => throw new java.util.concurrent.CancellationException())(_ => fail("cancel"))
        }
      }
      val file = dir.resolve("bad.gz"); gzip(file,csv)
      val bytes = Files.readAllBytes(file); bytes(bytes.length-8) = (bytes(bytes.length-8) ^ 1).toByte; Files.write(file,bytes)
      val error = intercept[FormatFailure] { Containers.byId("gzip").withFile(file,"/orders.csv.gz","orders.csv",() => ())(_ => ()) }
      assert(!error.getMessage.contains(file.toString))
    } finally Data.deleteTree(dir)
  }
  test("Flight serves original container bytes, entry records, and rejects folder records") {
    val dir = Files.createTempDirectory("container-flight-"); val allocator = new RootAllocator()
    try {
      val file = dir.resolve("private.tar.gz"); gzip(file,tar(Seq("reports/orders.csv" -> csv)))
      val gz = dir.resolve("private.csv.gz"); gzip(gz,csv)
      val c = config(dir,file).copy(datasets=Map("public" -> DatasetDefinition.custom(Seq(PublishedItem("/archive",file=Some(file)),PublishedItem("/orders.csv.gz",file=Some(gz))))))
      val server = FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(c,allocator)).build().start()
      val client = FlightClient.builder(allocator,server.getLocation).build()
      try {
        for ((alias,physical) <- Seq("/archive" -> file,"/orders.csv.gz" -> gz)) {
          val info = client.getInfo(FlightDescriptor.path("datasets","public",alias))
          val metadata = Metadata.Item.parseFrom(info.getAppMetadata)
          assert(metadata.getKind == "folder" && metadata.getViewsList.asScala == Seq("bytes"))
          val out = new ByteArrayOutputStream(); val stream = client.getStream(info.getEndpoints.get(0).getTicket)
          try while (stream.next()) {
            val root = stream.getRoot
            (0 until root.getRowCount).foreach(i => out.write(root.getVector(0).asInstanceOf[VarBinaryVector].get(i)))
          } finally stream.close()
          assert(out.toByteArray.sameElements(Files.readAllBytes(physical)))
          val bad = Metadata.Ticket.parseFrom(info.getEndpoints.get(0).getTicket.getBytes).toBuilder.setView("records").build()
          val invalid = client.getStream(new Ticket(bad.toByteArray))
          try intercept[FlightRuntimeException](invalid.next()) finally invalid.close()
        }
        for (path <- Seq("/archive/reports/orders.csv","/orders.csv.gz/orders.csv")) {
          val info = client.getInfo(FlightDescriptor.path("datasets","public",path))
          val stream = client.getStream(info.getEndpoints.get(0).getTicket)
          try { var n=0; while(stream.next()) n += stream.getRoot.getRowCount; assert(n==2) } finally stream.close()
        }
      } finally { client.close(); server.close() }
    } finally { allocator.close(); Data.deleteTree(dir) }
  }
}
