package dacp

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VarBinaryVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

// Architecture: both sides of the language-neutral container boundary use Arrow IPC streams.
// Parquet requires a footer, so conversion finishes the local file before exposing its binary Arrow stream.
object OperatorOutput {
  def encode(dir: Path, allocator: BufferAllocator): Unit = {
    val files = Files.list(dir)
    val paths = try files.iterator.asScala.filter(p => Files.isRegularFile(p) && p.getFileName.toString != "_schema.json").toVector.sortBy(_.toString) finally files.close()
    val manifest = Data.mapper.createArrayNode()
    paths.zipWithIndex.foreach { case (p,i) =>
      val name = s"_stream-$i.arrow"
      val entry = manifest.addObject();entry.put("file",name);entry.put("path",p.getFileName.toString);entry.put("format",Data.format(p))
      val root = VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
      val writer = new ArrowStreamWriter(root,null,Files.newOutputStream(dir.resolve(name),StandardOpenOption.CREATE_NEW))
      val input = Files.newInputStream(p)
      try {
        writer.start();val bytes=new Array[Byte](65536);var n=input.read(bytes)
        while(n >= 0) { root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,bytes,0,n);root.setRowCount(1);writer.writeBatch();n=input.read(bytes) }
        writer.end()
      } finally {input.close();writer.close();root.close()}
      Files.delete(p)
    }
    Files.write(dir.resolve("_output.json"),Data.mapper.writeValueAsBytes(manifest))
  }
  def decode(dir: Path, allocator: BufferAllocator): Unit = {
    val manifest = Data.mapper.readTree(Files.readAllBytes(dir.resolve("_output.json")))
    require(manifest.isArray && manifest.size > 0,"operator must return an Arrow output manifest")
    def safe(name: String): Path = {
      require(name.nonEmpty && !name.contains("/") && !name.contains("\\") && name != "." && name != "..","invalid operator output path")
      val p=dir.resolve(name); require(!Files.isSymbolicLink(p),"operator output symlinks forbidden");p
    }
    manifest.elements.asScala.foreach { entry =>
      val ipc=safe(entry.get("file").asText);val target=safe(entry.get("path").asText)
      require(!target.getFileName.toString.startsWith("_"),"reserved output path")
      val reader = new ArrowStreamReader(Files.newInputStream(ipc),allocator)
      val out = Files.newOutputStream(target,StandardOpenOption.CREATE_NEW)
      try {
        require(reader.getVectorSchemaRoot.getSchema == PluginSchemas.bytes,"operator output must be Arrow binary view")
        while(reader.loadNextBatch()) { val root=reader.getVectorSchemaRoot;val v=root.getVector(0).asInstanceOf[VarBinaryVector]
          (0 until root.getRowCount).foreach(i => out.write(v.get(i))) }
      } finally {out.close();reader.close()}
      Files.delete(ipc)
    }
    Files.delete(dir.resolve("_output.json"))
  }
}
