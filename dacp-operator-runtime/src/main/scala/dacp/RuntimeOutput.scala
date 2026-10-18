package dacp
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VarBinaryVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

object RuntimeOutput {
  def encode(dir: Path, allocator: BufferAllocator, format: String): Unit = {
    val files = Files.list(dir)
    val paths = try files.iterator.asScala.filter(p => Files.isRegularFile(p) && p.getFileName.toString != "_schema.json").toVector.sortBy(_.toString) finally files.close()
    val manifest = RuntimeJson.mapper.createArrayNode()
    paths.zipWithIndex.foreach { case (p,i) =>
      val name = s"_stream-$i.arrow"
      val entry = manifest.addObject();entry.put("file",name);entry.put("path",p.getFileName.toString);entry.put("format",format)
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
    Files.write(dir.resolve("_output.json"),RuntimeJson.mapper.writeValueAsBytes(manifest))
  }
}
