package dacp

import java.nio.file.{Files,Path,StandardOpenOption}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ArrowStreamWriter}

// A byte-preserving operator allows downstream tasks to consume any upstream format.
// Reading Arrow EOS (rather than waiting for OS EOF) is essential for the single-input FIFO.
object Identity {
  def run(input: Path, output: Path, allocator: BufferAllocator): Unit = {
    Files.createDirectories(output)
    val manifest = RuntimeJson.mapper.readTree(Files.readAllBytes(input.resolve("manifest.json")))
    val result = RuntimeJson.mapper.createArrayNode()
    manifest.elements.asScala.zipWithIndex.foreach { case (entry,index) =>
      val format = entry.get("format").asText()
      require(format.matches("[a-z][a-z0-9_-]*"),"unsupported input format")
      val extension = format match { case "excel" => "xlsx"; case "sql" => "bin"; case other => other }
      val name = s"_stream-$index.arrow"
      val reader = new ArrowStreamReader(Files.newInputStream(input.resolve(entry.get("file").asText())),allocator)
      try {
        require(reader.getVectorSchemaRoot.getSchema == PluginSchemas.bytes,"identity expects Arrow binary streams")
        val writer = new ArrowStreamWriter(reader.getVectorSchemaRoot,null,Files.newOutputStream(output.resolve(name),StandardOpenOption.CREATE_NEW))
        try { writer.start();while(reader.loadNextBatch()) writer.writeBatch();writer.end() }
        finally writer.close()
      } finally reader.close()
      val item = result.addObject();item.put("file",name);item.put("path",s"part-$index.$extension");item.put("format",format)
    }
    Files.write(output.resolve("_output.json"),RuntimeJson.mapper.writeValueAsBytes(result))
  }
}
