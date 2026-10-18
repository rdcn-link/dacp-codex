package example

import java.nio.file.{Files, Path, StandardOpenOption}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import scala.jdk.CollectionConverters._

/** Independently compiled operator: no dependency on DACP server classes. */
object ScalaIdentity {
  def main(args: Array[String]): Unit = {
    require(args.length == 4 && args(0) == "operator", "expected operator INPUT OUTPUT PARAMS")
    val input = Path.of(args(1)); val output = Path.of(args(2))
    val json = new ObjectMapper()
    val params = json.readTree(Path.of(args(3)).toFile)
    require(params.isObject && params.size() == 0, "scala_identity accepts no parameters")
    val manifest = json.readTree(input.resolve("manifest.json").toFile)
    val result = json.createArrayNode()
    val allocator = new RootAllocator(256L * 1024 * 1024)
    try manifest.elements.asScala.zipWithIndex.foreach { case (item, index) =>
      val format = item.path("format").asText
      require(format.matches("[a-z][a-z0-9_-]*"), "invalid input format")
      val filename = item.path("file").asText
      require(filename.nonEmpty && !filename.contains("/") && !filename.contains("\\"), "invalid input filename")
      val streamName = s"_stream-$index.arrow"
      val reader = new ArrowStreamReader(Files.newInputStream(input.resolve(filename)), allocator)
      try {
        val root = reader.getVectorSchemaRoot
        require(root.getSchema.getFields.size() == 1 && root.getVector(0).isInstanceOf[org.apache.arrow.vector.VarBinaryVector] &&
          root.getSchema.getFields.get(0).getName == "data", "expected Arrow bytes view")
        val writer = new ArrowStreamWriter(root, null,
          Files.newOutputStream(output.resolve(streamName), StandardOpenOption.CREATE_NEW))
        try {
          writer.start()
          // Process one bounded batch at a time. IPC EOS also works with DACP FIFO inputs.
          while (reader.loadNextBatch()) writer.writeBatch()
          writer.end()
        } finally writer.close()
      } finally reader.close()
      val extension = Map("excel" -> "xlsx", "sql" -> "bin").getOrElse(format, format)
      result.addObject().put("file", streamName).put("path", s"part-$index.$extension").put("format", format)
    } finally allocator.close()
    Files.write(output.resolve("_output.json"), json.writeValueAsBytes(result))
  }
}
