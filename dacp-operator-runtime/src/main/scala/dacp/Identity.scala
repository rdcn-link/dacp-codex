package dacp

import java.nio.file.{Files,Path}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ArrowStreamWriter}

/** A single named data item enters and leaves as an incremental stream. */
object Identity {
  def run(input: Path, allocator: BufferAllocator): Unit = {
    val manifest=RuntimeJson.mapper.readTree(Files.readAllBytes(input.resolve("manifest.json")))
    require(manifest.size()==1 && manifest.get(0).path("port").asText=="source","identity requires source")
    val entry=manifest.get(0)
    val reader=new ArrowStreamReader(new java.io.FileInputStream(input.resolve(entry.path("file").asText).toFile),allocator)
    try {
      val root=reader.getVectorSchemaRoot
      val repr=entry.path("representation").asText
      require(Set("bytes","records")(repr),"unsupported representation")
      if(repr=="bytes")require(root.getSchema==PluginSchemas.bytes,"invalid bytes schema")
      val header=RuntimeJson.mapper.createObjectNode().put("type","start").put("port","result")
        .put("representation",repr).put("format",entry.path("format").asText)
      if(repr=="records")header.put("schema",java.util.Base64.getEncoder.encodeToString(root.getSchema.serializeAsMessage()))
      OperatorFrames.write(System.out,OperatorFrame(RuntimeJson.mapper.writeValueAsBytes(header)))
      while(reader.loadNextBatch()) {
        if(repr=="bytes") (0 until root.getRowCount).foreach{i=>
          root.getVector(0).asInstanceOf[VarBinaryVector].get(i).grouped(65536).foreach(StreamOutput.data)
        } else {
          val out=new java.io.ByteArrayOutputStream()
          val writer=new ArrowStreamWriter(root,null,out)
          try {writer.start();writer.writeBatch();writer.end();StreamOutput.data(out.toByteArray)} finally writer.close()
        }
      }
      StreamOutput.event("end")
    } finally reader.close()
  }
}
object StreamOutput {
  def event(kind:String, data:Array[Byte]=Array.emptyByteArray):Unit =
    OperatorFrames.write(System.out,OperatorFrame(RuntimeJson.mapper.createObjectNode().put("type",kind).put("port","result").toString.getBytes(java.nio.charset.StandardCharsets.UTF_8),data))
  def data(bytes:Array[Byte]):Unit=event("data",bytes)
}
