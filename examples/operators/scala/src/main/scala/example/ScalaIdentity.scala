package example


import java.io.{DataOutputStream, FileInputStream}
import java.nio.file.{Files, Paths}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.ipc.ArrowStreamReader

object ScalaIdentity {
  def main(args: Array[String]): Unit = {
    if(args.headOption.contains("infer")) {
      dacp.operator.OperatorRunner.infer(args,preview=(inputs,params,limits)=>inputs.get("source").flatMap(_.sample).map(s=>Map("result"->s.bounded(limits))).getOrElse(throw new IllegalArgumentException("Upstream sample unavailable")))((inputs,params)=>{
        require(inputs.keySet==Set("source") && params.isEmpty,"expected source and no params")
        dacp.InferenceResult(Map("result"->inputs("source")))
      });return
    }
    val mapper = new ObjectMapper()
    val input = Paths.get(args(1))
    val manifest = mapper.readTree(Files.readAllBytes(input.resolve("manifest.json")))
    require(manifest.size() == 1 && manifest.get(0).path("port").asText() == "source")
    require(manifest.get(0).path("representation").asText() == "bytes")
    val output = new DataOutputStream(System.out)
    def emit(kind: String, data: Array[Byte] = Array.emptyByteArray): Unit = {
      val header = mapper.createObjectNode().put("type", kind).put("port", "result")
      if (kind == "start") header.put("representation", "bytes").put("format", manifest.get(0).path("format").asText())
      val bytes = mapper.writeValueAsBytes(header)
      output.writeInt(bytes.length); output.writeInt(data.length)
      output.write(bytes); output.write(data); output.flush()
    }
    val allocator = new RootAllocator(64L * 1024 * 1024)
    val reader = new ArrowStreamReader(new FileInputStream(input.resolve(manifest.get(0).path("file").asText()).toFile), allocator)
    try {
      emit("start")
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val vector = root.getVector(0).asInstanceOf[VarBinaryVector]
        for (i <- 0 until root.getRowCount) {
          val bytes = vector.get(i)
          emit("data", bytes)
        }
      }
      emit("end")
    } finally { reader.close(); allocator.close() }
  }
}
