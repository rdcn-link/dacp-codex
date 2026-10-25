package dacp

import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.RootAllocator

private[dacp] object RuntimeJson {
  val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
}
object OperatorMain {
  def inferOutputs(inputs:Map[String,StreamDescriptor],params:com.fasterxml.jackson.databind.JsonNode):InferenceResult={
    require(inputs.keySet==Set("source"),"expected source input")
    sys.env.getOrElse("DACP_OPERATOR",throw new IllegalArgumentException("DACP_OPERATOR is required")).takeWhile(_!='@') match {
      case "identity"=>InferenceResult(Map("result"->inputs("source")))
      case "format_convert"=>
        require(inputs("source").representations.isEmpty || inputs("source").representations==Set("bytes"),"format_convert requires bytes")
        require(params.path("to").asText=="parquet","format_convert output must be parquet")
        require(Set("csv","json","jsonl","ndjson")(params.path("from").asText),"unsupported input format")
        InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set("parquet"),certainty="known")))
      case _=>throw new IllegalArgumentException("unknown operator")
    }
  }
  def main(args: Array[String]): Unit = {
    if(args.headOption.contains("infer")) {
      dacp.operator.OperatorRunner.infer(args)(inferOutputs);return
    }
    require(args.length == 3 && args(0) == "operator", "expected operator INPUT PARAMS")
    val allocator = new RootAllocator(Long.MaxValue)
    try sys.env.getOrElse("DACP_OPERATOR", throw new IllegalArgumentException("DACP_OPERATOR is required")).takeWhile(_ != '@') match {
      case "format_convert" =>
        val input=Path.of(args(1))
        val manifest=RuntimeJson.mapper.readTree(input.resolve("manifest.json").toFile)
        require(manifest.size()==1 && manifest.get(0).path("port").asText=="source" && manifest.get(0).path("representation").asText=="bytes","format_convert requires source bytes")
        // Parquet needs a completed footer; scratch is private, ephemeral container storage.
        val scratch=Files.createTempDirectory("convert-")
        try {
          FormatConvert.run(input,scratch,Path.of(args(2)))
          val header=RuntimeJson.mapper.createObjectNode().put("type","start").put("port","result").put("representation","bytes").put("format","parquet")
          OperatorFrames.write(System.out,OperatorFrame(RuntimeJson.mapper.writeValueAsBytes(header)))
          val in=Files.newInputStream(scratch.resolve("part-0.parquet"))
          try {val buffer=new Array[Byte](65536);var n=in.read(buffer);while(n>=0){if(n>0)StreamOutput.data(buffer.take(n));n=in.read(buffer)}} finally in.close()
          StreamOutput.event("end")
        } finally {val files=Files.walk(scratch);try files.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally files.close()}
      case "identity" => Identity.run(Path.of(args(1)), allocator)
      case operator => throw new IllegalArgumentException(s"unknown operator: $operator")
    } finally allocator.close()
  }
}
