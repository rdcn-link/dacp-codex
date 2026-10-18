package dacp

import java.nio.file.Path
import org.apache.arrow.memory.RootAllocator

private[dacp] object RuntimeJson {
  val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
}
object OperatorMain {
  def main(args: Array[String]): Unit = {
    require(args.length == 4 && args(0) == "operator", "expected operator INPUT OUTPUT PARAMS")
    val allocator = new RootAllocator(Long.MaxValue)
    try sys.env.getOrElse("DACP_OPERATOR", throw new IllegalArgumentException("DACP_OPERATOR is required")) match {
      case "format_convert@1.1" =>
        FormatConvert.run(Path.of(args(1)), Path.of(args(2)), Path.of(args(3)))
        RuntimeOutput.encode(Path.of(args(2)), allocator, "parquet")
      case "identity@1.1" => Identity.run(Path.of(args(1)), Path.of(args(2)), allocator)
      case operator => throw new IllegalArgumentException(s"unknown operator: $operator")
    } finally allocator.close()
  }
}
