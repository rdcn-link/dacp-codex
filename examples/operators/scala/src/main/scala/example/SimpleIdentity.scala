package example

import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.BytesOperator

/** The SDK owns protocol, input discovery and stream cleanup. */
object SimpleIdentity extends BytesOperator {
  override def transform(chunk:Array[Byte],params:JsonNode):Array[Byte]=chunk
}
