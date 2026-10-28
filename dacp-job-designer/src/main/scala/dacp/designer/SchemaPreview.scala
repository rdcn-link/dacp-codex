package dacp.designer

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import dacp.union.Json
import java.util.Base64
import java.nio.ByteBuffer
import org.apache.arrow.vector.types.pojo.Schema
import scala.jdk.CollectionConverters._

/** Preserve wire descriptors and add readable Arrow schema JSON for the browser. */
object SchemaPreview {
  def present(report:JsonNode):JsonNode={
    val out=report.deepCopy[ObjectNode]()
    def ports(n:JsonNode):Unit=n.elements.asScala.foreach{d=>
      if(d.path("schema").isTextual){
        val schema=Schema.deserializeMessage(ByteBuffer.wrap(Base64.getDecoder.decode(d.path("schema").asText)))
        d.asInstanceOf[ObjectNode].set[JsonNode]("schema_json",Json.mapper.readTree(schema.toJson))
      }
    }
    ports(out.path("outputs"));out.path("tasks").elements.asScala.foreach(t=>ports(t.path("outputs")));out
  }
}
