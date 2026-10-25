package dacp.operator

import dacp._
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.vector.types.pojo.Schema

class InferenceSpec extends AnyFunSuite {
  val mapper=StructureInference.mapper
  val bytes=StreamDescriptor(Set("bytes"),Set("text"),certainty="known")
  test("byte templates infer without executing transform") {
    val op=new BytesOperator {def transform(b:Array[Byte],p:JsonNode):Array[Byte]=fail("must not process data")}
    assert(op.inferOutputs(Map("source"->bytes),mapper.createObjectNode()).outputs("result").formats==Set("binary"))
    intercept[IllegalArgumentException](op.inferOutputs(Map.empty,mapper.createObjectNode()))
  }
  test("records template reuses schema transformation, unknown stays explicit") {
    val schema=Schema.fromJSON("""{"fields":[{"name":"value","nullable":true,"type":{"name":"int","bitWidth":64,"isSigned":true},"children":[]}]}""")
    val op=new RecordsOperator {def transform(b:org.apache.arrow.vector.VectorSchemaRoot,p:JsonNode)=fail("must not read records")}
    assert(op.inferOutputs(Map("source"->StreamDescriptor(Set("records"),schema=Some(schema),certainty="known")),mapper.createObjectNode()).outputs("result").schema.contains(schema))
    assert(op.inferOutputs(Map("source"->StreamDescriptor(Set("records"))),mapper.createObjectNode()).outputs("result").certainty=="unknown")
  }
  test("standalone inference supports empty output and preserves protocol stdout") {
    val file=Files.createTempFile("infer-",".json")
    try {
      Files.write(file,mapper.writeValueAsBytes(StructureInference.json(InferenceRequest(Map.empty,mapper.createObjectNode()))))
      val out=new java.io.ByteArrayOutputStream()
      OperatorRunner.infer(Array("infer",file.toString),out){(inputs,params)=>assert(inputs.isEmpty);println("business diagnostic");InferenceResult(Map.empty)}
      assert(StructureInference.result(mapper.readTree(out.toByteArray)).outputs.isEmpty)
    }finally Files.delete(file)
  }
  test("wire descriptors round trip schemas, reject malformed certainty and preserve domain structures") {
    val descriptor=StreamDescriptor(Set("array"),certainty="unknown",structure=Some(mapper.readTree("""{"dimensions":["time","latitude"]}""")))
    assert(StructureInference.descriptor(StructureInference.json(descriptor))==descriptor)
    intercept[IllegalArgumentException](StructureInference.descriptor(mapper.readTree("""{"representations":["records"],"formats":[],"certainty":"known"}""")))
    intercept[IllegalArgumentException](InferenceResult(Map.empty,Vector(InferenceDiagnostic("error","bad_field","unknown field"))).requireValid())
  }
}
