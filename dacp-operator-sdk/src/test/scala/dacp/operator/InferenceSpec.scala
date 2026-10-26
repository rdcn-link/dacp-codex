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
  test("worker processes changing parameters in one instance and rejects truncated or oversized frames") {
    val requests=new java.io.ByteArrayOutputStream();val writer=new java.io.DataOutputStream(requests)
    Seq("json","text").foreach { format=>
      val request=StructureInference.mapper.writeValueAsBytes(StructureInference.json(InferenceRequest(Map.empty,mapper.createObjectNode().put("format",format))))
      writer.writeInt(request.length);writer.write(request)
    }
    var calls=0;val output=new java.io.ByteArrayOutputStream()
    OperatorRunner.inferWorker(new java.io.DataInputStream(new java.io.ByteArrayInputStream(requests.toByteArray)),new java.io.DataOutputStream(output)){(inputs,params)=>
      calls+=1;InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set(params.path("format").asText),certainty="known")))
    }
    val reader=new java.io.DataInputStream(new java.io.ByteArrayInputStream(output.toByteArray));assert(reader.readInt()==0)
    Seq("json","text").foreach { format=>val raw=new Array[Byte](reader.readInt());reader.readFully(raw);assert(StructureInference.result(mapper.readTree(raw)).outputs("result").formats==Set(format)) }
    assert(calls==2 && reader.read()== -1)
    def invalid(raw:Array[Byte]):Unit=OperatorRunner.inferWorker(new java.io.DataInputStream(new java.io.ByteArrayInputStream(raw)),new java.io.DataOutputStream(new java.io.ByteArrayOutputStream())){(_,_)=>fail("invalid frames must not invoke the operator")}
    intercept[java.io.EOFException](invalid(Array[Byte](0,0)))
    intercept[IllegalArgumentException](invalid(java.nio.ByteBuffer.allocate(4).putInt(StructureInference.MaxBytes+1).array()))
  }

}
