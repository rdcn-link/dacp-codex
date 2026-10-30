package dacp.operator

import _root_.xstream.data._

import dacp._
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.vector.types.pojo.Schema

class InferenceSpec extends AnyFunSuite {
  val mapper=StructureInference.mapper
  val bytes=StreamDescriptor(Set("bytes"),Set("text"),certainty="known")
  test("samples are opt-in, supplied by operators, and bounded by rows and UTF-8 bytes") {
    val limits=PreviewLimits(2,256)
    val sample=OutputSample("records","json",Vector.fill(5)(mapper.readTree("""{"label":"气象数据"}"""))).bounded(limits)
    assert(sample.rows.size<=2 && sample.truncated && OutputSamples.size(sample)<=256)
    assert(OutputSamples.read(OutputSamples.json(sample))==sample)
    val oversized=OutputSample("records","json",Vector(mapper.createObjectNode().put("large","气"*1000))).bounded(limits)
    assert(oversized.rows.isEmpty && oversized.truncated && OutputSamples.size(oversized)<=256)
    val binary=OutputSample("bytes","binary",data=Vector.fill(1000)(1.toByte)).bounded(limits)
    assert(binary.truncated && OutputSamples.size(binary)<=256)
    intercept[IllegalArgumentException](PreviewLimits(0,256))
    intercept[IllegalArgumentException](PreviewLimits(10,65537))
    var previews=0
    val file=Files.createTempFile("preview-",".json")
    val method:OperatorRunner.PreviewMethod=(_,_,limit)=>{previews+=1;assert(limit==limits);Map("result"->OutputSample("bytes","text",data=Vector.fill(1000)(1.toByte)))}
    try {
      Seq(None,Some(limits)).foreach { requested=>
        Files.write(file,mapper.writeValueAsBytes(StructureInference.json(InferenceRequest(Map.empty,mapper.createObjectNode(),requested))))
        val out=new java.io.ByteArrayOutputStream()
        OperatorRunner.infer(Array("infer",file.toString),out,method){(_,_)=>InferenceResult(Map("result"->bytes))}
        val result=StructureInference.result(mapper.readTree(out.toByteArray)).outputs("result")
        assert(result.sample.isDefined==requested.isDefined)
        result.sample.foreach(s=>assert(OutputSamples.size(s)<=256))
      }
      assert(previews==1)
    }finally Files.delete(file)
  }
  test("record view is preferred only when no explicit view was chosen") {
    assert(ItemViews.select("auto",Seq("bytes","records"))=="records")
    assert(ItemViews.select("auto",Seq("bytes"))=="bytes")
    assert(ItemViews.select("bytes",Seq("bytes","records"))=="bytes")
    intercept[IllegalArgumentException](ItemViews.select("records",Seq("bytes")))
  }
  test("byte templates infer without executing transform") {
    val op=new BytesOperator {def transform(b:Array[Byte],p:JsonNode):Array[Byte]=fail("must not process data")}
    assert(op.inferOutputs(Map("source"->bytes),mapper.createObjectNode()).outputs("result").formats==Set("binary"))
    intercept[IllegalArgumentException](op.inferOutputs(Map.empty,mapper.createObjectNode()))
  }
  test("records template reuses schema transformation, unknown stays explicit") {
    val schema=Schema.fromJSON("""{"fields":[{"name":"value","nullable":true,"type":{"name":"int","bitWidth":64,"isSigned":true},"children":[]}]}""")
    val op=new RecordsOperator {override def previewOutputs(i:Map[String,StreamDescriptor],p:JsonNode,l:PreviewLimits)=Map.empty[String,OutputSample];def transform(b:org.apache.arrow.vector.VectorSchemaRoot,p:JsonNode)=fail("must not read records")}
    assert(op.inferOutputs(Map("source"->StreamDescriptor(Set("records"),schema=Some(schema),certainty="known")),mapper.createObjectNode()).outputs("result").schema.contains(schema))
    assert(op.inferOutputs(Map("source"->StreamDescriptor(Set("records"))),mapper.createObjectNode()).outputs("result").certainty=="unknown")
  }
  test("standalone inference supports empty output and preserves protocol stdout") {
    val file=Files.createTempFile("infer-",".json")
    try {
      Files.write(file,mapper.writeValueAsBytes(StructureInference.json(InferenceRequest(Map.empty,mapper.createObjectNode()))))
      val out=new java.io.ByteArrayOutputStream()
      OperatorRunner.infer(Array("infer",file.toString),out,(_,_,_)=>Map.empty){(inputs,params)=>assert(inputs.isEmpty);println("business diagnostic");InferenceResult(Map.empty)}
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
    OperatorRunner.inferWorker(new java.io.DataInputStream(new java.io.ByteArrayInputStream(requests.toByteArray)),new java.io.DataOutputStream(output),(_,_,_)=>Map.empty){(inputs,params)=>
      calls+=1;InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set(params.path("format").asText),certainty="known")))
    }
    val reader=new java.io.DataInputStream(new java.io.ByteArrayInputStream(output.toByteArray));assert(reader.readInt()==0)
    Seq("json","text").foreach { format=>val raw=new Array[Byte](reader.readInt());reader.readFully(raw);assert(StructureInference.result(mapper.readTree(raw)).outputs("result").formats==Set(format)) }
    assert(calls==2 && reader.read()== -1)
    def invalid(raw:Array[Byte]):Unit=OperatorRunner.inferWorker(new java.io.DataInputStream(new java.io.ByteArrayInputStream(raw)),new java.io.DataOutputStream(new java.io.ByteArrayOutputStream()),(_,_,_)=>Map.empty){(_,_)=>fail("invalid frames must not invoke the operator")}
    intercept[java.io.EOFException](invalid(Array[Byte](0,0)))
    intercept[IllegalArgumentException](invalid(java.nio.ByteBuffer.allocate(4).putInt(StructureInference.MaxBytes+1).array()))
  }

}
