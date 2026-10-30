package dacp


import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Independent row and UTF-8 JSON size budgets for an optional authoring preview. */
final case class PreviewLimits(maxRows:Int=10,maxBytes:Int=16384) {
  require(maxRows>=1 && maxRows<=100,"preview max_rows must be between 1 and 100")
  require(maxBytes>=256 && maxBytes<=65536,"preview max_bytes must be between 256 and 65536")
}
final case class OutputSample(representation:String,format:String,rows:Vector[JsonNode]=Vector.empty,
  data:Vector[Byte]=Vector.empty,truncated:Boolean=false) {
  require(representation.matches("[a-z][a-z0-9_-]*"),"invalid sample representation")
  require(format.nonEmpty && format.length<=128,"invalid sample format")
  require(if(representation!="bytes")data.isEmpty && rows.forall(_.isObject)else rows.isEmpty,"invalid sample content")
  def bounded(limits:PreviewLimits):OutputSample={
    var result=copy(rows=rows.take(limits.maxRows),data=data.take(limits.maxBytes),
      truncated=truncated || rows.size>limits.maxRows || data.size>limits.maxBytes)
    while(OutputSamples.size(result)>limits.maxBytes && (result.rows.nonEmpty || result.data.nonEmpty)){
      result=if(result.representation!="bytes")result.copy(rows=result.rows.dropRight(1),truncated=true)
        else result.copy(data=result.data.take(math.max(0,result.data.size-(OutputSamples.size(result)-limits.maxBytes))),truncated=true)
    }
    require(OutputSamples.size(result)<=limits.maxBytes,"sample metadata exceeds byte budget")
    result
  }
}
object OutputSamples {
  def limits(n:JsonNode):PreviewLimits={
    require(n.isObject && n.fieldNames.asScala.toSet==Set("max_rows","max_bytes"),"invalid preview limits")
    require(n.path("max_rows").isIntegralNumber && n.path("max_bytes").isIntegralNumber,"preview limits must be integers")
    PreviewLimits(n.path("max_rows").asInt,n.path("max_bytes").asInt)
  }
  def json(limits:PreviewLimits):JsonNode=StructureInference.mapper.createObjectNode().put("max_rows",limits.maxRows).put("max_bytes",limits.maxBytes)
  def json(sample:OutputSample):JsonNode={
    val out=StructureInference.mapper.createObjectNode().put("representation",sample.representation).put("format",sample.format).put("truncated",sample.truncated)
    if(sample.representation!="bytes"){val rows=out.putArray("rows");sample.rows.foreach(rows.add)}
    else out.put("data_base64",java.util.Base64.getEncoder.encodeToString(sample.data.toArray))
    out
  }
  def size(sample:OutputSample):Int=StructureInference.mapper.writeValueAsBytes(json(sample)).length
  def read(n:JsonNode):OutputSample={
    val representation=n.path("representation").asText
    val content=if(representation!="bytes")"rows"else "data_base64"
    require(n.isObject && n.fieldNames.asScala.toSet==Set("representation","format","truncated",content),"invalid output sample fields")
    require(n.path("truncated").isBoolean && n.path("format").isTextual,"invalid output sample")
    val sample=if(representation!="bytes"){
      require(n.path("rows").isArray && n.path("rows").size<=100,"too many sample rows")
      OutputSample(representation,n.path("format").asText,n.path("rows").elements.asScala.toVector,truncated=n.path("truncated").asBoolean)
    }else{
      require(n.path("data_base64").isTextual,"invalid byte sample")
      OutputSample(representation,n.path("format").asText,data=java.util.Base64.getDecoder.decode(n.path("data_base64").asText).toVector,truncated=n.path("truncated").asBoolean)
    }
    require(size(sample)<=65536,"output sample exceeds 64 KiB");sample
  }
  /** Workers and hosts both enforce limits. A result's samples share a 64 KiB cap. */
  def attach(result:InferenceResult,samples:Map[String,OutputSample],limits:PreviewLimits):InferenceResult={
    require(samples.keySet.subsetOf(result.outputs.keySet),"sample refers to an undeclared output port")
    var budget=65536
    result.copy(outputs=result.outputs.toVector.sortBy(_._1).map{case(port,d)=>
      val sample=samples.get(port).filter(_=>budget>=256).map(_.bounded(limits.copy(maxBytes=math.min(limits.maxBytes,budget))))
      sample.foreach(s=>budget-=size(s));port->d.copy(sample=sample)
    }.toMap)
  }
}
