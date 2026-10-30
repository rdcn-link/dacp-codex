package dacp


import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import org.apache.arrow.vector.types.pojo.Schema
import scala.jdk.CollectionConverters._

/** Metadata only. Unknown structure is explicit; port names are always enumerated. */
final case class StreamDescriptor(representations:Set[String], formats:Set[String]=Set.empty,
  schema:Option[Schema]=None, certainty:String="unknown", structure:Option[JsonNode]=None,sample:Option[OutputSample]=None) {
  require(Set("known","unknown")(certainty),"invalid inference certainty")
  require(representations.forall(_.matches("[a-z][a-z0-9_-]*")),"invalid representation")
  require(certainty!="known" || representations.nonEmpty,"known descriptor needs a representation")
  require(schema.isEmpty || representations==Set("records"),"Arrow schema requires records representation")
  require(certainty!="known" || representations!=Set("records") || schema.nonEmpty,"known records need a schema")
  require(structure.forall(_.isObject),"structure must be an object")
  require(sample.forall(s=>representations(s.representation)),"sample representation must match output")
}
final case class InferenceDiagnostic(severity:String,code:String,message:String) {
  require(Set("error","warning")(severity) && code.nonEmpty && message.nonEmpty,"invalid inference diagnostic")
}
final case class InferenceRequest(inputs:Map[String,StreamDescriptor],params:JsonNode,preview:Option[PreviewLimits]=None) {
  require(params!=null && params.isObject,"inference params must be an object")
}
final case class InferenceResult(outputs:Map[String,StreamDescriptor],diagnostics:Vector[InferenceDiagnostic]=Vector.empty) {
  def requireValid():Unit=require(!diagnostics.exists(_.severity=="error"),diagnostics.filter(_.severity=="error").map(_.message).mkString("; "))
}
/** One JSON contract for SDKs, executor plugins and Flight. Schemas use base64 Arrow IPC schema messages, matching stream headers. */
object StructureInference {
  val mapper=new ObjectMapper()
  val MaxBytes=1024*1024
  def port(name:String):Unit=require(name.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"invalid inference port")
  private def fields(n:JsonNode,allowed:Set[String],required:Set[String]):Unit={
    require(n!=null && n.isObject,"inference value must be an object")
    val keys=n.fieldNames.asScala.toSet
    require(keys.subsetOf(allowed) && required.subsetOf(keys),"invalid inference fields")
  }
  def descriptor(n:JsonNode):StreamDescriptor={
    fields(n,Set("representations","formats","schema","certainty","structure","sample"),Set("representations","formats","certainty"))
    def strings(key:String):Set[String]={val v=n.path(key);require(v.isArray && v.elements.asScala.forall(_.isTextual),s"$key must be a string array");val xs=v.elements.asScala.map(_.asText).toVector;require(xs.distinct.size==xs.size,s"duplicate $key");xs.toSet}
    StreamDescriptor(strings("representations"),strings("formats"),Option(n.get("schema")).filterNot(_.isNull).map { v=>
      require(v.isTextual,"schema must be base64 Arrow IPC")
      val bytes=java.util.Base64.getDecoder.decode(v.asText)
      org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema(new org.apache.arrow.vector.ipc.ReadChannel(java.nio.channels.Channels.newChannel(new java.io.ByteArrayInputStream(bytes))))
    },
      n.path("certainty").asText,Option(n.get("structure")).filterNot(_.isNull),Option(n.get("sample")).map(OutputSamples.read))
  }
  def json(d:StreamDescriptor):JsonNode={
    val n=mapper.createObjectNode().put("certainty",d.certainty)
    val r=n.putArray("representations");d.representations.toVector.sorted.foreach(r.add)
    val f=n.putArray("formats");d.formats.toVector.sorted.foreach(f.add)
    d.schema.foreach(s=>n.put("schema",java.util.Base64.getEncoder.encodeToString(s.serializeAsMessage())))
    d.structure.foreach(s=>n.set[JsonNode]("structure",s));d.sample.foreach(s=>n.set[JsonNode]("sample",OutputSamples.json(s)));n
  }
  def ports(n:JsonNode):Map[String,StreamDescriptor]={
    require(n!=null && n.isObject && n.size<=128,"inference ports must be a mapping of at most 128 ports")
    n.fields.asScala.map(e=>{port(e.getKey);e.getKey->descriptor(e.getValue)}).toMap
  }
  def portsJson(ps:Map[String,StreamDescriptor]):JsonNode={val n=mapper.createObjectNode();require(ps.size<=128,"too many inference ports");ps.toVector.sortBy(_._1).foreach{case(p,d)=>port(p);n.set[JsonNode](p,json(d))};n}
  def request(n:JsonNode):InferenceRequest={fields(n,Set("inputs","params","preview"),Set("inputs","params"));InferenceRequest(ports(n.path("inputs")),n.path("params"),Option(n.get("preview")).map(OutputSamples.limits))}
  def json(r:InferenceRequest):JsonNode={val n=mapper.createObjectNode();n.set[JsonNode]("inputs",portsJson(r.inputs));n.set[JsonNode]("params",r.params);r.preview.foreach(p=>n.set[JsonNode]("preview",OutputSamples.json(p)));n}
  def result(n:JsonNode):InferenceResult={
    fields(n,Set("outputs","diagnostics"),Set("outputs","diagnostics"))
    require(n.path("diagnostics").isArray && n.path("diagnostics").size<=128,"invalid inference diagnostics")
    val diagnostics=n.path("diagnostics").elements.asScala.map{d=>fields(d,Set("severity","code","message"),Set("severity","code","message"));InferenceDiagnostic(d.path("severity").asText,d.path("code").asText,d.path("message").asText)}.toVector
    InferenceResult(ports(n.path("outputs")),diagnostics)
  }
  def json(r:InferenceResult):JsonNode={
    val n=mapper.createObjectNode();n.set[JsonNode]("outputs",portsJson(r.outputs));val ds=n.putArray("diagnostics")
    r.diagnostics.foreach(d=>ds.addObject().put("severity",d.severity).put("code",d.code).put("message",d.message));n
  }
}
