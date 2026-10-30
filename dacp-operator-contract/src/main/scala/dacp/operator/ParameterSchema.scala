package dacp.operator


import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Static, language-neutral operator parameter contract. No worker or operator code is run. */
object ParameterSchema {
  private val types=Set("string","integer","number","boolean","null","object","array")
  private val common=Set("type","title","description","default","enum")
  private def declared(s:JsonNode):Set[String]=if(s.path("type").isArray)s.path("type").elements.asScala.map(_.asText).toSet else Set(s.path("type").asText)
  private def check(ok:Boolean,path:String,message:String):Unit=require(ok,s"$path: $message")
  def validate(schema:JsonNode):Unit={
    check(schema!=null && schema.isObject && schema.path("type").asText=="object","parameters","must be an object schema")
    definition(schema,"parameters",0)
  }
  private def definition(s:JsonNode,path:String,depth:Int):Unit={
    check(depth<=16,path,"schema nesting exceeds 16")
    check(s!=null && s.isObject,path,"definition must be an object")
    val ts=declared(s)
    check(ts.nonEmpty && ts.subsetOf(types) && (!s.path("type").isArray || ts.size==s.path("type").size),path,"unsupported, duplicate or missing type")
    val extra=ts.flatMap {
      case t=>t match {
      case "object"=>Set("properties","required","additionalProperties")
      case "array"=>Set("items","minItems","maxItems")
      case "string"=>Set("minLength","maxLength")
      case "integer"|"number"=>Set("minimum","maximum")
      case _=>Set.empty[String]
    }}
    check(s.fieldNames.asScala.toSet.subsetOf(common++extra),path,"unsupported schema keyword")
    Seq("title","description").foreach(k=>check(!s.has(k)||s.path(k).isTextual,path,s"$k must be text"))
    if(ts("object")){
      val p=s.path("properties");check(p.isObject,path,"properties must be an object")
      check(s.has("additionalProperties") && (s.path("additionalProperties").isBoolean || s.path("additionalProperties").isObject),path,"additionalProperties must explicitly be boolean or a schema")
      if(s.path("additionalProperties").isObject)definition(s.path("additionalProperties"),path+".*",depth+1)
      p.fields.asScala.foreach{e=>check(e.getKey.nonEmpty && !e.getKey.exists(_.isControl),path,"invalid property name");definition(e.getValue,s"$path.${e.getKey}",depth+1)}
      val r=s.path("required");check(r.isArray && r.elements.asScala.forall(n=>n.isTextual && p.has(n.asText)),path,"required must list declared properties")
      check(r.elements.asScala.map(_.asText).toSeq.distinct.size==r.size,path,"duplicate required property")
    }
    if(ts("array"))definition(s.path("items"),s"$path.items",depth+1)
    val pairs= ts.toSeq.flatMap {
      case t=>t match {
      case "string"=>Seq("minLength"->"maxLength")
      case "array"=>Seq("minItems"->"maxItems")
      case "integer"|"number"=>Seq("minimum"->"maximum")
      case _=>Seq.empty
    }}
    pairs.foreach{case(lo,hi)=>
      Seq(lo,hi).filter(s.has).foreach{k=>val n=s.path(k);check(n.isNumber && java.lang.Double.isFinite(n.asDouble),path,s"$k must be finite");if(k!="minimum"&&k!="maximum")check(n.isIntegralNumber && n.asLong>=0,path,s"$k must be a nonnegative integer")}
      if(s.has(lo)&&s.has(hi))check(s.path(lo).decimalValue.compareTo(s.path(hi).decimalValue)<=0,path,s"$lo exceeds $hi")
    }
    if(s.has("enum")){
      val e=s.path("enum");check(e.isArray && e.size>0,path,"enum must be a nonempty array")
      check(e.elements.asScala.toSeq.distinct.size==e.size,path,"duplicate enum value")
      e.elements.asScala.foreach(v=>value(s,v,path,checkEnum=false))
    }
    if(s.has("default"))value(s,s.path("default"),s"$path.default")
  }
  /** Validates literal metadata defaults/enums; runtime job submission is deliberately unchanged. */
  private def value(s:JsonNode,v:JsonNode,path:String,checkEnum:Boolean=true):Unit={
    val ts=declared(s)
    def fits(t:String):Boolean=t match{
      case "string"=>v.isTextual
      case "integer"=>v.isIntegralNumber
      case "number"=>v.isNumber && java.lang.Double.isFinite(v.asDouble)
      case "boolean"=>v.isBoolean
      case "null"=>v.isNull
      case "object"=>v.isObject
      case "array"=>v.isArray
    }
    check(ts.exists(fits),path,s"value must have type ${ts.mkString("/")}")
    val t=ts.toSeq.sorted.find(fits).get
    if(checkEnum && s.has("enum"))check(s.path("enum").elements.asScala.exists(_==v),path,"value is not in enum")
    if(t=="object"){
      s.path("required").elements.asScala.foreach(k=>check(v.has(k.asText),path,s"missing ${k.asText}"))
      v.fields.asScala.foreach{e=>val sub=s.path("properties").get(e.getKey);if(sub==null){val a=s.path("additionalProperties");if(a.isObject)value(a,e.getValue,s"$path.${e.getKey}")else check(a.asBoolean,path,s"unknown ${e.getKey}")}else value(sub,e.getValue,s"$path.${e.getKey}")}
    }
    if(t=="array")v.elements.asScala.foreach(n=>value(s.path("items"),n,path+"[]"))
    val measure=if(t=="string")BigDecimal(v.asText.codePointCount(0,v.asText.length))else if(t=="array")BigDecimal(v.size)else if(t=="number"||t=="integer")BigDecimal(v.decimalValue)else BigDecimal(0)
    val bounds=t match{case "string"=>Seq("minLength"->true,"maxLength"->false);case "array"=>Seq("minItems"->true,"maxItems"->false);case "number"|"integer"=>Seq("minimum"->true,"maximum"->false);case _=>Seq.empty}
    bounds.filter(x=>s.has(x._1)).foreach{case(k,min)=>val b=BigDecimal(s.path(k).decimalValue);check(if(min)measure>=b else measure<=b,path,s"violates $k")}
  }
}
