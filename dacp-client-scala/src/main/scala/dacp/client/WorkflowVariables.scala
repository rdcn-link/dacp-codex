package dacp.client

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json
import org.yaml.snakeyaml.{LoaderOptions,Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import scala.jdk.CollectionConverters._

/** Resolve complete scalar values once; preserve JSON types and never execute code. */
object WorkflowVariables {
  private val variable = "\\$([A-Za-z_][A-Za-z0-9_]*)".r
  def resolve(text:String, variables:JsonNode):String = {
    if(variables==null)return text
    require(variables.isObject,"variables must be a mapping")
    require(variables.fieldNames().asScala.forall(_.matches("[A-Za-z_][A-Za-z0-9_]*")),"invalid workflow variable name")
    val document=dacp.union.YamlDocument.parse(text)
    def visit(value:JsonNode,depth:Int):JsonNode = {
      require(depth<=100,"workflow nesting exceeds 100 levels")
      if(value.isTextual)value.asText match {
        case variable(name)=>
          require(variables.has(name),s"missing workflow variable: $name")
          variables.get(name).deepCopy[JsonNode]()
        case _=>value
      }
      else if(value.isArray){val a=Json.mapper.createArrayNode();value.elements().asScala.foreach(v=>a.add(visit(v,depth+1)));a}
      else if(value.isObject){val o=Json.obj();value.fields().asScala.foreach(e=>o.set[JsonNode](e.getKey,visit(e.getValue,depth+1)));o}
      else value
    }
    dacp.union.YamlDocument.dump(visit(document,0))
  }
}
