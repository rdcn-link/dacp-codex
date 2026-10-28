package dacp.operator

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

class ParameterSchemaSpec extends AnyFunSuite {
  private val mapper=new ObjectMapper()
  private def schema(fields:String)=mapper.readTree(s"""{"type":"object","properties":{$fields},"required":[],"additionalProperties":false}""")
  test("recursive parameter declarations include typed defaults, unions, enums, ranges and dynamic maps"){
    ParameterSchema.validate(schema(""""count":{"type":"integer","minimum":1,"maximum":100,"default":3},
      "mode":{"type":"string","enum":["fast","safe"],"default":"safe"},
      "columns":{"type":"array","items":{"type":"string"},"default":["temperature"]},
      "mapping":{"type":["string","object"],"properties":{},"required":[],"additionalProperties":{"type":"string"},"default":{"value":"float64"}}"""))
  }
  test("missing contracts, undeclared keywords, invalid types and contradictory bounds are rejected"){
    intercept[IllegalArgumentException](ParameterSchema.validate(null))
    for(body<-Seq(""""x":{"type":"int"}""",""""x":{"type":"number","minimum":3,"maximum":2}""",
      """"x":{"type":"string","pattern":".*"}""",""""x":{"type":"array"}""",
      """"x":{"type":"object","properties":{},"required":[]}"""))intercept[IllegalArgumentException](ParameterSchema.validate(schema(body)))
  }
  test("defaults and enum values must satisfy the full parameter contract"){
    for(body<-Seq(""""x":{"type":"integer","default":"3"}""",
      """"x":{"type":"integer","minimum":1,"default":0}""",
      """"x":{"type":"boolean","enum":[true],"default":false}""",
      """"x":{"type":"array","items":{"type":"number"},"default":["bad"]}""",
      """"x":{"type":"string","enum":[1]}"""))intercept[IllegalArgumentException](ParameterSchema.validate(schema(body)))
  }
  test("required names and optional values are explicit; no parameter contract may be omitted"){
    val n=schema(""""x":{"type":"string"}""")
    n.path("required").asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode].add("missing")
    intercept[IllegalArgumentException](ParameterSchema.validate(n))
    ParameterSchema.validate(schema(""))
  }
}
