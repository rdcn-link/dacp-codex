package dacp

import org.scalatest.funsuite.AnyFunSuite
import dacp.union.YamlDocument

class YamlWorkflowSpec extends AnyFunSuite {
  test("YAML roundtrips typed parameters without changing textual numbers or booleans") {
    val value=YamlDocument.parse("count: 123\nratio: 1.25\nenabled: true\ncode: '001'\nlabel: 'true'\nitems: [1, null, '2026-10-27']\n")
    assert(YamlDocument.parse(YamlDocument.dump(value))==value)
    assert(value.path("count").isIntegralNumber && value.path("code").asText=="001")
  }
  test("YAML rejects duplicate keys, object tags and collection aliases") {
    Seq("x: 1\nx: 2", "x: !!java.lang.ProcessBuilder {}", "x: &x [1]\ny: *x").foreach { text=>
      intercept[IllegalArgumentException](YamlDocument.parse(text))
    }
  }
  test("every workflow requires a readable name distinct from its generated execution id") {
    val text="name: Station screening\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'dacp://localhost/weather/stations', view: bytes}\noutputs: {result: read}\n"
    assert(Jobs.parse(text).name=="Station screening")
    assert(Jobs.parse(text+"designer: {version: 1, positions: {read: {x: 80, y: 140}}, generator: {name: DACP Job Designer, version: test}}\n")==Jobs.parse(text))
    Seq(text.replace("name:","job:"),text.replace("Station screening","''"),text.replace("Station screening","123")).foreach { invalid=>intercept[IllegalArgumentException](Jobs.parse(invalid)) }
  }
}
