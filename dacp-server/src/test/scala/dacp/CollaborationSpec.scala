package dacp

import _root_.xstream.engine.{Jobs}

import _root_.xstream.data._


import java.nio.file.Path
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator

class CollaborationSpec extends AnyFunSuite {
  def config = Config("localhost",18815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"))
  val yaml="""name: inline
    |operators:
    |  local:
    |    type: script
    |    role: source
    |    runtime: python
    |    interpreter: /usr/local/bin/python
    |    script: {source: 'print("hello")'}
    |    inputs: {}
    |    outputs: {result: {view: bytes, binding: stdout, format: text}}
    |steps:
    | - id: first
    |   operator: {inline: local}
    |   inputs: {}
    |""".stripMargin
  test("task titles describe work while ids remain dependency identifiers") {
    val text=yaml.replace("id: first","id: first\n   title: 汇总销量")
    assert(Jobs.parse(text).steps.head.title.contains("汇总销量"))
    assert(Jobs.parse(text).steps.head.id=="first")
    for(value<-Seq("''","123","null","'"+("x"*257)+"'"))
      intercept[IllegalArgumentException](Jobs.parse(yaml.replace("id: first",s"id: first\n   title: $value")))
  }
  test("inline definitions are local-only without a server bypass") {
    val task=Jobs.parse(yaml).steps.head
    assert(task.inlineOperator.get.name=="local")
    assert(intercept[IllegalArgumentException](Jobs.authorizeSource(config,task)).getMessage.contains("FORBIDDEN"))
    intercept[IllegalArgumentException](xstream.execution.RemoteJobPolicy.validateYaml(yaml))
  }
  test("references use task with optional port and reject old object bindings") {
    val text="""name: pipeline
      |steps:
      | - {id: b, operator: identity@1.1, inputs: {source: a}}
      | - {id: a, operator: live_ticks@1.0, inputs: {}}
      | - id: client_sink
      |   operator: client_sink@1.0
      |   inputs: {result: b}
      |""".stripMargin
    val plan=Jobs.parse(text)
    assert(plan.steps.map(_.id)==Vector("a","b"))
    assert(plan.steps.last.inputs("source").port=="result" && plan.outputs("result")==("b","result"))
    assert(Jobs.parse(text.replace("source: a","source: a:other")).steps.last.inputs("source").port=="other")
    Seq("{task: a}","{uri: 'dacp://localhost:18815/data/item'}","a:","a:b:c","unknown").foreach { binding=>
      intercept[IllegalArgumentException](Jobs.parse(text.replace("source: a","source: "+binding)))
    }
    intercept[IllegalArgumentException](Jobs.parse(text.replace("result: b","result: {task: b, port: result}")))
  }
  test("stream dependencies define the graph and depends_on is rejected") {
    val text="""name: stream
      |steps:
      | - {id: consume, operator: sink@1, inputs: {source: generate}}
      | - {id: generate, operator: generate@1, inputs: {}}
      |""".stripMargin
    val plan=Jobs.parse(text)
    assert(Jobs.dependencies(plan.steps.last).toSet==Set("generate"))
    assert(Jobs.roots(plan)==Set("consume"))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("source: generate}","source: generate}, depends_on: [generate]")))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("id: generate, operator: generate@1, inputs: {}","id: generate, operator: generate@1, inputs: {source: consume}")))
  }
  test("item_source requires an Item URI and defaults to metadata-based view selection") {
    val text="""name: read
      |steps:
      | - {id: read, operator: item_source@1.0, inputs: {}, params: {uri: 'dacp://localhost:18815/data/item', view: records}}
      | - id: client_sink
      |   operator: client_sink@1.0
      |   inputs: {result: read}
      |""".stripMargin
    assert(Jobs.readBinding(Jobs.parse(text).steps.head).get.view=="records")
    assert(Jobs.readBinding(Jobs.parse(text.replace(", view: records","")).steps.head).get.view=="auto")
    assert(Jobs.readBinding(Jobs.parse(text.replace("view: records","view: image")).steps.head).get.view=="image")
    Seq(text.replace("/data/item","/data"),text.replace("view: records","view: bad view"),text.replace("view: records","view: auto"),text.replace("inputs: {}","inputs: {source: read}"))
      .foreach(s=>intercept[IllegalArgumentException](Jobs.parse(s)))
  }
  test("URI policy rejects traversal and prefix lookalikes") {
    assert(DacpUri.within("dacp://localhost:18815/data/item","dacp://localhost:18815/data"))
    assert(!DacpUri.within("dacp://localhost:18815/data-other/item","dacp://localhost:18815/data"))
    Seq("dacp://localhost/data/%2e%2e/secret","https://localhost/data/item","dacp://user:password@localhost/data/item")
      .foreach(v=>intercept[IllegalArgumentException](DacpUri.parse(v)))
  }
}
