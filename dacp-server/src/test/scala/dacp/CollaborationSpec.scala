package dacp

import java.nio.file.Path
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator

class CollaborationSpec extends AnyFunSuite {
  def config = Config("localhost",18815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"))
  val yaml="""name: inline
    |tasks:
    | - id: first
    |   operator:
    |     runtime: python@1.0
    |     source: |
    |       def process(ctx):
    |           ctx.bytes_output('result').write(b'hello')
    |   inputs: {}
    |""".stripMargin
  test("task titles describe work while ids remain dependency identifiers") {
    val text=yaml.replace("id: first","id: first\n   title: 汇总销量")
    assert(Jobs.parse(text).tasks.head.title.contains("汇总销量"))
    assert(Jobs.parse(text).tasks.head.id=="first")
    for(value<-Seq("''","123","null","'"+("x"*257)+"'"))
      intercept[IllegalArgumentException](Jobs.parse(yaml.replace("id: first",s"id: first\n   title: $value")))
  }
  test("inline source preserves text and requires DEBUG") {
    val task=Jobs.parse(yaml).tasks.head
    assert(task.source.get.runtime=="python@1.0" && task.source.get.source.contains("    ctx.bytes_output"))
    assert(intercept[IllegalArgumentException](Jobs.authorizeSource(config,task)).getMessage.contains("FORBIDDEN"))
    Jobs.authorizeSource(config.copy(debug=true),task)
  }
  test("references use task with optional port and reject old object bindings") {
    val text="""name: pipeline
      |tasks:
      | - {id: b, operator: identity@1.1, inputs: {source: a}}
      | - {id: a, operator: live_ticks@1.0, inputs: {}}
      |outputs: {result: b}
      |""".stripMargin
    val plan=Jobs.parse(text)
    assert(plan.tasks.map(_.id)==Vector("a","b"))
    assert(plan.tasks.last.inputs("source").port=="result" && plan.outputs("result")==("b","result"))
    assert(Jobs.parse(text.replace("source: a","source: a:other")).tasks.last.inputs("source").port=="other")
    Seq("{task: a}","{uri: 'dacp://localhost:18815/data/item'}","a:","a:b:c","unknown").foreach { binding=>
      intercept[IllegalArgumentException](Jobs.parse(text.replace("source: a","source: "+binding)))
    }
    intercept[IllegalArgumentException](Jobs.parse(text.replace("result: b","result: {task: b, port: result}")))
  }
  test("stream dependencies define the graph and depends_on is rejected") {
    val text="""name: stream
      |tasks:
      | - {id: consume, operator: sink@1, inputs: {source: generate}}
      | - {id: generate, operator: generate@1, inputs: {}}
      |outputs: {}
      |""".stripMargin
    val plan=Jobs.parse(text)
    assert(Jobs.dependencies(plan.tasks.last).toSet==Set("generate"))
    assert(Jobs.roots(plan)==Set("consume"))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("source: generate}","source: generate}, depends_on: [generate]")))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("id: generate, operator: generate@1, inputs: {}","id: generate, operator: generate@1, inputs: {source: consume}")))
  }
  test("read_item requires an Item URI and explicit view") {
    val text="""name: read
      |tasks:
      | - {id: read, operator: read_item@1.0, inputs: {}, params: {uri: 'dacp://localhost:18815/data/item', view: records}}
      |outputs: {result: read}
      |""".stripMargin
    assert(Jobs.readBinding(Jobs.parse(text).tasks.head).get.view=="records")
    Seq(text.replace("/data/item","/data"),text.replace("view: records","view: image"),text.replace(", view: records",""),text.replace("inputs: {}","inputs: {source: read}"))
      .foreach(s=>intercept[IllegalArgumentException](Jobs.parse(s)))
  }
  test("URI policy rejects traversal and prefix lookalikes") {
    assert(DacpUri.within("dacp://localhost:18815/data/item","dacp://localhost:18815/data"))
    assert(!DacpUri.within("dacp://localhost:18815/data-other/item","dacp://localhost:18815/data"))
    Seq("dacp://localhost/data/%2e%2e/secret","https://localhost/data/item","dacp://user:password@localhost/data/item")
      .foreach(v=>intercept[IllegalArgumentException](DacpUri.parse(v)))
  }
}
