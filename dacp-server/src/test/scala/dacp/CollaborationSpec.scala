package dacp

import java.nio.file.Path
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator

class CollaborationSpec extends AnyFunSuite {
  def config = Config("localhost",18815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"))
  val yaml="""job: inline
    |tasks:
    | - id: first
    |   operator:
    |     runtime: python@1.0
    |     source: |
    |       def process(inputs, params):
    |           yield from inputs['source']
    |   inputs:
    |     source: {uri: 'dacp://localhost:18815/data/message'}
    |""".stripMargin
  test("task titles describe work while ids remain dependency identifiers") {
    val text=yaml.replace("id: first","id: first\n   title: 汇总销量")
    assert(Jobs.parse(text).tasks.head.title.contains("汇总销量"))
    assert(Jobs.parse(text).tasks.head.id=="first")
    assert(Jobs.parse(yaml).tasks.head.title.isEmpty)
    for(value<-Seq("''","123","null","'"+("x"*257)+"'"))
      intercept[IllegalArgumentException](Jobs.parse(yaml.replace("id: first",s"id: first\n   title: $value")))
  }
  test("source task preserves text and explicit runtime without an operator ID") {
    val task=Jobs.parse(yaml).tasks.head
    assert(task.operator.isEmpty)
    assert(task.source.get.runtime=="python@1.0")
    assert(task.source.get.source.contains("    yield from"))
    intercept[IllegalArgumentException](Jobs.parse(yaml.replace("runtime: python@1.0","runtime: arbitrary/image")))
    intercept[IllegalArgumentException](Jobs.parse(yaml.replace("runtime: python@1.0","runtime: python@1.0\n     image: attacker")))
  }
  test("normal mode rejects source before contacting Docker, DEBUG explicitly permits it") {
    val task=Jobs.parse(yaml).tasks.head
    val error=intercept[IllegalArgumentException](Jobs.authorizeSource(config,task))
    assert(error.getMessage.contains("FORBIDDEN"))
    Jobs.authorizeSource(config.copy(debug=true),task)
    val allocator=new RootAllocator()
    try {
      val executor=new TaskExecutor(config,allocator)
      intercept[IllegalArgumentException](executor.execute(task,Map.empty,_=>fail("no output expected"),new StreamCancel()))
    } finally allocator.close()
  }
  test("repository tasks remain permitted without DEBUG") {
    val task=Jobs.parse(yaml.replace("""operator:
    |     runtime: python@1.0
    |     source: |
    |       def process(inputs, params):
    |           yield from inputs['source']""".stripMargin,"operator: identity@1.1")).tasks.head
    Jobs.authorizeSource(config,task)
  }
  test("URI policy rejects traversal and prefix lookalikes") {
    assert(DacpUri.within("dacp://localhost:18815/data/item","dacp://localhost:18815/data"))
    assert(!DacpUri.within("dacp://localhost:18815/data-other/item","dacp://localhost:18815/data"))
    Seq("dacp://localhost/data/%2e%2e/secret","https://localhost/data/item","dacp://user:password@localhost/data/item")
      .foreach(v=>intercept[IllegalArgumentException](DacpUri.parse(v)))
  }
  test("named-port DAG orders dependencies and rejects cycles and dataset-only inputs") {
    val text="""job: pipeline
      |tasks:
      | - {id: b, operator: identity@1.1, inputs: {source: {task: a, port: result}}}
      | - {id: a, operator: identity@1.1, inputs: {source: {uri: 'dacp://localhost:18815/data/item'}}}
      |""".stripMargin
    assert(Jobs.parse(text).tasks.map(_.id)==Vector("a","b"))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("uri: 'dacp://localhost:18815/data/item'","task: b")))
    intercept[IllegalArgumentException](Jobs.parse(text.replace("/data/item","/data")))
  }
}
