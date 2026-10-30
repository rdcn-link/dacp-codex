package dacp

import _root_.xstream.data._


import org.scalatest.funsuite.AnyFunSuite

class RecordedJobGraphSpec extends AnyFunSuite {
  test("stored scripts render independently of current operator parameter contracts") {
    val script="""name: Stored read
                  |tasks:
                  |  - id: read
                  |    operator: read_item@1.0
                  |    inputs: {}
                  |    params: {path: /old/item}
                  |  - id: filter
                  |    operator: filter@1.0
                  |    inputs: {source: 'read:result'}
                  |outputs: {result: filter}
                  |""".stripMargin
    val graph=RecordedJobGraph(script)
    assert(graph.path("tasks").get(1).path("inputs").get(0).path("task").asText=="read")
    assert(graph.path("outputs").path("result").path("port").asText=="result")
    intercept[IllegalArgumentException](Jobs.parse(script))
  }
}
