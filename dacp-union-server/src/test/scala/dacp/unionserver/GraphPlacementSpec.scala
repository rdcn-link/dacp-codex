package dacp.unionserver
import org.scalatest.funsuite.AnyFunSuite
class GraphPlacementSpec extends AnyFunSuite {
  test("Union places reads at data owners and respects member capacity") {
    val steps=Vector(PlanningStep("read",Vector.empty,Some("east"->Some(100L))),PlanningStep("filter",Vector("read")),PlanningStep("sink",Vector("filter")))
    val result=GraphPlacement.plan(steps,Vector(PlanningTarget("east",2),PlanningTarget("west",2)))
    assert(result("read").target=="east" && result("filter").target=="east" && result("sink").target=="west")
    assert(result("read").reason=="data_locality")
    intercept[IllegalArgumentException](GraphPlacement.plan(steps,Vector(PlanningTarget("west",10))))
  }
}
