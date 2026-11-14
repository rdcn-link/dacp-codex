package dacp.unionserver
import dacp.union._
import org.scalatest.funsuite.AnyFunSuite
class GraphPlacementSpec extends AnyFunSuite {
  test("Union places reads at data owners and respects member capacity") {
    val steps=Vector(PlanningStep("read",Vector.empty,Some("east"->Some(100L))),PlanningStep("filter",Vector("read")),PlanningStep("sink",Vector("filter")))
    val result=GraphPlacement.plan(steps,Vector(PlanningTarget("east",2),PlanningTarget("west",2)))
    assert(result("read").target=="east" && result("filter").target=="east" && result("sink").target=="west")
    assert(result("read").reason=="data_locality")
    intercept[IllegalArgumentException](GraphPlacement.plan(steps,Vector(PlanningTarget("west",10))))
  }

  test("whole graph lookahead moves a join to its downstream owner instead of the locally cheapest node") {
    val graph=Vector(PlanningStep("a",Vector.empty,Some("east"->Some(1000000L))),PlanningStep("b",Vector.empty,Some("west"->Some(10000L))),
      PlanningStep("join",Vector("a","b")),PlanningStep("sink",Vector("join"),requested=Some("west")))
    val input=PlanningInput(graph,Vector(PlanningTarget("east",8),PlanningTarget("west",8)))
    val policy=new PhysicalPlanOptimizer{val id="test";override def estimate(i:PlanningInput,s:PlanningStep,n:PlanningTarget,b:Double,c:Map[String,Object])=Some(StepEstimate(if(s.id=="join")1e9 else b,0,0))}
    val result=GraphPlacement.optimize(input,Some(policy))
    assert(result.placements("join").target=="west")
    assert(result.estimate.edges.exists(e=>e.from=="join"&&e.to=="sink"&&e.mode=="arrow_ipc"))
  }
  test("compatible whole partitions estimate fusion while runtime and materialization boundaries stay isolated") {
    val profile=PlanningProfile("jvm","sha256:runtime",Some("locked-env"))
    val nodes=Vector(PlanningTarget("east",8,Set("jvm")))
    def graph(boundary:Boolean)=Vector(PlanningStep("a",Vector.empty,operator="a@1",profiles=Map("east"->profile)),
      PlanningStep("b",Vector("a"),operator="b@1",profiles=Map("east"->profile),fusionBoundary=boundary))
    val fused=GraphPlacement.optimize(PlanningInput(graph(false),nodes))
    assert(fused.estimate.processes==1);assert(fused.estimate.edges.head.mode=="arrow_in_process_candidate")
    val isolated=GraphPlacement.optimize(PlanningInput(graph(true),nodes));assert(isolated.estimate.processes==2)
    assert(fused.estimate.completionMs<isolated.estimate.completionMs)
    val mismatched=graph(false).updated(1,graph(false)(1).copy(profiles=Map("east"->profile.copy(fusionKey=Some("other")))))
    assert(GraphPlacement.optimize(PlanningInput(mismatched,nodes)).estimate.processes==2)
  }
  test("plugins cannot override owner, executor, memory or numeric constraints and cancellation propagates") {
    val graph=Vector(PlanningStep("read",Vector.empty,Some("east"->Some(10L)),operator="item_source@1.0"))
    val input=PlanningInput(graph,Vector(PlanningTarget("east",4),PlanningTarget("west",4)))
    val malicious=new PhysicalPlanOptimizer{val id="bad";override def candidates(i:PlanningInput,s:Map[String,Object])=Vector(Map("read"->"west"))}
    intercept[IllegalArgumentException](GraphPlacement.optimize(input,Some(malicious)))
    val invalid=new PhysicalPlanOptimizer{val id="bad";override def score(p:PlanEstimate,s:Map[String,Object])=Double.NaN}
    intercept[IllegalArgumentException](GraphPlacement.optimize(input,Some(invalid)))
    val memory=new PhysicalPlanOptimizer{val id="large";override def estimate(i:PlanningInput,s:PlanningStep,n:PlanningTarget,b:Double,c:Map[String,Object])=Some(StepEstimate(b,memoryBytes=100))}
    intercept[IllegalArgumentException](GraphPlacement.optimize(input.copy(targets=Vector(PlanningTarget("east",4,freeMemory=Some(50)))),Some(memory)))
    val unsupported=graph.map(_.copy(profiles=Map("east"->PlanningProfile("python"))))
    intercept[IllegalArgumentException](GraphPlacement.optimize(input.copy(steps=unsupported,targets=Vector(PlanningTarget("east",4,Set("jvm"))))))
    intercept[java.util.concurrent.CancellationException](GraphPlacement.optimize(input,check=()=>throw new java.util.concurrent.CancellationException()))
  }
  test("pipeline critical path overlaps independent branches and estimates remain deterministic") {
    val graph=Vector(PlanningStep("a",Vector.empty),PlanningStep("b",Vector.empty),PlanningStep("c",Vector("a","b")))
    val input=PlanningInput(graph,Vector(PlanningTarget("east",5)),beamWidth=8)
    val result=GraphPlacement.optimize(input)
    assert(result.estimate.completionMs<result.estimate.steps.values.map(s=>s.computeMs+s.startupMs).sum)
    assert(result==GraphPlacement.optimize(input))
    intercept[IllegalArgumentException](new CostOptimizer().validate(Map("startup_ms"->java.lang.Double.valueOf(-1))))
    intercept[IllegalArgumentException](PlanningSettings.read(Json.parse("""{"beam_width":0}""")))
  }
}
