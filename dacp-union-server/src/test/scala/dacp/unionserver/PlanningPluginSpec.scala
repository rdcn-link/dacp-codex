package dacp.unionserver

import dacp.union._
import xstream.data.PluginEnvironment
import java.nio.file.Files
import java.util.jar.{JarEntry,JarOutputStream}
import org.scalatest.funsuite.AnyFunSuite

class ExamplePlanningPolicy extends PhysicalPlanOptimizer {
  val id="example-test"
  var closed=false
  override def score(p:PlanEstimate,s:Map[String,Object]):Double=p.completionMs+p.transferBytes
  override def close():Unit={closed=true}
}
class PlanningPluginSpec extends AnyFunSuite {
  test("external service registration discovers optimizer and closes its lifecycle") {
    val root=Files.createTempDirectory("planning-plugin-");val file=root.resolve("optimizer.jar")
    val jar=new JarOutputStream(Files.newOutputStream(file))
    try {jar.putNextEntry(new JarEntry("META-INF/services/dacp.union.PhysicalPlanOptimizer"));jar.write("dacp.unionserver.ExamplePlanningPolicy\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));jar.closeEntry()}finally jar.close()
    val environment=new PluginEnvironment(Some(root));val policy=environment.byId(classOf[PhysicalPlanOptimizer],"example-test").asInstanceOf[ExamplePlanningPolicy]
    try {
      assert(environment.byId(classOf[PhysicalPlanOptimizer],"cost").id=="cost")
      val result=GraphPlacement.optimize(PlanningInput(Vector(PlanningStep("a",Vector.empty)),Vector(PlanningTarget("n",2))),Some(policy))
      assert(result.placements("a").target=="n")
    }finally {environment.close();assert(policy.closed);Files.delete(file);Files.delete(root)}
  }
}
