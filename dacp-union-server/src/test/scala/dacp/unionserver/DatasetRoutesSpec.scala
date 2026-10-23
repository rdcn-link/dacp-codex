package dacp.unionserver
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
class DatasetRoutesSpec extends AnyFunSuite {
  test("short names have fixed shape, decode uniquely, persist and never reuse removed routes") {
    val dir=Files.createTempDirectory("dataset-routes-")
    var routes=new DatasetRoutes(dir)
    try {
      val ids=(1L to 2000L).map(routes.encode)
      assert(ids.distinct.size==2000 && ids.forall(_.matches("ds-[a-z2-9]{4}-[a-z2-9]{4}")))
      assert(ids.map(routes.decode)==(1L to 2000L))
      val a=routes.assign("member-a","data");val b=routes.assign("member-b","data")
      assert(a!=b && routes.assign("member-a","data")==a)
      val route=routes.resolve(a).get
      assert(route.path("name").asText==a && route.path("local_name").asText=="data")
      assert(!route.has("id"))
      routes.close();routes=new DatasetRoutes(dir)
      assert(routes.assign("member-a","data")==a)
      routes.reconcile("member-a",Set.empty);assert(routes.resolve(a).isEmpty)
      assert(routes.assign("member-a","data")!=a)
      routes.retainMembers(Set("member-a"));assert(routes.resolve(b).isEmpty)
      intercept[IllegalStateException](new DatasetRoutes(dir))
    }finally {routes.close();dacp.Data.deleteTree(dir)}
  }
}
