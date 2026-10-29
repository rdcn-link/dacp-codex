package dacp

import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class ReadItemSpec extends AnyFunSuite {
  def params(values:(String,Object)*)=new java.util.LinkedHashMap[String,Object](values.toMap.asJava)
  def task(p:java.util.Map[String,Object])=Task("read",Map.empty,Jobs.ReadItem,p)
  test("context and DACP URIs are validated and resolved without changing the submitted definition") {
    val t=task(params("uri"->"context:/weather/observations"))
    Jobs.validate(t)
    val local=Jobs.resolveReads(JobPlan(Vector(t),Map("result"->("read","result"))),"dacp://localhost:8815/")
    assert(Jobs.readBinding(local.tasks.head).get.uri.contains("dacp://localhost:8815/weather/observations"))
    assert(t.params.get("uri")=="context:/weather/observations")
    val remote=task(params("uri"->"dacp://remote:8815/weather/observations"))
    assert(Jobs.resolveReads(JobPlan(Vector(remote),Map.empty),"dacp://local:8815").tasks.head==remote)
    Seq(params(),params("path"->"/weather/observations","uri"->"dacp://remote/weather/a"),params("uri"->"context://host/a"),params("uri"->"context:/weather/../a"),params("uri"->"context:/weather"),params("uri"->"https://host/weather/a"),params("uri"->"dacp://token@host/weather/a"),params("uri"->"context:/weather/a?token=secret"),params("uri"->"context:/weather/a","sql"->"SELECT * FROM source","view"->"bytes")).foreach(p=>intercept[IllegalArgumentException](Jobs.validate(task(p))))
  }
  test("readonly single Item SQL lowers to plugin scans and projects the inferred schema") {
    val scan=ItemSql.parse("SELECT name, id FROM source WHERE (id >= 2 AND name != 'O''Brien') OR id IN (4,5) LIMIT 10;")
    assert(scan.columns.contains(Seq("name","id")) && scan.limit.contains(10) && scan.predicate.get.op=="or")
    val schema=new org.apache.arrow.vector.types.pojo.Schema(Seq(Records.field("id","int64"),Records.field("name","string")).asJava)
    assert(scan.bind(schema).outputSchema.getFields.asScala.map(_.getName)==Seq("name","id"))
    val b=Jobs.readBinding(task(params("uri"->"context:/weather/a","sql"->"SELECT id FROM source LIMIT 0"))).get
    assert(b.view=="records" && b.scan.get.limit.contains(0))
    Seq("DELETE FROM source","SELECT * FROM other","SELECT * FROM source; DROP TABLE source","SELECT count(*) FROM source","SELECT * FROM source ORDER BY id","SELECT * FROM source WHERE id = 1 -- comment","SELECT * FROM source LIMIT -1","SELECT * FROM source WHERE id = 1 UNION SELECT * FROM other").foreach(q=>intercept[IllegalArgumentException](ItemSql.parse(q)))
  }
}
