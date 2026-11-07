package dacp

import org.scalatest.funsuite.AnyFunSuite

class AccessOperationsSpec extends AnyFunSuite {
  test("metadata requests distinguish directory listing, item description and counting") {
    assert(AccessOperations.metadata(Seq("datasets","science"))=="item.list")
    assert(AccessOperations.metadata(Seq("datasets","science","list","/folder"))=="item.list")
    assert(AccessOperations.metadata(Seq("datasets","science","/file"))=="item.describe")
    assert(AccessOperations.metadata(Seq("datasets","science","count","/file"))=="item.count")
  }
  test("control requests have distinct business names; unknown actions never enter logs verbatim") {
    val operations=Seq("prepare"->"job.prepare","start"->"job.start","execution_status"->"job.status","cancel_execution"->"job.cancel","membership"->"union.membership.get","node_status"->"node.status","materialized_status"->"step.materialization.status")
    operations.foreach {case(action,name)=>assert(AccessOperations.action("dacp.union."+action)==name)}
    assert(AccessOperations.action("dacp.capabilities")=="service.capabilities")
    assert(AccessOperations.action("dacp.catalog")=="catalog.describe")
    assert(AccessOperations.action("dacp.infer")=="step.infer")
    assert(AccessOperations.action("dacp.infer_job")=="job.infer")
    assert(AccessOperations.action("unknown-secret-value")=="request.unsupported")
  }
}
