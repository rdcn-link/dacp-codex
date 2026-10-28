package dacp.repository

import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class AdministrationSpec extends AnyFunSuite {
  test("credential hashes and revocations survive restart; only one process owns repository metadata"){
    val root=Files.createTempDirectory("repository-admin-")
    try {
      val first=new Administration(root,Some("persistent-admin-password"))
      val issued=first.issue("alice");val token=issued.path("token").asText
      try {
        assert(first.login("persistent-admin-password"));assert(!first.login("wrong"))
        assert(first.authenticate("Bearer "+token)=="alice")
        intercept[Exception](new Administration(root,None))
        val state=Files.readString(root.resolve(".admin/state.json"));assert(!state.contains(token) && !state.contains("persistent-admin-password"))
      }finally first.close()
      val second=new Administration(root,Some("different-initial-password"))
      try {assert(second.login("persistent-admin-password"));assert(!second.login("different-initial-password"));assert(second.authenticate("Bearer "+token)=="alice");second.revoke(issued.path("id").asText)}finally second.close()
      val third=new Administration(root,None)
      try intercept[PublishFailure](third.authenticate("Bearer "+token))finally third.close()
    }finally{val files=Files.walk(root);try files.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally files.close()}
  }
}
