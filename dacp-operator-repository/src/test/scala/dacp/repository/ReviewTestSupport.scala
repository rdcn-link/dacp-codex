package dacp.repository

import java.nio.file.{Files,Path}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import scala.jdk.CollectionConverters._

object ReviewTestSupport {
  /** Explicit test administrator decisions; never used by production startup. */
  def approveLocal(root:Path):Unit={
    val manifests=Repository.catalog(root).elements.asScala.toVector
    val admin=new Administration(root,Some("test-administrator-password"))
    try manifests.foreach{original=>
      val n=original.deepCopy[ObjectNode]();val runtime=n.path("runtime")
      val rt=Repository.mapper.createObjectNode().put("name","runtime").put("version",runtime.path("image").asText.stripPrefix("sha256:")).put("description","Test runtime").put("image",runtime.path("image").asText)
      rt.set[JsonNode]("archive",runtime.path("archive"));rt.putArray("executors").add(n.path("execution").path("executor").asText).add("python");rt.putObject("dependencies")
      val build=rt.putObject("build").put("image",runtime.path("image").asText);build.set[JsonNode]("archive",runtime.path("archive"));build.putArray("languages").add("python")
      if(!admin.runtimes.elements.asScala.exists(_.path("image")==rt.path("image")))admin.addRuntime(rt)
      n.put("source_public",true);n.putObject("build").put("language","python")
      val artifact=Option(n.remove("artifact"))
      if(!n.path("provenance").has("source_archive")){
        val source=root.resolve("test-source.zip");Files.writeString(source,"test source")
        val p=if(n.has("provenance"))n.path("provenance").asInstanceOf[ObjectNode]else n.putObject("provenance")
        p.set[JsonNode]("source_archive",Repository.store(root,source,"sources.zip"))
      }
      admin.descriptors(n).foreach(a=>admin.grantBlob("test",a.path("sha256").asText))
      val submission=admin.submit(n,"test");val id=submission.path("id").asText
      admin.processing(id,"analysis",Repository.mapper.createObjectNode().put("status","complete").put("score",5))
      admin.processing(id,"build_result",Repository.mapper.createObjectNode().put("status","succeeded"),artifact)
      admin.review(id,"approved",Repository.mapper.createArrayNode().add("Data processing"),"")
    }finally admin.close()
  }
}
