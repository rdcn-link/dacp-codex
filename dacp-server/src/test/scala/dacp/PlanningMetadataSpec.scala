package dacp

import java.nio.file.{Files,Path}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class PlanningMetadataSpec extends AnyFunSuite {
  test("installed planning profiles expose pinned environments without reading code artifacts") {
    val root=Files.createTempDirectory("planning-metadata-")
    val repository="https://operators.example.test"
    val hash=java.security.MessageDigest.getInstance("SHA-256").digest(repository.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map(b=>f"${b&255}%02x").mkString
    val home=root.resolve(s"cache/manifests/$hash/example");Files.createDirectories(home)
    Files.writeString(home.resolve("1.json"),s"""{"name":"example","version":"1","execution":{"executor":"jvm","config":{}},"environment":{"image":"sha256:${"a"*64}"}}""")
    val c=Config("localhost",1234,"localhost",Map.empty,root.resolve("cert"),root.resolve("key"),root.resolve("ca"),operatorCache=root.resolve("cache"),remoteOperatorLibraryUrls=Seq(repository))
    try {
      val profile=PlanningMetadata.profiles(c,Vector("example@1","missing@1","item_source@1.0"))
      assert(profile.path("example@1").path("fusion_key").asText.nonEmpty)
      assert(profile.path("example@1").path("executor").asText=="jvm")
      assert(profile.path("missing@1").isEmpty)
      assert(!Files.exists(home.resolve("missing.jar")))
      intercept[IllegalArgumentException](PlanningMetadata.profiles(c,Vector("../bad@1")))
      intercept[IllegalArgumentException](PlanningMetadata.profiles(c,Vector("example@1","example@1")))
      val disabled=PlanningMetadata.profiles(c.copy(operatorFusionEnabled=false),Vector("example@1"))
      assert(!disabled.path("example@1").has("fusion_key"))
    }finally {c.close();val paths=Files.walk(root);try paths.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally paths.close()}
  }
}
