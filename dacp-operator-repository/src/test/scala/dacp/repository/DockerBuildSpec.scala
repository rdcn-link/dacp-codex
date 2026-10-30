package dacp.repository



import java.nio.file.{Files,Path}
import java.util.zip.{ZipEntry,ZipOutputStream,ZipFile}
import java.util.concurrent.TimeUnit
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

/** Opt-in real compiler checks: DACP_TEST_BUILD_IMAGE must name a locally installed pinned compiler image. */
class DockerBuildSpec extends AnyFunSuite {
  test("registered Java and Scala recipes compile and load actual JAR entrypoints in isolation"){
    val image=sys.env.getOrElse("DACP_TEST_BUILD_IMAGE",cancel("set DACP_TEST_BUILD_IMAGE for real Docker compiler checks"))
    require(image.matches("sha256:[a-f0-9]{64}"))
    val root=Files.createTempDirectory("jvm-source-build-")
    try for((language,code)<-Seq(
      "java"->"public final class Example { public static void main(String[] args) { System.out.print(\"compiled-java\"); } }",
      "scala"->"object Example { def main(args: Array[String]): Unit = print(\"compiled-scala\") }")){
      val source=root.resolve(language+".zip");val zip=new ZipOutputStream(Files.newOutputStream(source))
      try{zip.putNextEntry(new ZipEntry("src/Example."+language));zip.write(code.getBytes("UTF-8"));zip.closeEntry()}finally zip.close()
      val manifest=Repository.mapper.createObjectNode();manifest.putObject("build").put("language",language)
      manifest.putObject("execution").put("executor","jvm").put("entrypoint","Example")
      manifest.putObject("provenance").set[com.fasterxml.jackson.databind.JsonNode]("source_archive",Repository.store(root,source,language+".zip"))
      val runtime=Repository.mapper.createObjectNode();runtime.putObject("build").put("image",image)
      val artifact=new DockerSourceBuild().build(root,manifest,runtime)
      val path=Repository.blob(root,artifact.path("sha256").asText)
      val jar=new ZipFile(path.toFile);try assert(jar.getEntry("Example.class")!=null)finally jar.close()
      val p=new ProcessBuilder("docker","run","--rm","--network","none","--mount",s"type=bind,src=$path,dst=/operator.jar,readonly","--entrypoint","/opt/java/bin/java",image,"-cp","/operator.jar:/opt/lib/*","Example").redirectError(ProcessBuilder.Redirect.DISCARD).start()
      val result=new String(p.getInputStream.readNBytes(1024),"UTF-8")
      if(!p.waitFor(30,TimeUnit.SECONDS)){p.destroyForcibly();fail("compiled JVM program timed out")}
      assert(p.exitValue()==0 && result=="compiled-"+language)
    }finally{val files=Files.walk(root);try files.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally files.close()}
  }
}
