package dacp.repository

import java.nio.file.{Files,Path}
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}
import java.util.zip.{ZipEntry,ZipOutputStream}
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class ProvenanceSpec extends AnyFunSuite {
  def fixture(f:Path=>Unit):Unit={
    val root=Files.createTempDirectory("provenance-")
    try f(root) finally {val paths=Files.walk(root);try paths.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally paths.close()}
  }
  val ports=Repository.mapper.readTree("""{"inputs":{"source":{"representations":["bytes"],"formats":[]}},"outputs":{"result":{"representations":["bytes"],"formats":[]}}}""")
  val exporter=new Repository.ImageExporter {
    def inspect(image:String):String="sha256:"+"a"*64
    def save(image:String,file:Path):Unit={Files.writeString(file,"runtime");()}
  }
  def archive(root:Path):Path={
    val file=root.resolve("sources.zip");val out=new ZipOutputStream(Files.newOutputStream(file))
    try {out.putNextEntry(new ZipEntry("main.c"));out.write("int main() {return 0;}".getBytes);out.closeEntry()}finally out.close()
    file
  }
  def publish(root:Path,name:String,p:Provenance)=Repository.mapper.readTree(
    Repository.publish(root,name+"@1","runtime:1",ports,exporter,provenance=p))

  test("source, license and SBOM are immutable verified downloads with escaped review metadata") {fixture {root=>
    val source=archive(root)
    val license=root.resolve("LICENSE.txt");Files.writeString(license,"Example license text")
    val sbom=root.resolve("sbom.json");Files.writeString(sbom,"{\"components\":[]}")
    val p=Provenance(Some(source),Some("https://example.org/project"),Some("abcdef123"),Some("MIT"),Some(license),Some("cc main.c -o program\n<script>not HTML</script>"),Some(sbom))
    val one=publish(root,"one",p);val two=publish(root,"two",p)
    assert(one.path("provenance")==two.path("provenance"))
    Repository.validate(root,one)
    val html=WebPages.detail(one,Seq(one))
    assert(html.contains("Source archive") && html.contains("License text") && html.contains("Dependency SBOM"))
    assert(html.contains("&lt;script&gt;") && !html.contains("<script>not HTML"))
    assert(html.contains("do not certify safety") && html.contains("abcdef123"))
    val server=new RepositoryServer(root,"127.0.0.1",0);server.start()
    try {
      val client=HttpClient.newHttpClient()
      Provenance.descriptors(one.path("provenance")).foreach {a=>
        val uri=URI.create(s"http://127.0.0.1:${server.boundPort}/v1/blobs/sha256/${a.path("sha256").asText}")
        val response=client.send(HttpRequest.newBuilder(uri).GET().build(),HttpResponse.BodyHandlers.ofByteArray())
        assert(response.statusCode()==200 && response.body().length==a.path("size").asLong)
      }
    }finally server.close()
    intercept[IllegalArgumentException](publish(root,"one",p.copy(license=Some("changed"))))
    Files.writeString(Repository.blob(root,one.path("provenance").path("source_archive").path("sha256").asText),"corrupt")
    intercept[IllegalArgumentException](Repository.validate(root,one))
  }}
  test("missing evidence is labelled explicitly and unsafe source links never become links") {fixture {root=>
    val manifest=publish(root,"closed",Provenance())
    val html=WebPages.detail(manifest,Seq(manifest))
    assert(html.contains("Source archive not provided") && html.contains("License not declared"))
    val bad=manifest.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    bad.putObject("provenance").put("source_repository","javascript:alert(1)")
    assert(!WebPages.detail(bad,Seq(bad)).contains("href=\"javascript:"))
  }}
  test("invalid review metadata fails before Docker export or publication") {fixture {root=>
    val never=new Repository.ImageExporter {
      def inspect(image:String):String=fail("must validate before Docker")
      def save(image:String,file:Path):Unit=fail("must validate before Docker")
    }
    val missing=root.resolve("missing.zip")
    val invalid=Seq(Provenance(sourceArchive=Some(missing)),Provenance(sourceRepository=Some("javascript:alert(1)")),
      Provenance(sourceRepository=Some("https://user:secret@example.org/repo")),Provenance(sourceRevision=Some("abc")),
      Provenance(license=Some("")),Provenance(buildInstructions=Some("x"*32769)),Provenance(license=Some("bad\u0000license")))
    invalid.foreach {p=>intercept[IllegalArgumentException](Repository.publish(root,"bad@1","runtime",ports,never,provenance=p))}
    assert(!Files.exists(root.resolve("bad")))
    intercept[IllegalArgumentException](Provenance.fromDefinition(Repository.mapper.readTree("""{"unknown":true}"""),root))
    intercept[IllegalArgumentException](Provenance.fromDefinition(Repository.mapper.readTree("""{"license":123}"""),root))
  }}
  test("seeding resolves evidence files relative to definitions and preserves multiline instructions") {fixture {root=>
    val dir=root.resolve("definitions/example/1");Files.createDirectories(dir)
    val source=archive(dir)
    val definition=Repository.mapper.createObjectNode().put("name","example").put("version","1").put("image","runtime")
    definition.set[com.fasterxml.jackson.databind.JsonNode]("inputs",ports.path("inputs"))
    definition.set[com.fasterxml.jackson.databind.JsonNode]("outputs",ports.path("outputs"))
    definition.putObject("provenance").put("source_archive",source.getFileName.toString)
      .put("source_repository","https://example.org/repo").put("source_revision","123abc").put("build_instructions","step 1\nstep 2")
    Files.writeString(dir.resolve("operator.yaml"),definition.toString)
    val catalog=root.resolve("catalog")
    Repository.seed(catalog,root.resolve("definitions"),exporter)
    val metadata=Repository.catalog(catalog).get(0).path("provenance")
    assert(metadata.path("build_instructions").asText=="step 1\nstep 2")
    assert(metadata.path("source_archive").path("sha256").asText==Repository.checksum(source))
    Repository.seed(catalog,root.resolve("definitions"),exporter)
  }}
}
