package dacp.repository


import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files,Path}
import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Publisher-supplied evidence, never an execution input or a safety/legal certification. */
case class Provenance(sourceArchive:Option[Path]=None, sourceRepository:Option[String]=None,
                      sourceRevision:Option[String]=None, license:Option[String]=None,
                      licenseFile:Option[Path]=None, buildInstructions:Option[String]=None,
                      sbom:Option[Path]=None) {
  def validate():Unit = {
    val metadata=Provenance.textMetadata(this)
    Provenance.validateMetadata(metadata)
    Seq(sourceArchive,licenseFile,sbom).flatten.foreach { file =>
      require(Files.isRegularFile(file) && Files.size(file)>0,"missing or empty provenance file")
      require(Provenance.validFilename(file.getFileName.toString),"invalid provenance filename")
    }
    sourceArchive.foreach(p=>require(Provenance.isArchive(p.getFileName.toString),"source_archive must be a ZIP or TAR archive"))
  }
}
object Provenance {
  private val textKeys=Set("source_repository","source_revision","license","build_instructions")
  val fileKeys=Vector("source_archive","license_file","sbom")
  private val keys=textKeys++fileKeys
  def validFilename(name:String):Boolean=name.length<=255 && name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")
  def isArchive(name:String):Boolean=java.util.ServiceLoader.load(classOf[dacp.operator.SourceArchive]).iterator.asScala.exists(_.supports(name))
  def textMetadata(p:Provenance):com.fasterxml.jackson.databind.node.ObjectNode = {
    val n=Repository.mapper.createObjectNode()
    Seq("source_repository"->p.sourceRepository,"source_revision"->p.sourceRevision,"license"->p.license,
      "build_instructions"->p.buildInstructions).foreach {case(k,v)=>v.foreach(n.put(k,_))}
    n
  }
  def validateMetadata(n:JsonNode):Unit = {
    require(n.isObject && n.fieldNames.asScala.toSet.subsetOf(keys),"invalid provenance settings")
    textKeys.foreach { key => Option(n.get(key)).foreach { v =>
      val limit=if(key=="build_instructions")32768 else if(key=="source_repository")4096 else 256
      require(v.isTextual && v.asText.trim.nonEmpty && v.asText.getBytes(UTF_8).length<=limit &&
        !v.asText.exists(c=>c.isControl && !(key=="build_instructions" && Set('\n','\r','\t')(c))),s"invalid provenance $key")
    }}
    Option(n.get("source_repository")).foreach { v =>
      val uri=URI.create(v.asText)
      require(Set("http","https")(uri.getScheme) && uri.getHost!=null && uri.getUserInfo==null,
        "source_repository must be an HTTP(S) URL without credentials")
    }
    require(!n.has("source_revision") || n.has("source_repository"),"source_revision requires source_repository")
    fileKeys.foreach { key=>Option(n.get(key)).foreach { a=>
      require(a.isObject && a.fieldNames.asScala.toSet==Set("sha256","size","filename") &&
        a.path("sha256").isTextual && a.path("sha256").asText.matches("[a-f0-9]{64}") &&
        a.path("size").isIntegralNumber && a.path("size").canConvertToLong && a.path("size").asLong>0 &&
        a.path("filename").isTextual && validFilename(a.path("filename").asText),"invalid provenance artifact descriptor")
      if(key=="source_archive")require(isArchive(a.path("filename").asText),"source_archive must be a ZIP or TAR archive")
    }}
  }
  def fromDefinition(n:JsonNode, directory:Path):Provenance = {
    require(n.isObject && n.fieldNames.asScala.toSet.subsetOf(keys),"invalid provenance settings")
    def text(key:String)=Option(n.get(key)).map { v=>require(v.isTextual,s"provenance $key must be text");v.asText }
    def file(key:String)=text(key).map { s=>require(s.nonEmpty,"empty provenance file path");directory.resolve(s) }
    val p=Provenance(file("source_archive"),text("source_repository"),text("source_revision"),text("license"),
      file("license_file"),text("build_instructions"),file("sbom"))
    p.validate();p
  }
  def descriptors(n:JsonNode):Seq[JsonNode] = fileKeys.flatMap(k=>Option(n.get(k)))
}
