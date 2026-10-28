package dacp.operator

import java.nio.file.Path
import com.fasterxml.jackson.databind.JsonNode

/** Repository analysis extension; never execute submitted code or extract it onto the host. */
trait SourceAnalyzer {
  def id:String
  def supports(filename:String):Boolean
  def analyze(archive:Path,filename:String):JsonNode
}

/** Build language extension. Core runs this fixed command only inside an isolated build container. */
trait SourceBuildRecipe {
  def language:String
  def executor:String
  def artifactFilename:String
  def command:Seq[String]
  def verifyArtifact(path:Path,entrypoint:String):Unit
}

/** Unpack source archives with strict path, entry, expanded-size and link checks. */
trait SourceArchive {
  def supports(filename:String):Boolean
  def unpack(archive:Path,filename:String,destination:Path):Unit
}
