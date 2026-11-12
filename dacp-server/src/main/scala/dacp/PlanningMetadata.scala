package dacp

import dacp.union.Json
import xstream.engine.Jobs
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Files
import scala.jdk.CollectionConverters._

/** Read only installed metadata: no operator invocation, input scan, image load or download. */
object PlanningMetadata {
  def profiles(config:Config,references:Vector[String],check:()=>Unit=()=>()):JsonNode={
    require(references.size<=256&&references.distinct.size==references.size,"too many or duplicate planning operators")
    val out=Json.obj()
    references.foreach { reference=>
      check()
      val p=reference.split("@",-1);require(p.length==2&&p.forall(_.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")),"invalid planning operator")
      val entry=out.putObject(reference)
      if(reference==Jobs.ReadItem)entry.put("runtime","host").put("executor","")
      else {
        val local=config.localOperatorLibrary.resolve(p(0)).resolve(p(1)).resolve("operator.yaml")
        val cached=config.remoteOperatorLibraryUrls.iterator.map { base=>
          val hash=java.security.MessageDigest.getInstance("SHA-256").digest(base.getBytes(java.nio.charset.StandardCharsets.UTF_8)).map(b=>f"${b&255}%02x").mkString
          config.operatorCache.resolve("manifests").resolve(hash).resolve(p(0)).resolve(p(1)+".json")
        }.find(Files.isRegularFile(_))
        val metadata=if(Files.isRegularFile(local)&&Files.size(local)<=65536)Some(Json.yaml(local)->true)
          else cached.filter(Files.size(_)<=65536).map(f=>Json.read(f)->false)
        metadata.foreach {case(m,isLocal)=>
          require(m.path("name").asText==p(0)&&m.path("version").asText==p(1),"planning operator identity mismatch")
          val executor=if(isLocal)m.path("executor").asText("container")else m.path("execution").path("executor").asText
          val declaredImage=if(isLocal)m.path("image").asText else m.path("environment").path("image").asText(m.path("runtime").path("image").asText)
          val image=if(declaredImage.matches("sha256:[a-f0-9]{64}"))declaredImage else {
            // Inspect an already installed image only; never pull or load it for planning.
            scala.util.Try(xstream.operator.inference.OperatorInference.withDocker(d=>d.inspectImageCmd(declaredImage).exec().getId)).getOrElse(declaredImage)
          }
          entry.put("executor",executor).put("runtime",image).put("metadata","installed")
          // Exact environment identity is only an estimate of fusion eligibility. The
          // executor must still accept the whole group during member preparation.
          if(config.operatorFusionEnabled&&image.matches("sha256:[a-f0-9]{64}")){
            val environment=if(isLocal)m.path("executor_config")else m.path("execution").path("config")
            val key=image+":"+executor+":"+m.path("environment").path("fingerprint").asText+":"+environment.toString
            entry.put("fusion_key",dacp.union.Credentials.hash(key))
          }
        }
      }
    };out
  }
}
