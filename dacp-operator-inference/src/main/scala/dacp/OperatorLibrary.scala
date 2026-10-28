package dacp

import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.security.MessageDigest
import com.github.dockerjava.api.DockerClient
import com.fasterxml.jackson.databind.JsonNode
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

case class ResolvedOperator(image: String, artifact: Option[Path], artifactPath: Option[String], command: OperatorCommand, inputs: Map[String,PortContract], outputs: Map[String,PortContract], executor:String="", program:Option[OperatorProgram]=None)

/** Immutable operator metadata plus globally shared, content-addressed runtime/code artifacts. */
object OperatorLibrary {
  private def sha(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
  private def digest(path: Path): String = {
    val md = MessageDigest.getInstance("SHA-256"); val in = Files.newInputStream(path)
    try { val buffer = new Array[Byte](1048576); var n = in.read(buffer)
      while(n != -1) { md.update(buffer,0,n); n = in.read(buffer) }
    } finally in.close()
    md.digest().map(b => f"${b & 255}%02x").mkString
  }
  private class MissingOperator extends RuntimeException
  private def fetch(url: String, file: Path, limit: Long, manifest: Boolean = false): Unit = {
    val c = URI.create(url).toURL.openConnection().asInstanceOf[java.net.HttpURLConnection]
    c.setConnectTimeout(15000); c.setReadTimeout(60000); c.setInstanceFollowRedirects(false)
    try {
      if (c.getResponseCode == 404 && manifest) throw new MissingOperator
      require(c.getResponseCode == 200, s"operator library HTTP ${c.getResponseCode}: $url")
      val in = c.getInputStream
      try { val out = Files.newOutputStream(file)
        try { val buffer = new Array[Byte](1048576); var total = 0L; var n = in.read(buffer)
          while(n != -1) { total += n; require(total <= limit,"operator download exceeds size limit"); out.write(buffer,0,n); n = in.read(buffer) }
        } finally out.close()
      } finally in.close()
    } finally c.disconnect()
  }
  private def locked[T](file: Path)(f: => T): T = {
    Files.createDirectories(file.getParent)
    val c = java.nio.channels.FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    val lock = c.lock()
    try f finally { lock.release(); c.close() }
  }
  private def plan(config: OperatorEnvironment, reference: String, image: String, executor: String, entrypoint: String,
                   artifact: Option[Path], filename: Option[String], ports: JsonNode, configuration: JsonNode): ResolvedOperator = {
    val target = filename.map { name =>
      require(name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*"), "invalid artifact filename")
      "/operator/" + name
    }
    require(configuration.isObject, "executor_config must be a mapping")
    val settings = Data.mapper.convertValue(configuration,classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap
    dacp.operator.OperatorPresentation.validate(ports)
    dacp.operator.ParameterSchema.validate(ports.get("parameters"))
    require(!ports.has("outputs"),"output declarations belong in inferOutputs, not the manifest")
    val inputs = OperatorPorts.read(ports.path("inputs")); val outputs = Map.empty[String,PortContract]
    val program=OperatorProgram(reference,entrypoint,artifact,settings,
      inputs.map { case(k,v)=>k->v.representations },outputs.map { case(k,v)=>k->v.representations })
    val command=config.plugins.byId(classOf[OperatorExecutor],executor).command(program,target)
    ResolvedOperator(image,artifact,target,command,inputs,outputs,executor,Some(program))
  }
  def resolve(config: OperatorEnvironment, reference: String, docker: DockerClient): ResolvedOperator = synchronized {
    val p = reference.split("@",-1)
    require(p.length == 2 && p.forall(_.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")),"operator must be name@version")
    val local = config.localOperatorLibrary.resolve(p(0)).resolve(p(1)).resolve("operator.yaml")
    if (Files.exists(local)) {
      val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
      val in = Files.newInputStream(local)
      val m = try new Yaml(options).load[java.util.Map[String,Object]](in).asScala.toMap finally in.close()
      require(m.keySet.subsetOf(Set("name", "version", "image", "description", "icon", "executor", "entrypoint", "artifact", "inputs", "parameters", "executor_config", "provenance")), "unknown operator setting")
      dacp.operator.OperatorPresentation.validate(Data.mapper.valueToTree[JsonNode](m.asJava))
      dacp.operator.ParameterSchema.validate(Data.mapper.valueToTree[JsonNode](m.getOrElse("parameters",null)))
      require(m("name").toString == p(0) && m("version").toString == p(1), "operator identity mismatch")
      val artifact = m.get("artifact").map(v => local.getParent.resolve(v.toString).toAbsolutePath.normalize)
      artifact.foreach(f => require(Files.isRegularFile(f) && Files.size(f) > 0 && Files.size(f) <= config.operatorMaxBytes, "invalid operator artifact size"))
      try {
        val image = docker.inspectImageCmd(m("image").toString).exec().getId
        return plan(config,reference, image, m.getOrElse("executor", "container").toString, m.getOrElse("entrypoint", "").toString,
          artifact, artifact.map(_.getFileName.toString), Data.mapper.valueToTree[JsonNode](m.asJava), Data.mapper.valueToTree[JsonNode](m.getOrElse("executor_config",new java.util.LinkedHashMap[String,Object]())))
      } catch { case _: com.github.dockerjava.api.exception.NotFoundException => () }
    }
    def remote(base: String): ResolvedOperator = {
      val parent = config.operatorCache.resolve("manifests").resolve(sha(base.getBytes(java.nio.charset.StandardCharsets.UTF_8))).resolve(p(0))
      locked(parent.resolve(p(1)+".lock")) {
        val entry = parent.resolve(p(1)+".json")
        def metadata(file: Path): JsonNode = {
          val m = Data.mapper.readTree(file.toFile)
          require(m.path("api_version").asInt == 2 && m.path("name").asText == p(0) && m.path("version").asText == p(1),"operator identity mismatch or unsupported manifest")
          dacp.operator.OperatorPresentation.validate(m)
          dacp.operator.ParameterSchema.validate(m.get("parameters"))
          require(m.path("runtime").path("image").asText.matches("sha256:[a-f0-9]{64}"), "operator runtime must be pinned by ID")
          m
        }
        def blob(a: JsonNode): Path = {
          val hash = a.path("sha256").asText; val size = a.path("size").asLong(-1)
          require(hash.matches("[a-f0-9]{64}") && size > 0 && size <= config.operatorMaxBytes, "invalid operator artifact size/digest")
          val path = config.operatorCache.resolve("blobs/sha256").resolve(hash)
          locked(path.resolveSibling(hash + ".lock")) {
            def validate(file: Path): Unit = require(Files.size(file) == size && digest(file) == hash, "operator artifact checksum/size mismatch")
            if (!Files.exists(path)) {
              val temporary = Files.createTempFile(path.getParent, "download-", ".tmp")
              try {
                fetch(base.stripSuffix("/")+"/v1/blobs/sha256/"+hash, temporary, size)
                validate(temporary)
                Files.move(temporary,path,StandardCopyOption.ATOMIC_MOVE)
              } finally Files.deleteIfExists(temporary)
            } else validate(path)
            path
          }
        }
        def prepare(m: JsonNode): ResolvedOperator = {
          val runtime = m.path("runtime")
          val archive = blob(runtime.path("archive"))
          val artifact = Option(m.get("artifact")).map(blob)
          val execution = m.path("execution")
          val image = runtime.path("image").asText
          val resolved = plan(config,reference, image, execution.path("executor").asText, execution.path("entrypoint").asText,
            artifact, Option(m.get("artifact")).map(_.path("filename").asText), m, Option(execution.get("config")).getOrElse(Data.mapper.createObjectNode()))
          try docker.inspectImageCmd(image).exec() catch {
            case _: com.github.dockerjava.api.exception.NotFoundException =>
              val in = Files.newInputStream(archive)
              try docker.loadImageCmd(in).exec() finally in.close()
              docker.inspectImageCmd(image).exec()
          }
          resolved
        }
        if (Files.exists(entry)) prepare(metadata(entry))
        else {
          val temporary = Files.createTempFile(parent,"download-", ".json")
          try {
            fetch(base.stripSuffix("/")+s"/v1/operators/${p(0)}/${p(1)}",temporary,65536,manifest=true)
            val result = prepare(metadata(temporary))
            Files.move(temporary,entry,StandardCopyOption.ATOMIC_MOVE)
            result
          } finally Files.deleteIfExists(temporary)
        }
      }
    }
    val bases = config.remoteOperatorLibraryUrls
    val (cached, uncached) = bases.partition { base =>
      Files.exists(config.operatorCache.resolve("manifests").resolve(sha(base.getBytes(java.nio.charset.StandardCharsets.UTF_8))).resolve(p(0)).resolve(p(1)+".json"))
    }
    (cached ++ uncached).foreach { base =>
      try return remote(base) catch { case _: MissingOperator => () }
    }
    throw new IllegalArgumentException(s"operator not found locally or in configured libraries: $reference")
  }
}
