package dacp.repository

import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption, LinkOption}
import java.nio.channels.FileChannel
import java.security.MessageDigest
import com.fasterxml.jackson.databind.ObjectMapper
import scala.sys.process._
import scala.jdk.CollectionConverters._

/** Immutable operator artifacts, published by an administrator and served read-only. */
object Repository {
  val mapper = new ObjectMapper()
  def parts(reference: String): (String, String) = {
    val p = reference.split("@", -1)
    require(p.length == 2 && p.forall(_.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")),
      "operator must be name@version")
    (p(0), p(1))
  }
  def checksum(path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val in = Files.newInputStream(path)
    try {
      val buffer = new Array[Byte](1024 * 1024)
      var n = in.read(buffer)
      while (n != -1) { digest.update(buffer, 0, n); n = in.read(buffer) }
    } finally in.close()
    digest.digest().map(b => f"${b & 255}%02x").mkString
  }
  private def manifests(root: Path, filename: String): Seq[Path] = {
    require(Files.isDirectory(root), s"operator directory does not exist: $root")
    val paths = Files.walk(root, 3)
    try paths.iterator.asScala.filter { file =>
      val relative = root.relativize(file)
      relative.getNameCount == 3 && file.getFileName.toString == filename &&
        Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) &&
        relative.getName(0).toString.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*") &&
        relative.getName(1).toString.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")
    }.toSeq.sortBy(_.toString) finally paths.close()
  }
  def blob(root: Path, sha256: String): Path = {
    require(sha256.matches("[a-f0-9]{64}"), "invalid artifact digest")
    root.resolve("blobs/sha256").resolve(sha256)
  }
  private def store(root: Path, source: Path, filename: String): com.fasterxml.jackson.databind.node.ObjectNode = {
    require(Files.isRegularFile(source) && Files.size(source) > 0, "empty or missing artifact")
    val hash = checksum(source)
    val target = blob(root, hash)
    Files.createDirectories(target.getParent)
    if (Files.exists(target)) require(checksum(target) == hash, "stored artifact checksum mismatch")
    else {
      val temporary = Files.createTempFile(target.getParent, ".blob-", ".tmp")
      try { Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING); require(checksum(temporary) == hash, "artifact changed during publication"); Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE) }
      finally Files.deleteIfExists(temporary)
    }
    mapper.createObjectNode().put("sha256", hash).put("size", Files.size(source)).put("filename", filename)
  }
  def validatePorts(ports: com.fasterxml.jackson.databind.JsonNode): Unit = {
    Seq("inputs","outputs").foreach { key=>
      val n=ports.path(key);require(n.isObject && n.size()>0,"named input/output port declarations required")
      n.fields.asScala.foreach { e=>
        require(e.getKey.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"invalid port name")
        val v=e.getValue
        require(v.isObject && v.fieldNames.asScala.toSet.subsetOf(Set("representations","formats","schema")),"invalid port settings")
        require(v.path("representations").isArray && v.path("representations").size()>0 && v.path("formats").isArray,"invalid port representations/formats")
        require(v.path("representations").elements.asScala.forall(x=>Set("bytes","records")(x.asText)),"unsupported representation")
      }
    }
  }
  def validate(root: Path, manifest: com.fasterxml.jackson.databind.JsonNode): Unit = {
    validatePorts(manifest)
    require(manifest.path("api_version").asInt == 2, "unsupported operator manifest")
    parts(s"${manifest.path("name").asText}@${manifest.path("version").asText}")
    require(manifest.path("runtime").path("image").asText.matches("sha256:[a-f0-9]{64}"), "runtime image must be pinned")
    val descriptors = Seq(manifest.path("runtime").path("archive")) ++ Option(manifest.get("artifact"))
    descriptors.foreach { a =>
      val file = blob(root, a.path("sha256").asText)
      require(a.path("size").asLong > 0 && Files.size(file) == a.path("size").asLong && checksum(file) == a.path("sha256").asText,
        "artifact checksum/size mismatch")
    }
  }
  /** Listing reads metadata only; full verification is performed on publication and seeding. */
  def catalog(root: Path): com.fasterxml.jackson.databind.node.ArrayNode = {
    val result = mapper.createArrayNode()
    manifests(root, "manifest.json").foreach { file =>
      val manifest = mapper.readTree(file.toFile)
      require(manifest.path("api_version").asInt == 2, "unsupported operator manifest; rebuild the development catalog")
      require(manifest.path("name").asText == file.getParent.getParent.getFileName.toString &&
        manifest.path("version").asText == file.getParent.getFileName.toString, "operator identity mismatch")
      result.add(manifest)
    }
    result
  }
  /** Discover definitions; artifact paths are relative to operator.yaml. */
  def seed(root: Path, directory: Path, exporter: ImageExporter = Docker): Seq[String] = {
    val definitions = manifests(directory, "operator.yaml")
    require(definitions.nonEmpty, "no operator definitions found")
    definitions.map { file =>
      val options = new org.yaml.snakeyaml.LoaderOptions(); options.setAllowDuplicateKeys(false)
      val in = Files.newInputStream(file)
      val d = try new org.yaml.snakeyaml.Yaml(options).load[java.util.Map[String, Object]](in).asScala.toMap finally in.close()
      require(Set("name", "version", "image", "inputs", "outputs").subsetOf(d.keySet) &&
        d.keySet.subsetOf(Set("name", "version", "image", "description", "executor", "entrypoint", "artifact", "inputs", "outputs")), "invalid operator definition")
      val reference = s"${d("name")}@${d("version")}"
      val (name, version) = parts(reference)
      require(name == file.getParent.getParent.getFileName.toString && version == file.getParent.getFileName.toString,
        "operator definition must match its directory")
      val existing = root.resolve(name).resolve(version).resolve("manifest.json")
      if (Files.exists(existing)) validate(root, mapper.readTree(existing.toFile))
      else publish(root, reference, d("image").toString, mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](d.asJava), exporter, d.getOrElse("description", "").toString,
        d.getOrElse("executor", "container").toString, d.getOrElse("entrypoint", "").toString,
        d.get("artifact").map(v => file.getParent.resolve(v.toString)))
      reference
    }
  }
  trait ImageExporter {
    def inspect(image: String): String
    def save(imageId: String, destination: Path): Unit
  }
  object Docker extends ImageExporter {
    def inspect(image: String): String = Process(Seq("docker", "image", "inspect", "--format", "{{.Id}}", image)).!!.trim
    def save(imageId: String, destination: Path): Unit =
      require(Process(Seq("docker", "save", "-o", destination.toString, imageId)).! == 0, "docker save failed")
  }
  def publish(root: Path, reference: String, image: String, ports: com.fasterxml.jackson.databind.JsonNode, exporter: ImageExporter = Docker, description: String = "",
              executor: String = "container", entrypoint: String = "", artifact: Option[Path] = None): String = synchronized {
    validatePorts(ports)
    val (name, version) = parts(reference)
    require(executor.matches("[a-z][a-z0-9_-]*"), "invalid executor ID")
    require(entrypoint.length <= 1024 && !entrypoint.contains("\u0000"), "invalid entrypoint")
    Files.createDirectories(root)
    // One publication lock covers shared blobs and the image-ID export index across processes.
    val channel = FileChannel.open(root.resolve(".publish.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    val lock = channel.lock()
    try {
      val parent = root.resolve(name); Files.createDirectories(parent)
      val target = parent.resolve(version)
      require(!Files.exists(target), "operator version already published")
      val stage = Files.createTempDirectory(parent, ".publish-")
      try {
        val id = exporter.inspect(image)
        require(id.matches("sha256:[a-f0-9]{64}"), "invalid Docker image ID")
        val index = root.resolve("images").resolve(id.stripPrefix("sha256:") + ".json")
        val archive = if (Files.exists(index)) {
          val a = mapper.readTree(index.toFile)
          val path = blob(root, a.path("sha256").asText)
          require(Files.size(path) == a.path("size").asLong && checksum(path) == a.path("sha256").asText, "runtime archive checksum mismatch")
          a
        } else {
          val exported = stage.resolve("runtime.tar"); exporter.save(id, exported)
          val a = store(root, exported, "runtime.tar")
          Files.delete(exported)
          Files.createDirectories(index.getParent)
          val temporary = Files.createTempFile(index.getParent, ".image-", ".json")
          try { mapper.writeValue(temporary.toFile, a); Files.move(temporary, index, StandardCopyOption.ATOMIC_MOVE) }
          finally Files.deleteIfExists(temporary)
          a
        }
        val manifest = mapper.createObjectNode()
        manifest.put("api_version", 2).put("name", name).put("version", version).put("description", description)
        manifest.set[com.fasterxml.jackson.databind.JsonNode]("inputs",ports.path("inputs"))
        manifest.set[com.fasterxml.jackson.databind.JsonNode]("outputs",ports.path("outputs"))
        manifest.putObject("execution").put("executor", executor).put("entrypoint", entrypoint)
        val runtime = manifest.putObject("runtime"); runtime.put("image", id); runtime.set[com.fasterxml.jackson.databind.JsonNode]("archive", archive)
        artifact.foreach { source =>
          val filename = source.getFileName.toString
          require(filename.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*"), "invalid artifact filename")
          manifest.set[com.fasterxml.jackson.databind.JsonNode]("artifact", store(root, source, filename))
        }
        val json = mapper.writeValueAsString(manifest)
        Files.writeString(stage.resolve("manifest.json"), json)
        Files.move(stage, target, StandardCopyOption.ATOMIC_MOVE)
        json
      } finally if (Files.exists(stage)) {
        val paths = Files.walk(stage)
        try paths.iterator.asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.delete(_)) finally paths.close()
      }
    } finally { lock.release(); channel.close() }
  }
}
