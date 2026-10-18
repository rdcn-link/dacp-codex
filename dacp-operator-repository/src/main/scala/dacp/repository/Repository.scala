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
  /** Catalog reads metadata only; it never scans image archives for checksums. */
  def catalog(root: Path): com.fasterxml.jackson.databind.node.ArrayNode = {
    val result = mapper.createArrayNode()
    manifests(root, "manifest.json").foreach { file =>
      if (Files.isRegularFile(file.getParent.resolve("image.tar"), LinkOption.NOFOLLOW_LINKS)) {
        val manifest = mapper.readTree(file.toFile)
        require(manifest.path("name").asText == file.getParent.getParent.getFileName.toString &&
          manifest.path("version").asText == file.getParent.getFileName.toString, "operator identity mismatch")
        result.add(manifest)
      }
    }
    result
  }
  /** Discover contributions from <directory>/<name>/<version>/operator.yaml. */
  def seed(root: Path, directory: Path, exporter: ImageExporter = Docker): Seq[String] = {
    val definitions = manifests(directory, "operator.yaml").map { file =>
      val options = new org.yaml.snakeyaml.LoaderOptions()
      options.setAllowDuplicateKeys(false)
      val in = Files.newInputStream(file)
      val definition = try new org.yaml.snakeyaml.Yaml(options).load[java.util.Map[String, Object]](in) finally in.close()
      require(definition != null && Set("name", "version", "image").subsetOf(definition.keySet.asScala.toSet) && definition.keySet.asScala.toSet.subsetOf(Set("name", "version", "image", "description")), "operator definition requires name, version and image")
      val reference = s"${definition.get("name")}@${definition.get("version")}"
      val (name, version) = parts(reference)
      require(name == file.getParent.getParent.getFileName.toString && version == file.getParent.getFileName.toString,
        "operator definition must match its directory")
      val image = definition.get("image").toString
      require(image.nonEmpty, "operator image required")
      val description = Option(definition.get("description")).map(_.toString).getOrElse("")
      (reference, name, version, image, description)
    }
    require(definitions.nonEmpty, "no operator definitions found")
    definitions.map { case (reference, name, version, image, description) =>
      val existing = root.resolve(name).resolve(version)
      if (Files.exists(existing)) {
        val manifest = mapper.readTree(existing.resolve("manifest.json").toFile)
        val archive = existing.resolve("image.tar")
        require(manifest.path("api_version").asInt == 1 && manifest.path("name").asText == name &&
          manifest.path("version").asText == version && manifest.path("image").asText.matches("sha256:[a-f0-9]{64}") &&
          Files.size(archive) > 0 && Files.size(archive) == manifest.path("archive").path("size").asLong &&
          checksum(archive) == manifest.path("archive").path("sha256").asText, "existing operator archive is invalid")
      } else publish(root, reference, image, exporter, description)
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
  def publish(root: Path, reference: String, image: String, exporter: ImageExporter = Docker, description: String = ""): String = {
    val (name, version) = parts(reference)
    val parent = root.toAbsolutePath.normalize.resolve(name)
    Files.createDirectories(parent)
    val channel = FileChannel.open(parent.resolve(s".$version.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    val lock = channel.lock()
    try {
      val target = parent.resolve(version)
      require(!Files.exists(target), "operator version already published")
      val stage = Files.createTempDirectory(parent, ".publish-")
      try {
        val id = exporter.inspect(image)
        require(id.matches("sha256:[a-f0-9]{64}"), "invalid Docker image ID")
        val archive = stage.resolve("image.tar")
        exporter.save(id, archive)
        require(Files.size(archive) > 0, "empty Docker archive")
        val manifest = mapper.createObjectNode()
        manifest.put("api_version", 1).put("name", name).put("version", version).put("image", id)
        manifest.put("description", description)
        manifest.putObject("archive").put("size", Files.size(archive)).put("sha256", checksum(archive))
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
