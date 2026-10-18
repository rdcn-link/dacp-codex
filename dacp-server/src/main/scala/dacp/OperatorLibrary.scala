package dacp

import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import com.github.dockerjava.api.DockerClient
import org.yaml.snakeyaml.Yaml

/** Resolve on the execution node; container isolation and Flight data transport are unchanged. */
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
  private def fetch(url: String, file: Path, limit: Long): Unit = {
    val c = URI.create(url).toURL.openConnection().asInstanceOf[java.net.HttpURLConnection]
    c.setConnectTimeout(15000); c.setReadTimeout(60000); c.setInstanceFollowRedirects(false)
    try {
      if (c.getResponseCode == 404 && !url.endsWith("/image.tar")) throw new MissingOperator
      require(c.getResponseCode == 200, s"operator library HTTP ${c.getResponseCode}: $url")
      val in = c.getInputStream
      try { val out = Files.newOutputStream(file)
        try { val buffer = new Array[Byte](1048576); var total = 0L; var n = in.read(buffer)
          while(n != -1) { total += n; require(total <= limit,"operator download exceeds size limit"); out.write(buffer,0,n); n = in.read(buffer) }
        } finally out.close()
      } finally in.close()
    } finally c.disconnect()
  }
  // Immutable versions: cache hits never silently select a newly published implementation.
  def resolve(config: Config, reference: String, docker: DockerClient): String = synchronized {
    val p = reference.split("@",-1)
    require(p.length == 2 && p.forall(_.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")),"operator must be name@version")
    val local = config.localOperatorLibrary.resolve(p(0)).resolve(p(1)).resolve("operator.yaml")
    if (Files.exists(local)) {
      val in = Files.newInputStream(local)
      val m = try new Yaml().load[java.util.Map[String,Object]](in) finally in.close()
      try return docker.inspectImageCmd(m.get("image").toString).exec().getId
      catch { case _: com.github.dockerjava.api.exception.NotFoundException => () }
    }
    def remote(base: String): String = {
      val parent = config.operatorCache.resolve(sha(base.getBytes(java.nio.charset.StandardCharsets.UTF_8))).resolve(p(0))
      Files.createDirectories(parent)
      val channel = java.nio.channels.FileChannel.open(parent.resolve(p(1)+".lock"),java.nio.file.StandardOpenOption.CREATE,java.nio.file.StandardOpenOption.WRITE)
      val lock = channel.lock()
      try {
        val entry = parent.resolve(p(1))
        def validate(dir: Path): String = {
          val m = Data.mapper.readTree(dir.resolve("manifest.json").toFile)
          require(m.path("api_version").asInt == 1 && m.path("name").asText == p(0) && m.path("version").asText == p(1),"operator identity mismatch")
          val image = m.path("image").asText; require(image.matches("sha256:[a-f0-9]{64}"),"operator image must be pinned by ID")
          val a = m.path("archive"); val size = a.path("size").asLong(-1)
          require(size > 0 && size <= config.operatorMaxBytes,"invalid operator archive size")
          require(Files.size(dir.resolve("image.tar")) == size && digest(dir.resolve("image.tar")) == a.path("sha256").asText,"operator archive checksum/size mismatch")
          image
        }
        def load(dir: Path, image: String): Unit = {
          val in = Files.newInputStream(dir.resolve("image.tar"))
          try docker.loadImageCmd(in).exec() finally in.close()
          docker.inspectImageCmd(image).exec()
        }
        if (!Files.exists(entry)) {
          val stage = Files.createTempDirectory(parent,"download-")
          try {
            val endpoint = base.stripSuffix("/")+s"/v1/operators/${p(0)}/${p(1)}"
            fetch(endpoint,stage.resolve("manifest.json"),65536)
            fetch(endpoint+"/image.tar",stage.resolve("image.tar"),config.operatorMaxBytes)
            load(stage,validate(stage))
            Files.move(stage,entry,StandardCopyOption.ATOMIC_MOVE)
          } finally if (Files.exists(stage)) {
            Files.deleteIfExists(stage.resolve("manifest.json")); Files.deleteIfExists(stage.resolve("image.tar")); Files.delete(stage)
          }
        }
        val image = validate(entry)
        try docker.inspectImageCmd(image).exec() catch { case _: com.github.dockerjava.api.exception.NotFoundException => load(entry,image) }
        image
      } finally { lock.release(); channel.close() }
    }
    // Prefer completed cached versions before making any network requests.
    val bases = config.remoteOperatorLibraryUrls
    val (cached, uncached) = bases.partition { base =>
      Files.exists(config.operatorCache.resolve(sha(base.getBytes(java.nio.charset.StandardCharsets.UTF_8))).resolve(p(0)).resolve(p(1)))
    }
    val ordered = cached ++ uncached
    ordered.foreach { base =>
      try return remote(base) catch { case _: MissingOperator => () }
    }
    throw new IllegalArgumentException(s"operator not found locally or in configured libraries: $reference")
  }
}
