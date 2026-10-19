package dacp.repository

import java.nio.file.{Files, Path}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext}

object Main {
  private val usage = "publish name@version --image IMAGE --ports FILE --root DIR [--description TEXT] [--executor ID --entrypoint MAIN --artifact FILE --executor-config FILE] [--source-archive FILE --source-repository URL --source-revision COMMIT --license SPDX --license-file FILE --build-instructions FILE --sbom FILE] | seed --operators DIR --root DIR | serve --root DIR [--host HOST] [--port PORT] [--tls-keystore PKCS12 --tls-password-env ENV] [--publish-token-env ENV --publish-max-bytes BYTES]"
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, usage)
    val publish = args(0) == "publish"
    val seed = args(0) == "seed"
    require(publish || seed || args(0) == "serve", usage)
    require(!publish || args.length >= 2, usage)
    val options = args.drop(if (publish) 2 else 1)
    require(options.length % 2 == 0, usage)
    val pairs = options.grouped(2).map(v => v(0) -> v(1)).toSeq
    val allowed = if (publish) Set("--root", "--image", "--description", "--executor", "--entrypoint", "--artifact", "--ports", "--executor-config", "--source-archive", "--source-repository", "--source-revision", "--license", "--license-file", "--build-instructions", "--sbom") else if (seed) Set("--root", "--operators") else Set("--root", "--host", "--port", "--tls-keystore", "--tls-password-env", "--publish-token-env", "--publish-max-bytes")
    require(pairs.map(_._1).distinct.size == pairs.size && pairs.forall(p => allowed(p._1)), "unknown or duplicate option")
    val settings = pairs.toMap
    val root = Path.of(settings.getOrElse("--root", throw new IllegalArgumentException("--root required")))
    if (publish) {
      val file=Path.of(settings.getOrElse("--ports",throw new IllegalArgumentException("--ports required")))
      val options=new org.yaml.snakeyaml.LoaderOptions();options.setAllowDuplicateKeys(false)
      val ports=Repository.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](new org.yaml.snakeyaml.Yaml(options).load[Object](Files.readString(file)))
      val configuration=settings.get("--executor-config").map { name =>
        Repository.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](new org.yaml.snakeyaml.Yaml(options).load[Object](Files.readString(Path.of(name))))
      }.getOrElse(Repository.mapper.createObjectNode())
      val build=settings.get("--build-instructions").map { name=>
        val file=Path.of(name);require(Files.size(file)<=32768,"build instructions exceed 32 KiB");Files.readString(file)
      }
      val provenance=Provenance(settings.get("--source-archive").map(Path.of(_)),settings.get("--source-repository"),
        settings.get("--source-revision"),settings.get("--license"),settings.get("--license-file").map(Path.of(_)),build,
        settings.get("--sbom").map(Path.of(_)))
      println(Repository.publish(root, args(1), settings.getOrElse("--image", throw new IllegalArgumentException("--image required")), ports, description = settings.getOrElse("--description", ""), executor = settings.getOrElse("--executor", "container"), entrypoint = settings.getOrElse("--entrypoint", ""), artifact = settings.get("--artifact").map(Path.of(_)), configuration = configuration, provenance = provenance))
    } else if (seed) Repository.seed(root, Path.of(settings.getOrElse("--operators", throw new IllegalArgumentException("--operators required")))).foreach(ref => println(s"Available: $ref"))
    else {
      require(settings.contains("--tls-keystore") == settings.contains("--tls-password-env"), "TLS requires both keystore and password environment name")
      val tls = settings.get("--tls-keystore").map { file =>
        val password = sys.env.getOrElse(settings("--tls-password-env"), throw new IllegalArgumentException("missing TLS password environment variable")).toCharArray
        try {
          val store = KeyStore.getInstance("PKCS12")
          val in = Files.newInputStream(Path.of(file))
          try store.load(in, password) finally in.close()
          val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
          keys.init(store, password)
          val context = SSLContext.getInstance("TLS"); context.init(keys.getKeyManagers, null, null); context
        } finally java.util.Arrays.fill(password, '\u0000')
      }
      require(!settings.contains("--publish-max-bytes") || settings.contains("--publish-token-env"), "publish byte limit requires publisher authentication")
      val publishToken = settings.get("--publish-token-env").map { name =>
        sys.env.getOrElse(name, throw new IllegalArgumentException("missing publish token environment variable"))
      }
      val server = new RepositoryServer(root, settings.getOrElse("--host", "127.0.0.1"), settings.getOrElse("--port", "8820").toInt, tls, publishToken, settings.getOrElse("--publish-max-bytes", "8589934592").toLong)
      Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
      server.start()
      println(s"Operator repository listening on ${server.boundPort}")
    }
  }
}
