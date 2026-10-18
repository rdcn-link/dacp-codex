package dacp.library

import java.nio.file.{Files, Path}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext}

object Main {
  private val usage = "publish name@version --image IMAGE --root DIR | seed --operators DIR --root DIR | serve --root DIR [--host HOST] [--port PORT] [--tls-keystore PKCS12 --tls-password-env ENV]"
  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, usage)
    val publish = args(0) == "publish"
    val seed = args(0) == "seed"
    require(publish || seed || args(0) == "serve", usage)
    require(!publish || args.length >= 2, usage)
    val options = args.drop(if (publish) 2 else 1)
    require(options.length % 2 == 0, usage)
    val pairs = options.grouped(2).map(v => v(0) -> v(1)).toSeq
    val allowed = if (publish) Set("--root", "--image") else if (seed) Set("--root", "--operators") else Set("--root", "--host", "--port", "--tls-keystore", "--tls-password-env")
    require(pairs.map(_._1).distinct.size == pairs.size && pairs.forall(p => allowed(p._1)), "unknown or duplicate option")
    val settings = pairs.toMap
    val root = Path.of(settings.getOrElse("--root", throw new IllegalArgumentException("--root required")))
    if (publish) println(Repository.publish(root, args(1), settings.getOrElse("--image", throw new IllegalArgumentException("--image required"))))
    else if (seed) Repository.seed(root, Path.of(settings.getOrElse("--operators", throw new IllegalArgumentException("--operators required")))).foreach(ref => println(s"Available: $ref"))
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
      val server = new LibraryServer(root, settings.getOrElse("--host", "127.0.0.1"), settings.getOrElse("--port", "8820").toInt, tls)
      Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
      server.start()
      println(s"Operator library listening on ${server.boundPort}")
    }
  }
}
