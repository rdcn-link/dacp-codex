package xstream.data

import java.nio.file.{Files, Path}
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

final class Formats(environment: PluginEnvironment) {
  private class ContainerFormat(val id: String, val extensions: Set[String]) extends FormatHandler
  // Additional Java/Scala providers use META-INF/services/xstream.data.FormatHandler; restart to load.
  private lazy val handlers: Seq[FormatHandler] = {
    val all = environment.containers.registered.map(r => new ContainerFormat(r.id,r.extensions)) ++ environment.services(classOf[FormatHandler])
    require(all.forall(h => h.id.matches("[a-z][a-z0-9-]*") && h.id != "sql"), "invalid or reserved format id")
    require(all.forall(_.extensions.forall(_.matches("[a-z0-9]+"))), "invalid format extension")
    require(all.map(_.id).distinct.size == all.size, "duplicate format handler id")
    require(all.flatMap(_.extensions).distinct.size == all.flatMap(_.extensions).size, "duplicate format extension")
    all
  }
  def initialize(): Unit = { handlers; () }
  def byId(id: String): FormatHandler = handlers.find(_.id == id)
    .getOrElse(throw new IllegalArgumentException("unsupported configured format"))
  def views(id: String): Seq[String] = if (byId(id).records) Seq("records","bytes") else Seq("bytes")
  def extensionFormat(name: String): FormatHandler = {
    val ext = name.split("\\.").last.toLowerCase(java.util.Locale.ROOT)
    environment.containers.formatForName(name).map(byId).getOrElse(handlers.find(_.extensions(ext)).getOrElse(byId("bin")))
  }
  def detect(path: ByteSource, options: FileOptions = FileOptions()): FormatHandler = {
    val handler = options.format match {
      case Some(id) => byId(id)
      case None =>
        val in = path.open(()=>())
        val signature = try in.readNBytes(8) finally in.close()
        val ext = path.name.split("\\.").last.toLowerCase(java.util.Locale.ROOT)
        environment.containers.formatForName(path.name,compoundOnly=true).map(byId).orElse(handlers.find(_.signatureMatches(signature)))
          .orElse(handlers.find(_.matches(signature,ext))).getOrElse(byId("bin"))
    }
    handler.validateOptions(options)
    require(handler.records || options.columns.isEmpty, "schema requires a records format")
    handler
  }
}
