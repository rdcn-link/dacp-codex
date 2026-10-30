package xstream.data

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/** Discovery only; archive parsing belongs to ContainerReader plugins. */
final class Containers(environment: PluginEnvironment) {
  private lazy val readers: Seq[ContainerReader] = {
    val all = environment.services(classOf[ContainerReader])
    require(all.forall(_.id.matches("[a-z][a-z0-9-]*")), "invalid container reader id")
    require(all.map(_.id).distinct.size == all.size,"duplicate container reader id")
    all
  }
  def registered: Seq[ContainerReader] = readers
  def supports(id: String): Boolean = readers.exists(_.id == id)
  def byId(id: String): ContainerReader = readers.find(_.id == id)
    .getOrElse(throw new IllegalArgumentException("unsupported container format"))
  def formatForName(name: String, compoundOnly: Boolean = false): Option[String] = {
    val found = readers.filter(_.matchesName(name))
    // Compound names take precedence over ordinary suffixes (tar.gz versus gz).
    val compound = found.filterNot(r => r.extensions(name.split("\\.").last.toLowerCase(java.util.Locale.ROOT)))
    val selected = if (compound.nonEmpty || compoundOnly) compound else found
    require(selected.size <= 1,"ambiguous container filename")
    selected.headOption.map(_.id)
  }
}
