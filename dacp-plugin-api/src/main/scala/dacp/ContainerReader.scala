package dacp

import java.nio.file.Path

case class ContainerEntry(name: String, directory: Boolean, size: Option[Long], modified: Option[Long])

/** Container readers enumerate logical children; format readers interpret each child's content. */
trait ContainerReader extends Plugin {
  def id: String
  def extensions: Set[String]
  def matchesName(name: String): Boolean = extensions(name.split("\\.").last.toLowerCase(java.util.Locale.ROOT))
  def index(path: Path, publicName: String, demand: () => Unit): Seq[ContainerEntry]
  def withFile[A](path: Path, publicName: String, name: String, demand: () => Unit)(f: Path => A): A
}

