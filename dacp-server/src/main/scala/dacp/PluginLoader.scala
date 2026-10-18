package dacp

import java.net.URLClassLoader
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._

/** One installation per server process; plugins and their dependencies share the server API. */
object PluginLoader {
  private var installed: Option[(Path,URLClassLoader)] = None
  def classLoader: ClassLoader = installed.map(_._2).getOrElse(Thread.currentThread().getContextClassLoader)
  def initialize(directory: Option[Path]): Unit = synchronized {
    directory.foreach { raw =>
      val path = raw.toRealPath()
      require(Files.isDirectory(path),"plugin_directory must be a directory")
      installed match {
        case Some((existing,_)) => require(existing == path,"plugin_directory cannot change without restart")
        case None =>
          val in = Files.list(path)
          val jars = try in.iterator().asScala.filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(".jar"))
            .toVector.sortBy(_.toString) finally in.close()
          val loader = new URLClassLoader(jars.map(_.toUri.toURL).toArray,classLoader)
          installed = Some(path -> loader)
          Thread.currentThread().setContextClassLoader(loader)
      }
    }
  }
  def close(): Unit = synchronized { installed.foreach(_._2.close()) }
}
