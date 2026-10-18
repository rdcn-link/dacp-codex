package dacp

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

object OperatorExecutors {
  def command(id: String, program: OperatorProgram, artifactPath: Option[String]): OperatorCommand = {
    val all = ServiceLoader.load(classOf[OperatorExecutor], PluginLoader.classLoader).iterator.asScala.toVector
    require(all.map(_.id).distinct.size == all.size, "duplicate operator executor ID")
    all.find(_.id == id).getOrElse(throw new IllegalArgumentException(s"unknown operator executor: $id"))
      .command(program, artifactPath)
  }
}
