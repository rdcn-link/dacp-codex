package dacp

import java.nio.file.Path

/** Trusted launch plugins select commands; the server retains container lifecycle and resource control.
  * The artifact is verified and mounted read-only at artifactPath. Plugins never load operator code in the server JVM.
  */
case class OperatorProgram(reference: String, entrypoint: String, artifact: Option[Path])
case class OperatorCommand(entrypoint: Option[Vector[String]] = None,
                           arguments: Vector[String] = Vector("operator", "/input", "/output", "/input/params.json"),
                           environment: Map[String, String] = Map.empty)
trait OperatorExecutor {
  def id: String
  def command(program: OperatorProgram, artifactPath: Option[String]): OperatorCommand
}
