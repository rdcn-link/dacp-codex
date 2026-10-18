package dacp

import java.nio.file.Path

/** Trusted launch plugins select commands; the server retains container lifecycle and resource control.
  * The artifact is verified and mounted read-only at artifactPath. Plugins never load operator code in the server JVM.
  */
case class OperatorProgram(reference: String, entrypoint: String, artifact: Option[Path],
                           configuration: Map[String,Object] = Map.empty,
                           inputs: Map[String,Set[String]] = Map.empty,
                           outputs: Map[String,Set[String]] = Map.empty)
/** executableScratch requests a bounded executable tmpfs at /operator-work; /tmp remains non-executable. */
case class OperatorCommand(entrypoint: Option[Vector[String]] = None,
                           arguments: Vector[String] = Vector("operator", "/input", "/input/params.json"),
                           environment: Map[String, String] = Map.empty,
                           executableScratch: Boolean = false)
trait OperatorExecutor {
  /** Source compilers/launchers execute exclusively inside the isolated runtime container. */
  def sourceCommand(program: OperatorProgram, artifactPath: String): OperatorCommand =
    throw new IllegalArgumentException(s"executor $id does not support source")
  def id: String
  def command(program: OperatorProgram, artifactPath: Option[String]): OperatorCommand
}
