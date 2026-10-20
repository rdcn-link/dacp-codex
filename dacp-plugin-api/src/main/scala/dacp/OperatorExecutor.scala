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
                           executableScratch: Boolean = false,
                           files: Map[String,Array[Byte]] = Map.empty,
                           mounts: Map[String,Path] = Map.empty)
trait OperatorExecutor extends Plugin {
  /** Source compilers/launchers execute exclusively inside the isolated runtime container. */
  def sourceCommand(program: OperatorProgram, artifactPath: String): OperatorCommand =
    throw new IllegalArgumentException(s"executor $id does not support source")
  /** Return None unless every step supports the plugin's in-process contract. */
  def groupCommand(group: OperatorGroup): Option[OperatorCommand] = None
  def id: String
  def command(program: OperatorProgram, artifactPath: Option[String]): OperatorCommand
}

/** A single-node DAG; plugins own language-specific in-process execution. */
case class OperatorGroupBinding(task: Option[String], port: String)
case class OperatorGroupContract(representations:Set[String],formats:Set[String],schema:Option[String]=None)
case class OperatorGroupStep(id:String,program:OperatorProgram,artifactPath:Option[String],
                             params:java.util.Map[String,Object],bindings:Map[String,OperatorGroupBinding],
                             inputs:Map[String,OperatorGroupContract],outputs:Map[String,OperatorGroupContract])
case class OperatorGroup(steps:Vector[OperatorGroupStep],outputs:Map[String,(String,String)])
