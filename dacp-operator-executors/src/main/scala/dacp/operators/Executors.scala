package dacp.operators

import dacp.{OperatorExecutor, OperatorProgram, OperatorCommand}

final class ContainerExecutor extends OperatorExecutor {
  val id = "container"
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(p.artifact.isEmpty && p.entrypoint.isEmpty, "container executor uses its image entrypoint; no code artifact")
    OperatorCommand()
  }
}
final class JvmExecutor extends OperatorExecutor {
  val id = "jvm"
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(p.entrypoint.matches("[A-Za-z_$][A-Za-z0-9_.$]*"), "JVM entrypoint must be a main class")
    require(path.exists(_.endsWith(".jar")), "JVM executor requires a JAR artifact")
    OperatorCommand(Some(Vector("/opt/java/bin/java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true", "-cp", path.get + ":/opt/lib/*", p.entrypoint)))
  }
}
final class PythonExecutor extends OperatorExecutor {
  val id = "python"
  // Install the wheel offline into writable ephemeral storage, using dependencies supplied by the runtime.
  // pip validates wheel tags; its failure is fatal. No package index or network dependency resolution occurs.
  private val launcher = """import os,runpy,subprocess,sys
artifact,module=sys.argv[1:3]
args=sys.argv[3:]
if artifact.endswith('.whl'):
    subprocess.run([sys.executable,'-m','pip','install','--no-index','--no-deps','--no-compile','--target','/tmp/dacp-package',artifact],check=True)
    sys.path.insert(0,'/tmp/dacp-package')
    sys.argv=[module]+args
    runpy.run_module(module,run_name='__main__')
else:
    sys.path.insert(0,os.path.dirname(artifact))
    sys.argv=[artifact]+args
    runpy.run_path(artifact,run_name='__main__')
"""
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(path.exists(f => f.endsWith(".whl") || f.endsWith(".py")), "Python executor requires a wheel or Python source artifact")
    require(p.entrypoint.matches("[A-Za-z_][A-Za-z0-9_.]*"), "Python entrypoint must be a module name")
    OperatorCommand(Some(Vector("/usr/local/bin/python", "-c", launcher, path.get, p.entrypoint)),
      environment = Map("PYTHONDONTWRITEBYTECODE" -> "1", "PIP_DISABLE_PIP_VERSION_CHECK" -> "1"))
  }
}
