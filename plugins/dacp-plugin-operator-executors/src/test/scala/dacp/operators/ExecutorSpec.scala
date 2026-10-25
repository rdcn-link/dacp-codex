package dacp.operators

import java.nio.file.Path
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite
import dacp.{OperatorExecutor,OperatorProgram}

class ExecutorSpec extends AnyFunSuite {
  test("all built-in executors use the external service registration contract") {
    val found = ServiceLoader.load(classOf[OperatorExecutor]).iterator.asScala.map(_.id).toSet
    assert(found == Set("container","jvm","python","command"))
  }
  test("JAR launch selects the main class and shared dependencies without loading code") {
    val p = OperatorProgram("sum@1","example.Sum",Some(Path.of("/cache/digest")))
    val c = new JvmExecutor().command(p,Some("/operator/ops.jar"))
    assert(c.entrypoint.get.takeRight(3) == Vector("-cp","/operator/ops.jar:/opt/lib/*","example.Sum"))
    assert(c.arguments == Vector("operator","/input","/input/params.json"))
    intercept[IllegalArgumentException](new JvmExecutor().command(p.copy(entrypoint="bad main"),Some("/operator/ops.jar")))
    intercept[IllegalArgumentException](new JvmExecutor().command(p,Some("/operator/ops.py")))
  }
  test("Python launch supports source and offline wheels with runtime-owned dependencies") {
    val p = OperatorProgram("sum@1","example.sum",Some(Path.of("/cache/digest")))
    val c = new PythonExecutor().command(p,Some("/operator/example-1-py3-none-any.whl"))
    assert(c.entrypoint.get.last == "example.sum")
    assert(c.entrypoint.get(2).contains("'--no-index','--no-deps'"))
    assert(c.environment("PYTHONDONTWRITEBYTECODE") == "1")
    new PythonExecutor().command(p,Some("/operator/sum.py"))
    intercept[IllegalArgumentException](new PythonExecutor().command(p,Some("/operator/ops.jar")))
  }
  test("container launch preserves image entrypoint and rejects unused artifact configuration") {
    val p = OperatorProgram("copy@1","",None)
    assert(new ContainerExecutor().command(p,None).entrypoint.isEmpty)
    intercept[IllegalArgumentException](new ContainerExecutor().command(p.copy(entrypoint="main"),None))
  }
  test("source plugins compile/launch inside the configured runtime without a shell") {
    val p=OperatorProgram("scala@1.0","example.Main",Some(Path.of("source")))
    val c=new JvmExecutor().sourceCommand(p,"/operator/operator.source")
    assert(c.entrypoint.get.head=="/opt/java/bin/java")
    assert(c.entrypoint.get.contains("scala.tools.nsc.MainGenericRunner"))
    assert(c.entrypoint.get.exists(_.contains("scala.tools.nsc.Main.process")))
    assert(c.arguments==Vector("operator","/input","/input/params.json"))
    intercept[IllegalArgumentException](new ContainerExecutor().sourceCommand(p,"/operator/operator.source"))
  }

}
