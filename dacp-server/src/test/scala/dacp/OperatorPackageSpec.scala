package dacp

import java.nio.file.{Files,Path}
import java.util.zip.{ZipEntry,ZipOutputStream}
import scala.jdk.CollectionConverters._
import scala.sys.process._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

/** Opt-in real container tests: build example artifacts/runtimes, then set DACP_TEST_OPERATOR_PACKAGES=1. */
class OperatorPackageSpec extends AnyFunSuite {
  private def run(executor: String, image: String, file: Path, entrypoint: String): Unit = {
    assume(sys.env.get("DACP_TEST_OPERATOR_PACKAGES").contains("1"), "requires built runtime images, example JAR, and Docker")
    val base = Files.createTempDirectory("operator-package-")
    val allocator = new RootAllocator(64L * 1024 * 1024)
    val payload = "id,value\n1,example\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    try {
      val input = Files.createDirectory(base.resolve("input"))
      val root = VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
      try {
        val v = root.getVector(0).asInstanceOf[VarBinaryVector]; v.allocateNew(); v.setSafe(0,payload); v.setValueCount(1); root.setRowCount(1)
        val writer = new ArrowStreamWriter(root,null,Files.newOutputStream(input.resolve("0.arrow")))
        try { writer.start(); writer.writeBatch(); writer.end() } finally writer.close()
      } finally root.close()
      Files.writeString(input.resolve("manifest.json"),"""[{"file":"0.arrow","port":"source","representation":"bytes","format":"csv"}]""")
      Files.writeString(input.resolve("params.json"),"{}")
      val target = "/operator/"+file.getFileName.toString
      val launch = OperatorExecutors.command(executor,OperatorProgram("example@1",entrypoint,Some(file)),Some(target))
      val command = Seq("docker","run","--rm","--network","none","--read-only","--tmpfs","/tmp:rw,size=512m",
        "-v",s"$input:/input:ro","-v",s"${file.toAbsolutePath}:$target:ro") ++
        launch.environment.toSeq.flatMap { case (k,v) => Seq("-e",s"$k=$v") } ++
        Seq("--entrypoint",launch.entrypoint.get.head,image) ++ launch.entrypoint.get.tail ++ launch.arguments
      val output=new java.io.ByteArrayOutputStream()
      val process=Process(command).run(new ProcessIO(in=>in.close(),out=>{try out.transferTo(output) finally out.close()},err=>{try err.transferTo(System.err) finally err.close()}))
      assert(process.exitValue()==0)
      val stream=new java.io.ByteArrayInputStream(output.toByteArray)
      val result=new java.io.ByteArrayOutputStream()
      var frame=OperatorFrames.read(stream);var ended=false
      while(frame.nonEmpty) {
        val header=Data.mapper.readTree(frame.get.metadata)
        if(header.path("type").asText=="data")result.write(frame.get.data)
        if(header.path("type").asText=="end")ended=true
        frame=OperatorFrames.read(stream)
      }
      assert(ended && result.toByteArray.sameElements(payload))
    } finally { allocator.close(); Data.deleteTree(base) }
  }
  test("independently compiled thin Scala JAR runs with shared dependencies") {
    run("jvm","dacp/operator-jvm:1.1",Path.of("../examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar"),"example.ScalaIdentity")
  }
  test("Python source runs in a shared Arrow environment") {
    run("python","dacp/operator-python:1.0",Path.of("../examples/operators/python/identity_python/1.0/identity.py"),"identity")
  }
  test("Python wheel installs offline without dependencies and executes its selected module") {
    assume(sys.env.get("DACP_TEST_OPERATOR_PACKAGES").contains("1"), "requires a Python runtime image and Docker")
    val base = Files.createTempDirectory("operator-wheel-")
    try {
      val wheel = base.resolve("example_ops-1.0-py3-none-any.whl")
      val zip = new ZipOutputStream(Files.newOutputStream(wheel))
      try {
        val entries = Map(
          "identity.py" -> Files.readString(Path.of("../examples/operators/python/identity_python/1.0/identity.py")),
          "example_ops-1.0.dist-info/METADATA" -> "Metadata-Version: 2.1\nName: example-ops\nVersion: 1.0\n",
          "example_ops-1.0.dist-info/WHEEL" -> "Wheel-Version: 1.0\nGenerator: dacp-contract-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
          "example_ops-1.0.dist-info/RECORD" -> "")
        entries.foreach { case (name,text) => zip.putNextEntry(new ZipEntry(name)); zip.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)); zip.closeEntry() }
      } finally zip.close()
      run("python","dacp/operator-python:1.0",wheel,"identity")
    } finally Data.deleteTree(base)
  }
}
