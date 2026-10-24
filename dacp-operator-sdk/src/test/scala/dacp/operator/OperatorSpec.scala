package dacp.operator

import java.io.{ByteArrayInputStream,ByteArrayOutputStream}
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import dacp.OperatorFrames

class OperatorSpec extends AnyFunSuite {
  test("zero-output operators finish without emitting protocol frames") {
    val dir=Files.createTempDirectory("operator-sdk-sink-")
    try {
      Files.writeString(dir.resolve("manifest.json"),"[]");Files.writeString(dir.resolve("params.json"),"{}")
      val bytes=new ByteArrayOutputStream()
      OperatorRunner.run(Array("operator",dir.toString,dir.resolve("params.json").toString),bytes){ctx=>assert(ctx.inputNames.isEmpty)}
      assert(bytes.size()==0)
    }finally {Files.deleteIfExists(dir.resolve("manifest.json"));Files.deleteIfExists(dir.resolve("params.json"));Files.delete(dir)}
  }

  test("bytes template reads Arrow input and applies parameters") {
    val dir=Files.createTempDirectory("operator-sdk-input-")
    val allocator=new org.apache.arrow.memory.RootAllocator()
    val root=org.apache.arrow.vector.VectorSchemaRoot.create(dacp.PluginSchemas.bytes,allocator)
    try {
      val vector=root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
      vector.allocateNew();vector.setSafe(0,"hello".getBytes("UTF-8"));root.setRowCount(1)
      val raw=Files.newOutputStream(dir.resolve("0.arrow"))
      val writer=new org.apache.arrow.vector.ipc.ArrowStreamWriter(root,null,raw)
      try {writer.start();writer.writeBatch();writer.end()}finally {writer.close();raw.close()}
      Files.writeString(dir.resolve("manifest.json"),"""[{"port":"source","file":"0.arrow","representation":"bytes","format":"text"}]""")
      Files.writeString(dir.resolve("params.json"),"""{"suffix":"!"}""")
      val op=new BytesOperator {
        def transform(chunk:Array[Byte],params:com.fasterxml.jackson.databind.JsonNode):Array[Byte]=
          (new String(chunk,"UTF-8")+params.path("suffix").asText).getBytes("UTF-8")
      }
      val bytes=new ByteArrayOutputStream()
      OperatorRunner.run(Array("operator",dir.toString,dir.resolve("params.json").toString),bytes)(op.process)
      val in=new ByteArrayInputStream(bytes.toByteArray)
      assert(OperatorFrames.read(in).nonEmpty)
      assert(new String(OperatorFrames.read(in).get.data,"UTF-8")=="hello!")
      assert(OperatorFrames.read(in).nonEmpty);assert(OperatorFrames.read(in).isEmpty)
    } finally {
      root.close();allocator.close()
      Seq("0.arrow","manifest.json","params.json").foreach(n=>Files.deleteIfExists(dir.resolve(n)));Files.delete(dir)
    }
  }
  test("runner emits empty outputs and redirects business logs away from frames") {
    val dir=Files.createTempDirectory("operator-sdk-")
    try {
      Files.writeString(dir.resolve("manifest.json"),"[]")
      Files.writeString(dir.resolve("params.json"),"{\"factor\":2}")
      val bytes=new ByteArrayOutputStream()
      OperatorRunner.run(Array("operator",dir.toString,dir.resolve("params.json").toString),bytes){ctx=>
        assert(ctx.params.path("factor").asInt==2)
        ctx.bytesOutput("empty");ctx.bytesOutput("result").write("ok".getBytes("UTF-8"))
        println("business log")
      }
      val in=new ByteArrayInputStream(bytes.toByteArray)
      val frames=Iterator.continually(OperatorFrames.read(in)).takeWhile(_.nonEmpty).map(_.get).toVector
      assert(frames.size==5)
      assert(new String(frames(2).data,"UTF-8")=="ok")
      assert(new String(frames.last.metadata,"UTF-8").contains("end"))
    } finally {Files.deleteIfExists(dir.resolve("manifest.json"));Files.deleteIfExists(dir.resolve("params.json"));Files.delete(dir)}
  }
  test("failure never emits a successful end") {
    val dir=Files.createTempDirectory("operator-sdk-")
    try {
      Files.writeString(dir.resolve("manifest.json"),"[]");Files.writeString(dir.resolve("params.json"),"{}")
      val bytes=new ByteArrayOutputStream()
      intercept[IllegalStateException]{OperatorRunner.run(Array("operator",dir.toString,dir.resolve("params.json").toString),bytes){ctx=>ctx.bytesOutput("result");throw new IllegalStateException("failed")}}
      val in=new ByteArrayInputStream(bytes.toByteArray)
      assert(OperatorFrames.read(in).nonEmpty);assert(OperatorFrames.read(in).isEmpty)
    } finally {Files.deleteIfExists(dir.resolve("manifest.json"));Files.deleteIfExists(dir.resolve("params.json"));Files.delete(dir)}
  }
}
