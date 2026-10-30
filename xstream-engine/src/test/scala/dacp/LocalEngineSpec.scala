package dacp

import _root_.xstream.data._
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files
class LocalEngineSpec extends AnyFunSuite {
  test("read_item selection emits scientific structure and block metadata through a complete Job") {
    val dir=Files.createTempDirectory("xstream-scientific-");Files.writeString(dir.resolve("genome.fa"),">chr1\nACGTACGT\n")
    val settings=Data.mapper.createObjectNode();settings.put("execution_directory",dir.resolve("history").toString)
    settings.putObject("datasets").putObject("files").put("provider","directory").putObject("source").put("root",dir.toString)
    val plugins=new PluginEnvironment();val host=new LocalEngine(settings,plugins)
    val cancel=new StreamCancel()
    try {
      val yaml="name: Scientific read\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'context:/files/genome.fa/sequences', view: sequence, selection: {offset: 2, length: 4}}\noutputs: {result: read}\n"
      val frames=scala.collection.mutable.ArrayBuffer.empty[OperatorFrame]
      host.engine.run(yaml,frames+=_,cancel)
      val start=frames.map(StreamProtocol.header).find(_.path("type").asText=="start").get
      assert(start.path("representation").asText=="sequence" && start.path("structure").path("view").asText=="sequence")
      val data=frames.filter(f=>StreamProtocol.header(f).path("type").asText=="data")
      assert(data.map(f=>new String(f.data,java.nio.charset.StandardCharsets.UTF_8)).mkString=="GTAC")
      assert(StreamProtocol.header(data.head).path("block").path("offset").asLong==2)
      assert(frames.map(StreamProtocol.header).exists(_.path("type").asText=="end"))
      intercept[IllegalArgumentException](Jobs.parse(yaml.replace("view: sequence","view: records")))
    }finally {cancel.close();host.close();plugins.close();Data.deleteTree(dir)}
  }
  test("standalone context reads share the JVM catalog and stream without a DACP server") {
    val dir=Files.createTempDirectory("xstream-local-");Files.writeString(dir.resolve("input.txt"),"local stream\n")
    val settings=Data.mapper.createObjectNode();settings.put("execution_directory",dir.resolve("history").toString)
    settings.putObject("datasets").putObject("files").put("provider","directory").putObject("source").put("root",dir.toString)
    val plugins=new PluginEnvironment();val host=new LocalEngine(settings,plugins)
    try {
      assert(host.engine.dataCatalog eq DataCatalogRuntime.catalog)
      assert(host.engine.dataCatalog.list().map(_.name)==Seq("files"))
      val yaml="name: Local read\ntasks:\n- id: read\n  operator: read_item@1.0\n  inputs: {}\n  params: {uri: 'context:/files/input.txt', view: bytes}\noutputs: {result: read}\n"
      val out=new java.io.ByteArrayOutputStream();val cancel=new StreamCancel()
      try {host.engine.run(yaml,f=>if(StreamProtocol.header(f).path("type").asText=="data")out.write(f.data),cancel);assert(out.toString("UTF-8")=="local stream\n")}finally cancel.close()
      val other=new DataCatalog {def list():Seq[DatasetInfo]=Seq.empty;def open(n:String):PublishedDataset=throw new UnsupportedOperationException}
      intercept[IllegalArgumentException](DataCatalogRuntime.bind(other))
    }finally {host.close();plugins.close()}
    intercept[IllegalArgumentException](DataCatalogRuntime.catalog)
  }
}
