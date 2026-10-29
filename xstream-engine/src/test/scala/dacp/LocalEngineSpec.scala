package dacp
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files
class LocalEngineSpec extends AnyFunSuite {
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
