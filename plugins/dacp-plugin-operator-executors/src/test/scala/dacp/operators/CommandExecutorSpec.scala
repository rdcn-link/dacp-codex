package dacp.operators

import _root_.xstream.data._


import dacp.OperatorProgram
import java.nio.file.Path
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class CommandExecutorSpec extends AnyFunSuite {
  def settings(json:String):Map[String,Object] = new com.fasterxml.jackson.databind.ObjectMapper()
    .readValue(json,classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap
  def program = OperatorProgram("uppercase@1","/usr/bin/tr",None,
    settings("""{"args":["a-z","A-Z","$(touch /tmp/should-not-exist)"],"stdin":"source","stdout":{"port":"result","representation":"bytes","format":"text"}}"""),
    Map("source"->Set("bytes")),Map("result"->Set("bytes")))
  test("native sources and sinks declare absent streams explicitly with null") {
    val sink=program.copy(configuration=settings("""{"args":[],"stdin":null,"stdout":null}"""),inputs=Map.empty,outputs=Map.empty)
    val command=new CommandExecutor().command(sink,None)
    val config=new com.fasterxml.jackson.databind.ObjectMapper().readTree(command.entrypoint.get.last)
    assert(config.path("stdin").isNull && config.path("port").isNull)
    intercept[IllegalArgumentException](new CommandExecutor().command(sink.copy(inputs=program.inputs),None))
    val source=program.copy(configuration=program.configuration+("stdin"->null),inputs=Map.empty)
    new CommandExecutor().command(source,None)
  }
  test("command launches bridge without shell and preserves literal arguments") {
    val c=new CommandExecutor().command(program,None)
    assert(c.arguments.isEmpty && !c.executableScratch)
    val launch=c.entrypoint.get
    assert(launch.take(3)==Vector("/usr/local/bin/python","-u","-c"))
    val config=new com.fasterxml.jackson.databind.ObjectMapper().readTree(launch.last)
    assert(config.path("argv").get(3).asText=="$(touch /tmp/should-not-exist)")
    assert(!config.path("artifact").asBoolean)
  }
  test("executable artifacts use explicit paths and private scratch") {
    val c=new CommandExecutor().command(program.copy(entrypoint="/operator/program",artifact=Some(Path.of("/cache/digest"))),Some("/operator/program"))
    assert(c.executableScratch)
    assert(new com.fasterxml.jackson.databind.ObjectMapper().readTree(c.entrypoint.get.last).path("artifact").asBoolean)
    intercept[IllegalArgumentException](new CommandExecutor().command(program.copy(artifact=Some(Path.of("file"))),Some("/operator/program")))
  }
  test("command rejects unsupported ports, implicit conversions and malformed configuration") {
    val bad=Seq(program.copy(inputs=Map("source"->Set("records"))),
      program.copy(inputs=program.inputs+("extra"->Set("bytes"))),program.copy(entrypoint="tr"),
      program.copy(configuration=program.configuration+("unknown"->"setting")),
      program.copy(configuration=program.configuration+("args"->"a-z A-Z")),
      program.copy(configuration=program.configuration+("args"->Seq("bad\u0000argument").asJava)))
    bad.foreach(p=>intercept[IllegalArgumentException](new CommandExecutor().command(p,None)))
    intercept[IllegalArgumentException](new CommandExecutor().sourceCommand(program,"/source"))
  }
}
