package dacp

import java.nio.file.{Files,Path}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import dacp.protocol.Metadata

class SourceSubmissionSpec extends AnyFunSuite {
  test("NORMAL denies source at both coordinator and direct worker Flight entry points") {
    val allocator=new RootAllocator()
    val config=Config("localhost",18816,"localhost",Map.empty,Seq("dacp://localhost:18816/data"),Path.of("unused"),Path.of("unused"),Path.of("unused"))
    val service=new TaskService(config,allocator,new TaskExecutor(config,allocator))
    val producer=new NoOpFlightProducer {
      override def doExchange(ctx:FlightProducer.CallContext,r:FlightStream,w:FlightProducer.ServerStreamListener):Unit=service.exchange(ctx,r,w)
    }
    val server=FlightServer.builder(allocator,Location.forGrpcInsecure("127.0.0.1",0),producer).build().start()
    val client=FlightClient.builder(allocator,server.getLocation).build()
    val source="""{"runtime":"python@1.0","source":"def process(inputs, params): yield b'x'"}"""
    val job=s"""{"job":"source","tasks":[{"id":"run","operator":$source,"inputs":{"source":{"uri":"dacp://localhost:18816/data/item"}}}]}"""
    val worker=s"""{"id":"run","operator":$source,"params":{},"ports":["source"],"sources":["dacp://localhost:18816/data/item"]}"""
    val requests=Seq(Metadata.OperatorRequest.newBuilder().setJobYaml(job).build(),
      Metadata.OperatorRequest.newBuilder().setTaskJson(com.google.protobuf.ByteString.copyFromUtf8(worker)).build())
    try requests.foreach { request=>
      val exchange=client.doExchange(FlightDescriptor.command(request.toByteArray))
      val root=VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
      try {
        exchange.getWriter.start(root);exchange.getWriter.completed()
        val error=intercept[FlightRuntimeException](exchange.getReader.next())
        assert(error.status().code()==FlightStatusCode.UNAUTHORIZED)
        assert(error.getMessage.contains("DEBUG"))
      } finally {try exchange.close() catch{case _:FlightRuntimeException=>()};root.close()}
    } finally {client.close();server.close();service.close();allocator.close()}
  }
  test("record stream metadata uses IPC message schemas, including nonempty field types") {
    val schema=new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(Records.field("id","int64")))
    val frame=StreamProtocol.start("result","records","csv",Some(schema))
    assert(StreamProtocol.schema(StreamProtocol.header(frame))==schema)
  }
  test("configuration requires explicit DEBUG and strict runtime settings") {
    val example=Path.of("../examples/source-debug.yaml")
    val text=Files.readString(example)
    val temporary=Files.createTempFile("source-config-",".yaml")
    try {
      Seq("DEBUG"->true,"NORMAL"->false).foreach {case(mode,enabled)=>
        Files.writeString(temporary,text.replace("mode: DEBUG",s"mode: $mode"))
        val config=Config.load(temporary)
        try {assert(config.debug==enabled);assert(config.sourceRuntimes("python@1.0").executor=="python")}
        finally config.close()
      }
      Files.writeString(temporary,text.replace("mode: DEBUG","mode: debug"))
      intercept[IllegalArgumentException](Config.load(temporary))
      Files.writeString(temporary,text.replace("mode: DEBUG\n",""))
      val normal=Config.load(temporary);try assert(!normal.debug) finally normal.close()
    } finally Files.delete(temporary)
  }
}
