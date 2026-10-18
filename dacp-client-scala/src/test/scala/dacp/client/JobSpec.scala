package dacp.client

import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import dacp.protocol.Metadata
import java.util.concurrent.{CountDownLatch,TimeUnit}

class JobSpec extends AnyFunSuite {
  def frame(header:String,data:Array[Byte]=Array.emptyByteArray):Array[Byte]={
    val out=new java.io.ByteArrayOutputStream();val d=new java.io.DataOutputStream(out)
    val h=header.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    d.writeInt(h.length);d.writeInt(data.length);d.write(h);d.write(data);out.toByteArray
  }
  def fixture(frames:Seq[Array[Byte]])(body:JobStream=>Unit):Unit={
    val serverAllocator=new RootAllocator()
    val producer=new NoOpFlightProducer {
      override def doExchange(ctx:FlightProducer.CallContext,r:FlightStream,w:FlightProducer.ServerStreamListener):Unit={
        assert(Metadata.OperatorRequest.parseFrom(r.getDescriptor.getCommand).getJobYaml=="job: test")
        val root=VectorSchemaRoot.create(JobStream.envelope,serverAllocator)
        try {
          w.start(root)
          frames.foreach {f=>root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,f);root.setRowCount(1);w.putNext()}
          w.completed()
        } finally root.close()
      }
    }
    val server=FlightServer.builder(serverAllocator,Location.forGrpcInsecure("127.0.0.1",0),producer).build().start()
    val allocator=new RootAllocator();val client=FlightClient.builder(allocator,server.getLocation).build()
    val request=Metadata.OperatorRequest.newBuilder().setJobYaml("job: test").build().toByteArray
    val stream=Job.open(client,allocator,request,10)
    try body(stream) finally {stream.close();server.close();serverAllocator.close()}
  }
  val start=frame("""{"type":"start","port":"result","representation":"bytes","format":"text"}""")
  val data=frame("""{"type":"data","port":"result"}""","hello".getBytes)
  val end=frame("""{"type":"end","port":"result"}""")
  val complete=frame("""{"type":"complete"}""")
  test("stream preserves payload ownership and closes on successful completion") {
    fixture(Seq(start,data,end,complete)){s=>
      assert(s.hasNext && s.hasNext);assert(s.next().kind=="start")
      val chunk=s.next();assert(chunk.representation.contains("bytes"))
      assert(s.map(_.kind).toVector==Vector("end","complete"))
      assert(new String(chunk.data)=="hello");assert(!s.hasNext)
    }
  }
  test("early close stops iteration and is idempotent") {
    fixture(Seq(start,data,end,complete)){s=>s.next();s.close();s.close();assert(!s.hasNext)}
  }
  test("truncation, invalid ordering, and events after completion fail explicitly") {
    Seq(Seq(start,data),Seq(data),Seq(start,complete),Seq(start,end,complete,data),Seq(Array[Byte](1,2))).foreach{events=>
      fixture(events){s=>intercept[IllegalArgumentException](s.toVector)}
    }
  }
  test("records retain schema and payload after transport closure; schema changes fail") {
    val allocator = new RootAllocator()
    val schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
      """{"fields":[{"name":"value","nullable":true,"type":{"name":"int","bitWidth":32,"isSigned":true},"children":[]}]}""")
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      root.allocateNew()
      root.getVector(0).asInstanceOf[org.apache.arrow.vector.IntVector].setSafe(0, 42)
      root.setRowCount(1)
      val output = new java.io.ByteArrayOutputStream()
      val writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, output)
      try { writer.start(); writer.writeBatch(); writer.end() } finally writer.close()
      val encoded = java.util.Base64.getEncoder.encodeToString(schema.serializeAsMessage())
      val recordStart = frame(s"""{"type":"start","port":"result","representation":"records","format":"arrow","schema":"$encoded"}""")
      var retained: OutputEvent = null
      fixture(Seq(recordStart, frame("""{"type":"data","port":"result"}""", output.toByteArray), end, complete)) { stream =>
        val events = stream.toVector
        retained = events(1)
        assert(retained.schema.contains(schema))
      }
      val reader = retained.records(allocator)
      try {
        assert(reader.loadNextBatch())
        assert(reader.getVectorSchemaRoot.getVector(0).getObject(0) == Integer.valueOf(42))
        assert(!reader.loadNextBatch())
      } finally reader.close()
      fixture(Seq(recordStart, data)) { stream => intercept[Exception](stream.toVector) }
    } finally { root.close(); allocator.close() }
  }
  test("only node origins are accepted") {
    Seq("https://node","dacp://node/dataset","dacp://user:secret@node","dacp://node?x=y").foreach{n=>
      intercept[IllegalArgumentException](Job.run(n,"job: test"))
    }
  }
}
