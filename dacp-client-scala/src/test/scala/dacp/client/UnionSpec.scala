package dacp.client

import _root_.xstream.data._


import org.scalatest.funsuite.AnyFunSuite
import java.io._

class UnionSpec extends AnyFunSuite {
  private def source(bytes:Array[Byte]):dacp.union.BinarySource=new dacp.union.BinarySource {
    private val in=new DataInputStream(new ByteArrayInputStream(bytes))
    def next():Option[Array[Byte]]=dacp.OperatorFrames.read(in).map(dacp.OperatorFrames.encode)
    def close():Unit=in.close()
  }
  private def eventSource(value:String):dacp.union.BinarySource=new dacp.union.BinarySource {
    private var pending=true
    def next():Option[Array[Byte]]=if(pending){pending=false;Some(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))}else None
    def close():Unit=()
  }
  private def frame(header:String,bytes:Array[Byte]=Array.emptyByteArray):Array[Byte]={
    val out=new ByteArrayOutputStream();val d=new DataOutputStream(out);val h=header.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    d.writeInt(h.length);d.writeInt(bytes.length);d.write(h);d.write(bytes);out.toByteArray
  }
  private val start=frame("""{"type":"start","port":"r","representation":"bytes","format":"binary"}""")
  private val data=frame("""{"type":"data","port":"r"}""","abc".getBytes)
  private val end=frame("""{"type":"end","port":"r"}""")
  private val complete=frame("""{"type":"complete"}""")
  test("Union results retain data and complete without cancellation") {
    var succeeded=false;var aborted=false
    val stream=new UnionResults(source(start++data++end++complete),()=>succeeded=true,()=>aborted=true)
    val values=stream.toVector;stream.close()
    assert(values.map(_.kind)==Vector("start","data","end","complete"));assert(new String(values(1).data)=="abc")
    assert(succeeded && !aborted)
  }
  test("truncation, invalid ordering and early close abort the Union result") {
    Seq(start++data,complete,data,frame("""{"type":"error","message":"node failed"}""")).foreach { bytes=>
      var aborted=false
      val stream=new UnionResults(source(bytes),()=>fail("unexpected success"),()=>aborted=true)
      intercept[Exception](stream.toVector);assert(aborted);stream.close()
    }
    var aborted=false
    val stream=new UnionResults(source(start++data),()=>(),()=>aborted=true)
    assert(stream.next().kind=="start");stream.close();assert(aborted)
  }
  test("complete requires a successful Flight trailer") {
    val upstream=source(start++end++complete)
    val broken=new dacp.union.BinarySource {
      def next():Option[Array[Byte]]=upstream.next() match {case None=>throw new java.io.IOException("failed Flight trailer");case v=>v}
      def close():Unit=upstream.close()
    }
    var succeeded=false;var aborted=false
    val results=new UnionResults(broken,()=>succeeded=true,()=>aborted=true)
    intercept[java.io.IOException](results.toVector)
    assert(!succeeded && aborted)
  }
  test("events expose gap snapshots and reject invalid JSON") {
    val stream=new UnionEvents(eventSource("{\"type\":\"gap\",\"sequence\":9,\"snapshot\":{}}"))
    assert(stream.next().path("type").asText=="gap");assert(!stream.hasNext);stream.close()
    val broken=new UnionEvents(eventSource("{"))
    intercept[com.fasterxml.jackson.core.JsonProcessingException](broken.hasNext)
  }
}
