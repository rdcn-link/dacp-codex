package dacp

import java.nio.file.Path
import java.util.concurrent.{CountDownLatch,TimeUnit}
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight._
import dacp.protocol.Metadata

class CollaborationSpec extends AnyFunSuite {
  val a = "dacp://node-a:8815"
  val b = "dacp://node-b:8815"
  def config = Config("localhost",8815,"node-a",Map.empty,Seq(a+"/result"),Path.of("unused"),Path.of("unused"),Path.of("unused"),
    taskPeers=Seq(b),taskInputPrefixes=Seq(b+"/data",a+"/result"))
  val yaml = s"""job: distributed
    |tasks:
    | - {id: first, input: '$b/data', operator: format_convert@1.0, output: converted}
    | - {id: relay, input: converted/result-1, node: '$a', operator: identity@1.0, output: result}
    | - id: joined
    |   inputs: [converted, result]
    |   node: '$b'
    |   operator: identity@1.0
    |   output: joined
    |""".stripMargin
  test("URI-based placement resolves upstream outputs across nodes and supports multi-input overrides") {
    val calls = scala.collection.mutable.ArrayBuffer.empty[(String,Task)]
    val coordinator = new JobCoordinator(config,(node,task) => {calls += node -> task;s"$node/${task.output}"})
    val result = coordinator.run(yaml)
    assert(calls.map(_._1).toSeq == Seq(b,a,b))
    assert(calls(1)._2.inputs == Seq(b+"/converted/result-1"))
    assert(calls(2)._2.inputs == Seq(b+"/converted",a+"/result"))
    assert(calls.forall(_._2.node.isEmpty))
    assert(result("joined") == b+"/joined")
  }
  test("invalid target rejects the full graph before executing any task") {
    var calls=0
    val coordinator = new JobCoordinator(config,(_,_) => {calls+=1;""})
    intercept[IllegalArgumentException](coordinator.run(yaml.replace(s"node: '$a'","node: 'dacp://untrusted:8815'")))
    assert(calls==0)
  }
  test("failed upstream task stops downstream dispatch, without retries") {
    var calls=0
    val coordinator = new JobCoordinator(config,(_,_) => {calls+=1;throw new IllegalStateException("worker disconnected")})
    intercept[IllegalStateException](coordinator.run(yaml));assert(calls==1)
  }
  test("unexpected worker output location stops dependency execution") {
    var calls=0
    val coordinator = new JobCoordinator(config,(_,_) => {calls+=1;s"$a/wrong"})
    intercept[IllegalArgumentException](coordinator.run(yaml));assert(calls==1)
  }
  test("URI policy rejects traversal, credentials, non-dacp schemes and prefix lookalikes") {
    assert(DacpUri.origin("dacp://NODE-B/data") == b)
    assert(DacpUri.within(b+"/data/nested.csv",b+"/data"))
    assert(!DacpUri.within(b+"/data-other",b+"/data"))
    Seq(b+"/data/%2e%2e/secret", "https://node-b/data", "dacp://user:password@node-b/data",b+"/data?token=x")
      .foreach(v => intercept[IllegalArgumentException](DacpUri.parse(v)))
    intercept[IllegalArgumentException](DacpUri.node(b+"/dataset"))
  }
  class Response extends FlightProducer.StreamListener[Result] {
    val done=new CountDownLatch(1)
    @volatile var body: Array[Byte]=null
    @volatile var error: Throwable=null
    def onNext(r: Result): Unit = {body=r.getBody}
    def onError(e: Throwable): Unit = {error=e;done.countDown()}
    def onCompleted(): Unit = done.countDown()
    def await(): Unit = assert(done.await(5,TimeUnit.SECONDS))
  }
  def request(input: String=b+"/data",output: String="result") =
    new Action(TaskService.ExecuteAction,TaskWire.encode(Task("test",Seq(input),"identity@1.0",new java.util.LinkedHashMap[String,Object](),output)).toByteArray)
  test("worker authorizes output and every input before invoking the local executor") {
    val allocator=new RootAllocator();var calls=0
    val service=new TaskService(config,allocator,t => {calls+=1;a+"/"+t.output})
    try {
      val denied=new Response();service.handle(request(b+"/data-other"),denied);denied.await()
      assert(denied.error.getMessage.contains("FORBIDDEN"));assert(calls==0)
      val deniedOutput=new Response();service.handle(request(output="notallowed"),deniedOutput);deniedOutput.await()
      assert(deniedOutput.error.getMessage.contains("FORBIDDEN"));assert(calls==0)
      val ok=new Response();service.handle(request(),ok);ok.await();assert(ok.error==null)
      assert(Metadata.ExecuteTaskResult.parseFrom(ok.body).getOutputUri==a+"/result");assert(calls==1)
    } finally {service.close();allocator.close()}
  }
  test("node rejects excess control work without queueing or executing it") {
    val allocator=new RootAllocator();val started=new CountDownLatch(1);val release=new CountDownLatch(1)
    val service=new TaskService(config.copy(maxConcurrentTasks=1),allocator,t => {started.countDown();release.await();a+"/"+t.output})
    try {
      val first=new Response();service.handle(request(),first);assert(started.await(5,TimeUnit.SECONDS))
      val second=new Response();service.handle(request(),second);second.await()
      assert(second.error.getMessage.contains("capacity"))
      release.countDown();first.await();assert(first.error==null)
    } finally {release.countDown();service.close();allocator.close()}
  }
  test("task protobuf preserves ordered schema declarations and requires absolute worker inputs") {
    val params=new java.util.LinkedHashMap[String,Object]()
    params.put("schema",Map("id"->"int64").asJava)
    val task=Task("test",Seq(b+"/data"),"format_convert@1.0",params,"result")
    assert(TaskWire.decode(TaskWire.encode(task)) == task)
    intercept[IllegalArgumentException](TaskWire.decode(TaskWire.encode(task.copy(inputs=Seq("unresolved")))))
  }
}
