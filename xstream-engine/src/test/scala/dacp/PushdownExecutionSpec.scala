package dacp

import _root_.xstream.data._

import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files
import dacp.union.Json

class PushdownExecutionSpec extends AnyFunSuite {
  private val script="""name: Remote filter
    |tasks:
    |- id: read
    |  operator: read_item@1.0
    |  inputs: {}
    |  params: {uri: 'dacp://remote:18861/weather/observations'}
    |- id: filter
    |  operator: filter@1.0
    |  inputs: {source: read}
    |outputs: {result: filter}
    |""".stripMargin
  private def host(remote:ComputePushdown)(f:LocalEngine=>Unit):Unit={
    val dir=Files.createTempDirectory("pushdown-contract-");val plugins=new PluginEnvironment()
    val config=Json.obj().put("execution_directory",dir.resolve("history").toString).put("max_concurrent_tasks",1)
    val engine=new LocalEngine(config,plugins,(_,_,_)=>throw new AssertionError("Remote raw data must not be read locally"),Some(remote))
    try f(engine)finally {engine.close();plugins.close();Data.deleteTree(dir)}
  }
  test("embedded engine dispatches a whole chain and routes returned frames without a local operator") {
    var calls=0
    val remote=new ComputePushdown {
      val nodes=Set("dacp://remote:18861")
      def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit={
        calls+=1;assert(plan.tasks.map(_.id)==Vector("read","filter"));assert(plan.tasks.head.params.get("uri")=="context:/weather/observations")
        onExecution("remote-execution")
        emit(StreamProtocol.start("p0","bytes","text"));emit(StreamProtocol.frame("data","p0","filtered\n".getBytes));emit(StreamProtocol.frame("end","p0"))
      }
    }
    host(remote){h=>
      val cancel=new StreamCancel();val frames=scala.collection.mutable.ArrayBuffer.empty[OperatorFrame]
      try {
        val id=h.engine.run(script,frames+=_,cancel)
        assert(calls==1 && frames.map(StreamProtocol.header).forall(_.path("port").asText=="result"))
        assert(frames.filter(f=>StreamProtocol.header(f).path("type").asText=="data").map(f=>new String(f.data)).mkString=="filtered\n")
        assert(h.executionHistory.get(id).get.path("execution_mode").asText=="automatic_pushdown")
        assert(h.executionHistory.list().exists(_.path("remote_execution_id").asText=="remote-execution"))
      }finally cancel.close()
    }
  }
  test("remote failure is recorded and propagated without local fallback") {
    val remote=new ComputePushdown {
      val nodes=Set("dacp://remote:18861")
      def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit=throw new IllegalStateException("remote execution denied")
    }
    host(remote){h=>
      val cancel=new StreamCancel()
      try {
        val e=intercept[IllegalStateException](h.engine.run(script,_=>(),cancel));assert(e.getMessage=="remote execution denied")
        assert(h.executionHistory.list().filter(_.path("kind").asText=="remote_job").forall(_.path("state").asText=="failed"))
      }finally cancel.close()
    }
  }
  test("streaming retries rerun remote regions and notify the consumer to discard partial output") {
    val calls=new java.util.concurrent.atomic.AtomicInteger()
    val remote=new ComputePushdown {
      val nodes=Set("dacp://remote:18861")
      def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit={
        emit(StreamProtocol.start("p0","bytes","text"))
        if(calls.incrementAndGet()==1){emit(StreamProtocol.frame("data","p0","partial".getBytes));throw new IllegalStateException("transient failure")}
        emit(StreamProtocol.frame("data","p0","complete".getBytes));emit(StreamProtocol.frame("end","p0"))
      }
    }
    host(remote){h=>
      val cancel=new StreamCancel();val result=new StringBuilder;var retries=0
      try {
        h.engine.run(script,f=>StreamProtocol.header(f).path("type").asText match {
          case "attempt_failed"=>retries+=1;result.clear()
          case "data"=>result.append(new String(f.data))
          case _=>()
        },cancel,Json.obj().put("max_retries",1))
        assert(calls.get()==2 && retries==1 && result.toString=="complete")
      }finally cancel.close()
    }
  }
  test("cancelling the local Job closes the remote execution scope") {
    val started=new java.util.concurrent.CountDownLatch(1);val stopped=new java.util.concurrent.CountDownLatch(1)
    val remote=new ComputePushdown {
      val nodes=Set("dacp://remote:18861")
      def execute(node:String,plan:JobPlan,emit:OperatorFrame=>Unit,cancel:StreamCancel,onExecution:String=>Unit):Unit={
        cancel.add(new AutoCloseable{def close():Unit=stopped.countDown()});started.countDown()
        while(true){cancel.active();Thread.sleep(10)}
      }
    }
    host(remote){h=>
      val cancel=new StreamCancel();val failure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val worker=new Thread(()=>try h.engine.run(script,_=>(),cancel)catch{case t:Throwable=>failure.set(t)})
      worker.start();assert(started.await(5,java.util.concurrent.TimeUnit.SECONDS));cancel.close();worker.join(5000)
      assert(!worker.isAlive && stopped.await(1,java.util.concurrent.TimeUnit.SECONDS));assert(failure.get()!=null)
      val remoteRecords=h.executionHistory.list().filter(_.path("kind").asText=="remote_job")
      assert(remoteRecords.nonEmpty && remoteRecords.forall(_.path("state").asText=="cancelled"))
    }
  }
}
