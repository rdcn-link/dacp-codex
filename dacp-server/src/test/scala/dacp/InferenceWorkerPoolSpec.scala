package dacp

import java.util.concurrent.{CountDownLatch,TimeUnit,TimeoutException}
import java.util.concurrent.atomic.{AtomicInteger,AtomicReference}
import org.scalatest.funsuite.AnyFunSuite

class InferenceWorkerPoolSpec extends AnyFunSuite {
  private def await(latch:CountDownLatch):Unit=assert(latch.await(5,TimeUnit.SECONDS))
  test("different requests reuse a worker without caching results; capacity evicts idle workers") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,60,5));val created=new AtomicInteger;val called=new AtomicInteger;val closed=new AtomicInteger
    def create()=new InferenceWorker {created.incrementAndGet();def request(b:Array[Byte])={called.incrementAndGet();b.reverse};def close():Unit={closed.incrementAndGet();()}}
    val c=new StreamCancel()
    try {
      assert(pool.request("a",Array[Byte](1,2),c)(()=>create()).sameElements(Array[Byte](2,1)))
      assert(pool.request("a",Array[Byte](3),c)(()=>create()).sameElements(Array[Byte](3)))
      assert(created.get()==1 && called.get()==2)
      pool.request("b",Array[Byte](4),c)(()=>create());assert(created.get()==2 && closed.get()==1)
    }finally {pool.close();c.close()}
    assert(closed.get()==2)
  }
  test("idle workers are reclaimed and later requests start a fresh process") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,1,5));val closed=new CountDownLatch(1);val created=new AtomicInteger
    def create()=new InferenceWorker {created.incrementAndGet();def request(b:Array[Byte])=b;def close():Unit=closed.countDown()}
    val c=new StreamCancel()
    try {pool.request("a",Array[Byte](1),c)(()=>create());await(closed);pool.request("a",Array[Byte](2),c)(()=>create());assert(created.get()==2)}finally {pool.close();c.close()}
  }
  test("an exception discards the worker; the next call rebuilds it") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings());val closed=new AtomicInteger;val c=new StreamCancel()
    try {
      intercept[IllegalArgumentException](pool.request("a",Array[Byte](1),c)(()=>new InferenceWorker {def request(b:Array[Byte]):Array[Byte]=throw new IllegalArgumentException("broken");def close():Unit={closed.incrementAndGet();()}}))
      assert(closed.get()==1)
      assert(pool.request("a",Array[Byte](2),c)(()=>new InferenceWorker {def request(b:Array[Byte])=b;def close():Unit=()}).sameElements(Array[Byte](2)))
    }finally {pool.close();c.close()}
  }
  test("timeout destroys a busy worker and frees capacity") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,60,1));val stopped=new CountDownLatch(1);val c=new StreamCancel()
    try {
      intercept[TimeoutException](pool.request("a",Array[Byte](1),c)(()=>new InferenceWorker {def request(b:Array[Byte])={stopped.await();b};def close():Unit=stopped.countDown()}))
      await(stopped)
      pool.request("a",Array[Byte](2),c)(()=>new InferenceWorker {def request(b:Array[Byte])=b;def close():Unit=()})
    }finally {pool.close();c.close()}
  }
  test("same-key requests serialize and cancellation of a waiter does not destroy the active worker") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,60,5));val started=new CountDownLatch(1);val release=new CountDownLatch(1);val closed=new AtomicInteger;val count=new AtomicInteger
    val c=new StreamCancel();val waiting=new StreamCancel();val error=new AtomicReference[Throwable]()
    def create()=new InferenceWorker {def request(b:Array[Byte])={count.incrementAndGet();started.countDown();release.await();b};def close():Unit={closed.incrementAndGet();release.countDown()}}
    val first=new Thread(()=>try pool.request("a",Array[Byte](1),c)(()=>create())catch{case e:Throwable=>error.set(e)})
    first.start();await(started)
    val second=new Thread(()=>try pool.request("a",Array[Byte](2),waiting)(()=>throw new AssertionError("must reuse worker"))catch{case _:java.util.concurrent.CancellationException=>()})
    second.start();waiting.close();second.join(3000);assert(!second.isAlive);assert(closed.get()==0 && count.get()==1)
    release.countDown();first.join(3000);assert(!first.isAlive && error.get()==null)
    pool.close();c.close();assert(closed.get()==1)
  }
  test("closing the pool interrupts active work and rejects future requests") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings());val started=new CountDownLatch(1);val release=new CountDownLatch(1);val c=new StreamCancel()
    val thread=new Thread(()=>try pool.request("a",Array[Byte](1),c)(()=>new InferenceWorker {def request(b:Array[Byte])={started.countDown();release.await();throw new IllegalStateException("closed")};def close():Unit=release.countDown()})catch{case _:Throwable=>()})
    thread.start();await(started);pool.close();thread.join(3000);assert(!thread.isAlive)
    intercept[IllegalArgumentException](pool.request("b",Array[Byte](1),c)(()=>throw new AssertionError("closed")));c.close()
  }
  test("shutdown also cancels a worker factory that has not published its process yet") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,60,5));val started=new CountDownLatch(1);val interrupted=new CountDownLatch(1)
    val c=new StreamCancel();val error=new AtomicReference[Throwable]()
    val thread=new Thread(()=>try pool.request("a",Array[Byte](1),c)(()=>{
      started.countDown()
      try {new CountDownLatch(1).await();throw new AssertionError("unexpected startup completion")}
      catch{case e:InterruptedException=>interrupted.countDown();throw e}
    })catch{case t:Throwable=>error.set(t)})
    thread.start();await(started);pool.close();thread.join(3000);assert(!thread.isAlive);await(interrupted);assert(error.get()!=null);c.close()
  }
  test("a worker being destroyed still consumes capacity until cleanup finishes") {
    val pool=new InferenceWorkerPool(InferenceWorkerSettings(1,60,5));val closing=new CountDownLatch(1);val release=new CountDownLatch(1)
    val calls=new AtomicInteger;val c=new StreamCancel();val waiter=new StreamCancel();val error=new AtomicReference[Throwable]()
    pool.request("a",Array[Byte](1),c)(()=>new InferenceWorker {calls.incrementAndGet();def request(b:Array[Byte])=b;def close():Unit={closing.countDown();release.await()}})
    val evicting=new Thread(()=>try pool.request("b",Array[Byte](2),c)(()=>new InferenceWorker {calls.incrementAndGet();def request(b:Array[Byte])=b;def close():Unit=()})catch{case t:Throwable=>error.set(t)})
    evicting.start();await(closing)
    val waiting=new Thread(()=>try pool.request("c",Array[Byte](3),waiter)(()=>{calls.incrementAndGet();throw new AssertionError("cleanup must keep its capacity reservation")})catch{case _:java.util.concurrent.CancellationException=>()})
    waiting.start();Thread.sleep(50);waiter.close();waiting.join(3000);assert(!waiting.isAlive && calls.get()==1)
    release.countDown();evicting.join(3000);assert(!evicting.isAlive && error.get()==null && calls.get()==2)
    pool.close();c.close()
  }
  test("worker identity uses artifact contents and unambiguous launch configuration") {
    val file=java.nio.file.Files.createTempFile("inference-key-", ".py")
    val program=OperatorProgram("probe@1","probe",Some(file))
    val op=ResolvedOperator("sha256:"+"a"*64,Some(file),Some("/operator/probe.py"),OperatorCommand(),Map.empty,Map.empty,"python",Some(program))
    try {
      java.nio.file.Files.writeString(file,"first")
      val command=OperatorCommand(entrypoint=Some(Vector("a","arguments")),arguments=Vector("b"))
      val key=OperatorInference.workerKey(op,command,false)
      assert(key==OperatorInference.workerKey(op,command,false))
      assert(key!=OperatorInference.workerKey(op,command.copy(entrypoint=Some(Vector("a")),arguments=Vector("arguments","b")),false))
      assert(key!=OperatorInference.workerKey(op.copy(image="sha256:"+"b"*64),command,false))
      java.nio.file.Files.writeString(file,"second")
      assert(key!=OperatorInference.workerKey(op,command,false))
    }finally java.nio.file.Files.delete(file)
  }
  test("worker settings reject unknown fields, booleans and out-of-range limits") {
    def read(raw:String)=InferenceWorkerSettings.read(Some(Data.mapper.readValue(raw,classOf[java.util.LinkedHashMap[String,Object]])))
    assert(read("""{"max_workers":2,"idle_seconds":3,"timeout_seconds":4}""")==InferenceWorkerSettings(2,3,4))
    Seq("""{"max_workers":0}""","""{"idle_seconds":true}""","""{"timeout_seconds":301}""","""{"cache":true}""").foreach(s=>intercept[IllegalArgumentException](read(s)))
  }
}
