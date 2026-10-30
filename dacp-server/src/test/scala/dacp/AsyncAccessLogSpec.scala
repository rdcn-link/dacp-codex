package dacp

import _root_.xstream.data._

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch,TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean,AtomicReference}
import org.scalatest.funsuite.AnyFunSuite

class AsyncAccessLogSpec extends AnyFunSuite {
  test("slow disk synchronization does not delay new request records; shutdown drains the queue") {
    val dir=Files.createTempDirectory("async-access-log-")
    val syncing=new CountDownLatch(1);val release=new CountDownLatch(1)
    val out=new java.io.ByteArrayOutputStream();val ended=new AtomicBoolean()
    val log=new AccessLog(dir,1024*1024,_=>new AccessLogOutput.Output {
      def write(b:Array[Byte]):Unit=out.synchronized(out.write(b))
      def sync():Unit={syncing.countDown();release.await()}
      def close():Unit=ended.set(true)
    })
    val error=new AtomicReference[Throwable]()
    val append=new Thread(()=>try log.append(Data.mapper.createObjectNode().put("request",2))catch{case t:Throwable=>error.set(t)})
    val close=new Thread(()=>try log.close()catch{case t:Throwable=>error.set(t)})
    try {
      log.append(Data.mapper.createObjectNode().put("request",1))
      assert(syncing.await(3,TimeUnit.SECONDS))
      append.start();append.join(1000);assert(!append.isAlive && error.get()==null)
      close.start();close.join(100);assert(close.isAlive && !ended.get())
      release.countDown();close.join(3000);assert(!close.isAlive && ended.get() && error.get()==null)
      assert(out.toString("UTF-8").linesIterator.map(Data.mapper.readTree).map(_.path("request").asInt).toVector==Vector(1,2))
    }finally {release.countDown();log.close();Data.deleteTree(dir)}
  }
  test("the queue is bounded and saturated writers apply backpressure without dropping records") {
    val dir=Files.createTempDirectory("bounded-access-log-");val started=new CountDownLatch(1);val release=new CountDownLatch(1)
    val out=new java.io.ByteArrayOutputStream()
    val log=new AccessLog(dir,1024*1024,_=>new AccessLogOutput.Output {
      def write(b:Array[Byte]):Unit={started.countDown();release.await();out.write(b)}
      def sync():Unit=()
      def close():Unit=()
    },capacity=1)
    val done=new CountDownLatch(1)
    val thread=new Thread(()=>{log.append(Data.mapper.createObjectNode().put("request",3));done.countDown()})
    try {
      log.append(Data.mapper.createObjectNode().put("request",1));assert(started.await(3,TimeUnit.SECONDS))
      log.append(Data.mapper.createObjectNode().put("request",2));thread.start()
      assert(!done.await(100,TimeUnit.MILLISECONDS));release.countDown();assert(done.await(3,TimeUnit.SECONDS))
      log.close();assert(out.toString("UTF-8").linesIterator.size==3)
    }finally {release.countDown();log.close();thread.join(3000);Data.deleteTree(dir)}
  }
  test("background write errors are surfaced on later appends and shutdown") {
    val dir=Files.createTempDirectory("failed-access-log-");val failed=new CountDownLatch(1)
    val log=new AccessLog(dir,1024,_=>new AccessLogOutput.Output {
      def write(b:Array[Byte]):Unit=throw new java.io.IOException("test write failure")
      def sync():Unit=()
      def close():Unit=failed.countDown()
    })
    try {
      log.append(Data.mapper.createObjectNode().put("request",1));assert(failed.await(3,TimeUnit.SECONDS))
      intercept[IllegalStateException](log.append(Data.mapper.createObjectNode().put("request",2)))
      intercept[IllegalStateException](log.close())
    }finally Data.deleteTree(dir)
  }
}
