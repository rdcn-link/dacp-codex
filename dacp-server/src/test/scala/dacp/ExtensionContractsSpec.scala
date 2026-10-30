package dacp

import _root_.xstream.data._

import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class ExtensionContractsSpec extends AnyFunSuite {
  test("plugin environments isolate overrides and respect application ownership") {
    class Handler extends FormatHandler {val id="csv";val extensions=Set("csv");var initialized=0;var closed=0
      override def initialize(e:PluginEnvironment):Unit={initialized+=1}
      override def close():Unit={closed+=1}}
    val first=new Handler;val second=new Handler
    val a=new PluginEnvironment(registrations=Seq(PluginRegistration(first)))
    val b=new PluginEnvironment(registrations=Seq(PluginRegistration(second,true)))
    assert(a.formats.byId("csv") eq first);assert(b.formats.byId("csv") eq second)
    assert(first.initialized==1 && second.initialized==1)
    a.close();b.close();b.close()
    assert(first.closed==0 && second.closed==1)
  }
  test("CSV reads reopenable nonlocal streams and releases the input") {
    var opens=0;var closes=0
    val source=new ByteSource {
      val name="rows.csv";val size=None
      def open(check:()=>Unit):java.io.InputStream={opens+=1;new java.io.ByteArrayInputStream("id,name\n1,alpha\n2,beta\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
        override def close():Unit={closes+=1;super.close()}}}
    }
    val plugins=new PluginEnvironment()
    try {val handler=plugins.formats.detect(source);assert(handler.count(source,FileOptions()).contains(2L));assert(opens==closes)}finally plugins.close()
  }
  test("access logs rotate whole JSON records and preserve all concurrent writes") {
    val dir=Files.createTempDirectory("access-log-contract-")
    try {
      val log=new AccessLog(dir,200)
      val pool=java.util.concurrent.Executors.newFixedThreadPool(4)
      try (1 to 40).map(i=>pool.submit(new Runnable {def run():Unit=log.append(Data.mapper.createObjectNode().put("request",i).put("status","completed"))})).foreach(_.get())
      finally {pool.shutdown();log.close()}
      val files=Files.list(dir);val paths=try files.iterator.asScala.toVector finally files.close()
      assert(paths.size>1)
      val lines=paths.flatMap(p=>Files.readAllLines(p).asScala)
      assert(lines.size==40)
      assert(lines.map(l=>Data.mapper.readTree(l).path("request").asInt).toSet==(1 to 40).toSet)
      assert(paths.forall(Files.size(_)<=200))
    }finally Data.deleteTree(dir)
  }
  test("cancelled request threads cannot close the shared access log") {
    val dir=Files.createTempDirectory("access-interrupt-")
    val log=new AccessLog(dir,1024*1024)
    val error=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val worker=new Thread(()=>try {
      Thread.currentThread().interrupt()
      log.append(Data.mapper.createObjectNode().put("request",1))
      assert(Thread.currentThread().isInterrupted)
    }catch{case t:Throwable=>error.set(t)})
    try {
      worker.start();worker.join();assert(error.get()==null)
      log.append(Data.mapper.createObjectNode().put("request",2))
      log.close()
      val files=Files.list(dir);try assert(files.iterator.asScala.flatMap(p=>Files.readAllLines(p).asScala).size==2)finally files.close()
    }finally {log.close();Data.deleteTree(dir)}
  }
  test("mounted provider preserves child namespaces and native capabilities") {
    val dir=Files.createTempDirectory("mounted-contract-");val plugins=new PluginEnvironment()
    val config=Config("localhost",18818,"localhost",Map.empty,dir,dir,dir,pluginEnvironment=Some(plugins))
    try {
      Files.writeString(dir.resolve("a.csv"),"id\n1\n")
      val child=Map[String,Object]("provider"->"directory","source"->Map[String,Object]("root"->dir.toString).asJava).asJava
      val published=config.createDataset(DatasetDefinition("mounted",items=Map("/team/files"->Map("dataset"->child))))
      try {val access=published.open(()=>())
        try {assert(access.list("/").map(_.path)==Seq("/team"));assert(access.describe("/team/files/a.csv").format=="csv");assert(access.count("/team/files/a.csv").contains(1L))}finally access.close()
      }finally published.close()
    }finally {config.close();plugins.close();Data.deleteTree(dir)}
  }
  test("access logs record successful and missing metadata without private paths") {
    val dir=Files.createTempDirectory("access-flight-contract-")
    val allocator=new org.apache.arrow.memory.RootAllocator()
    val config=Config("localhost",18819,"localhost",Map("public"->FileDatasets.mixed(Seq(PublishedItem("/visible",file=Some(dir.resolve("private-secret.csv")))))),dir,dir,dir,accessLogDirectory=Some(dir.resolve("logs")))
    try {
      Files.writeString(dir.resolve("private-secret.csv"),"id\n1\n")
      val producer=new Producer(config,allocator)
      def request(ds:String,path:String):Unit={producer.getFlightInfo(TestCalls.context,org.apache.arrow.flight.FlightDescriptor.path("datasets",ds,path));()}
      request("public","/visible")
      intercept[org.apache.arrow.flight.FlightRuntimeException](request("public","/missing"))
      intercept[org.apache.arrow.flight.FlightRuntimeException](request("denied","/visible"))
      config.close()
      val files=Files.list(dir.resolve("logs"));val text=try files.iterator.asScala.map(p=>Files.readString(p)).mkString finally files.close()
      val entries=text.linesIterator.map(Data.mapper.readTree).toVector
      assert(entries.size==3 && entries.map(_.path("status").asText).toSet==Set("completed","failed"))
      assert(!text.contains("private-secret") && !text.contains(dir.toString))
    }finally {config.close();allocator.close();Data.deleteTree(dir)}
  }
  test("history can use an application-owned store with detached values") {
    class MemoryStore extends ExecutionStore {
      private val data=scala.collection.mutable.Map.empty[String,com.fasterxml.jackson.databind.JsonNode]
      var closed=false
      def get(id:String)=synchronized {data.get(id).map(_.deepCopy[com.fasterxml.jackson.databind.JsonNode]())}
      def records=synchronized {data.values.map(_.deepCopy[com.fasterxml.jackson.databind.JsonNode]()).toVector}
      def create(n:com.fasterxml.jackson.databind.JsonNode):Unit=synchronized {require(!data.contains(n.path("record_id").asText));put(n)}
      def put(n:com.fasterxml.jackson.databind.JsonNode):Unit=synchronized {data(n.path("record_id").asText)=n.deepCopy[com.fasterxml.jackson.databind.JsonNode]()}
      def close():Unit={closed=true}
    }
    val store=new MemoryStore;val history=new ExecutionHistory(store,ownsStore=false)
    val record=history.begin("task","memory");record.update("running");record.update("succeeded")
    assert(history.get("memory").get.path("state").asText=="succeeded")
    history.close();assert(!store.closed)
  }
  test("range reads are bounded and cancellation propagates through wrapped streams") {
    val file=Files.createTempFile("range-contract-",".bin")
    try {
      Files.write(file,Array[Byte](1,2,3,4,5))
      val source=ByteSource.local(file)
      val range=source.readRange(1,2,()=>())
      try assert(range.readAllBytes().toSeq==Seq[Byte](2,3))finally range.close()
      var cancelled=false
      val checked=ByteSource.withCheck(source,()=>if(cancelled)throw new java.util.concurrent.CancellationException())
      val in=checked.open(()=>())
      try {assert(in.read()==1);cancelled=true;intercept[java.util.concurrent.CancellationException](in.read())}finally in.close()
    }finally Files.delete(file)
  }
}
