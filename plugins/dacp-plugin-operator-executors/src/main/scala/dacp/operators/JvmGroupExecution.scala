package dacp.operators

import _root_.xstream.data._

import dacp._
import dacp.operator.{Operator,OperatorRunner}
import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import java.io._
import java.nio.file.{Files,Path}
import java.net.URLClassLoader
import java.util.concurrent.{ArrayBlockingQueue,CompletableFuture,CountDownLatch,TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector,VectorLoader,VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ArrowStreamWriter}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.ipc.ReadChannel
import scala.jdk.CollectionConverters._

/** One isolated JVM, one SDK instance per step, bounded in-memory Arrow links. */
private[operators] final class JvmGroupExecution(input:Path,output:OutputStream=System.out) {
  private val mapper=new ObjectMapper()
  private val group=mapper.readTree(Files.readAllBytes(input.resolve("group.json")))
  private val steps=group.path("steps").elements.asScala.toVector
  private val failure=new AtomicReference[Throwable]()
  private val threads=new java.util.concurrent.CopyOnWriteArrayList[Thread]()
  private val resources=new java.util.concurrent.ConcurrentLinkedDeque[AutoCloseable]()
  private val allocator=new RootAllocator()
  private val selected=group.path("outputs").fields.asScala.map(e=>(e.getValue.path("task").asText,e.getValue.path("port").asText)->e.getKey).toMap
  private val headers=new CountDownLatch(selected.size)
  private def event(h:JsonNode):Unit=output.synchronized{OperatorFrames.write(output,OperatorFrame(mapper.writeValueAsBytes(h)));output.flush()}
  private def state(task:String,value:String):Unit=event(mapper.createObjectNode().put("type","task_state").put("task",task).put("state",value).put("runtime_pid",ProcessHandle.current().pid()))
  private val recordPorts=java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
  private def observe(task:String,direction:String,port:String,f:OperatorFrame,h:JsonNode):Unit={
    val n=mapper.createObjectNode().put("type","task_metric").put("task",task).put("direction",direction).put("port",port).put("phase",h.path("type").asText).put("payload_bytes",f.data.length)
    val key=task+"/"+direction+"/"+port
    if(h.has("representation")){n.put("representation",h.path("representation").asText);if(h.path("representation").asText=="records")recordPorts.add(key)}
    if(h.path("type").asText=="data" && recordPorts.contains(key)){
      val reader=new ArrowStreamReader(new ByteArrayInputStream(f.data),allocator)
      try{require(reader.loadNextBatch(),"missing record batch");n.put("records",reader.getVectorSchemaRoot.getRowCount)}finally reader.close()
    }
    event(n)
  }
  private def active():Unit={if(Thread.currentThread.isInterrupted)throw new InterruptedException();Option(failure.get()).foreach(throw _)}
  private def fail(t:Throwable):Unit=if(failure.compareAndSet(null,t)){
    resources.iterator.asScala.foreach(r=>try r.close()catch{case _:Throwable=>()});threads.asScala.filterNot(_==Thread.currentThread).foreach(_.interrupt())
  }
  private def thread(body: =>Unit):Thread={val t=new Thread(()=>try body catch{case t:Throwable=>fail(t)},"dacp-in-process");t.setDaemon(true);threads.add(t);t.start();t}
  private def schema(h:JsonNode)=MessageSerializer.deserializeSchema(new ReadChannel(java.nio.channels.Channels.newChannel(new ByteArrayInputStream(java.util.Base64.getDecoder.decode(h.path("schema").asText)))))
  private def validate(c:JsonNode,h:JsonNode):Unit={
    val repr=h.path("representation").asText;require(c.path("representations").elements.asScala.exists(_.asText==repr),"group port representation mismatch")
    val formats=c.path("formats");require(formats.isEmpty || formats.elements.asScala.exists(_.asText==h.path("format").asText),"group port format mismatch")
    if(c.has("schema"))require(schema(h)==schema(mapper.createObjectNode().put("schema",c.path("schema").asText)),"group port schema mismatch")
  }
  private class Link(contract:JsonNode,task:String,port:String) {
    val metadata=new CompletableFuture[JsonNode]()
    val events=new ArrayBlockingQueue[OperatorFrame](2)
    val in=new PipedInputStream(262144);val out=new PipedOutputStream(in)
    resources.add(in);resources.add(out)
    def send(f:OperatorFrame,h:JsonNode):Unit={
      if(h.path("type").asText=="start"){validate(contract,h);require(metadata.complete(h),"duplicate internal metadata")}
      observe(task,"inputs",port,f,h)
      active();while(!events.offer(f,100,TimeUnit.MILLISECONDS))active()
    }
    def next():OperatorFrame={active();var f=events.poll(100,TimeUnit.MILLISECONDS);while(f==null){active();f=events.poll(100,TimeUnit.MILLISECONDS)};f}
    def start():Unit={thread {
      val first=mapper.readTree(next().metadata);val repr=first.path("representation").asText
      val s=if(repr=="records")schema(first)else PluginSchemas.bytes
      val root=VectorSchemaRoot.create(s,allocator);val writer=new ArrowStreamWriter(root,null,out)
      try{writer.start();var ended=false
        while(!ended){val f=next();mapper.readTree(f.metadata).path("type").asText match {
          case "end"=>ended=true
          case "data" if repr=="bytes"=>root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,f.data);root.setRowCount(1);writer.writeBatch()
          case "data"=>val reader=new ArrowStreamReader(new ByteArrayInputStream(f.data),allocator)
            try{require(reader.getVectorSchemaRoot.getSchema==s,"record schema changed");require(reader.loadNextBatch(),"empty record frame");val batch=new VectorUnloader(reader.getVectorSchemaRoot).getRecordBatch;try new VectorLoader(root).load(batch)finally batch.close();writer.writeBatch();require(!reader.loadNextBatch(),"one batch per frame required")}finally reader.close()
          case _=>throw new IllegalArgumentException("invalid internal stream event")
        }};writer.end()
      }finally{writer.close();root.close();out.close()}
    };()}
  }
  def run():Unit={
    val work=Files.createTempDirectory("dacp-in-process-")
    val external=mapper.readTree(Files.readAllBytes(input.resolve("manifest.json"))).elements.asScala.map(n=>n.path("port").asText->n).toMap
    val links=steps.flatMap(s=>s.path("bindings").fields.asScala.filter(_.getValue.has("task")).map { e=>
      val binding=e.getValue;val upstream=(binding.path("task").asText,binding.path("port").asText)
      upstream->(s.path("id").asText,e.getKey,new Link(s.path("inputs").path(e.getKey),s.path("id").asText,e.getKey))
    }).toVector
    require(links.map(_._1).distinct.size==links.size,"internal fan-out unsupported")
    val bySource=links.toMap;val byTarget=links.map{case(_, (task,port,link))=>(task,port)->link}.toMap
    try{
      // Preflight every operator before any input is consumed or any task is invoked.
      val operators=steps.map { s=>
        val loader=new URLClassLoader(Array(Path.of(s.path("artifact").asText).toUri.toURL),getClass.getClassLoader);resources.add(loader)
        val entry=s.path("entrypoint").asText
        val instance=try loader.loadClass(entry+"$").getField("MODULE$").get(null)catch{case _:ClassNotFoundException=>loader.loadClass(entry).getDeclaredConstructor().newInstance()}
        require(instance.isInstanceOf[Operator],"group operator must implement the SDK Operator contract")
        s.path("id").asText->instance.asInstanceOf[Operator]
      }.toMap
      links.foreach(_._2._3.start())
      val tasks=steps.map { step=>thread {
        val id=step.path("id").asText;val directory=Files.createDirectory(work.resolve(id));val manifest=mapper.createArrayNode()
        val streams=step.path("bindings").fields.asScala.map { e=>
          val port=e.getKey;val b=e.getValue
          val (meta,stream)=if(b.has("task")){
            val link=byTarget((id,port));var h:JsonNode=null
            while(h==null){active();try h=link.metadata.get(100,TimeUnit.MILLISECONDS)catch{case _:java.util.concurrent.TimeoutException=>()}}
            (h,link.in:InputStream)
          }else{val h=external(b.path("port").asText);validate(step.path("inputs").path(port),h);val raw=Files.newInputStream(input.resolve(h.path("file").asText));resources.add(raw);(h,raw)}
          val m=meta.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();m.put("port",port).put("file",port+".arrow");manifest.add(m);port->stream
        }.toMap
        Files.write(directory.resolve("manifest.json"),mapper.writeValueAsBytes(manifest));Files.write(directory.resolve("params.json"),mapper.writeValueAsBytes(step.path("params")))
        val ports=step.path("outputs").fieldNames.asScala.toSet;var begun=Set.empty[String];var ended=Set.empty[String]
        state(id,"running")
        OperatorRunner.runStreams(directory,directory.resolve("params.json"),streams,f=>{
          active();val h=mapper.readTree(f.metadata);val port=h.path("port").asText;val key=(id,port)
          h.path("type").asText match {
            case "start"=>require(!begun(port),"duplicate group start");if(ports(port))validate(step.path("outputs").path(port),h);begun+=port
            case "data"=>require(begun(port) && !ended(port),"group data before metadata or after end")
            case "end"=>require(begun(port) && !ended(port),"invalid group end");ended+=port
            case _=>throw new IllegalArgumentException("invalid group output event")
          }
          observe(id,"outputs",port,f,h)
          bySource.get(key).foreach(_._3.send(f,h))
          selected.get(key).foreach { label=>
            val meta=h.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();meta.put("port",label)
            if(h.path("type").asText!="start")while(!headers.await(100,TimeUnit.MILLISECONDS))active()
            output.synchronized{OperatorFrames.write(output,OperatorFrame(mapper.writeValueAsBytes(meta),f.data));output.flush()}
            if(h.path("type").asText=="start")headers.countDown()
          }
        })(operators(id).process)
        require(ended==begun && ports.subsetOf(ended),"incomplete group output")
        state(id,"succeeded")
      }}
      tasks.foreach(t=>{while(t.isAlive){active();t.join(100)}})
      threads.asScala.foreach(t=>while(t.isAlive){active();t.join(100)})
      active()
    }catch{case t:Throwable=>fail(t);throw t}
    finally{
      resources.iterator.asScala.foreach(r=>try r.close()catch{case _:Throwable=>()})
      threads.asScala.foreach(t=>if(t.isAlive)t.interrupt());threads.asScala.foreach(_.join(1000))
      allocator.close();val files=Files.walk(work);try files.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_))finally files.close()
    }
  }
}
