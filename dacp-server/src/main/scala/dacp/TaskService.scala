package dacp

import java.util.concurrent.{Semaphore,TimeUnit}
import scala.jdk.CollectionConverters._
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import dacp.protocol.Metadata

/** Incremental bidirectional Flight exchange. No retained result handles or dataset registry. */
class TaskService(config: Config,allocator: BufferAllocator,executor: TaskExecutor) extends AutoCloseable {
  private val permits=config.taskSlots
  private val jobs=new Semaphore(config.maxConcurrentTasks)
  private val active=java.util.concurrent.ConcurrentHashMap.newKeySet[StreamCancel]()
  private def join(t:Thread,c:StreamCancel):Unit={while(t.isAlive){c.active();t.join(50)};c.active()}
  def exchange(ctx: FlightProducer.CallContext,reader: FlightStream,writer: FlightProducer.ServerStreamListener): Unit = {
    val cancel=new StreamCancel(()=>ctx.isCancelled || writer.isCancelled)
    active.add(cancel)
    val root=VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
    var acquired:Semaphore=null
    var record:ExecutionHistory#Record=null
    var finalState="failed"
    var failureMessage=Option.empty[String]
    val metrics=new java.util.concurrent.ConcurrentHashMap[String,UnionPortMetrics]()
    def metric(direction:String,f:OperatorFrame):Unit={
      val h=StreamProtocol.header(f);val key=direction+"/"+h.path("port").asText
      metrics.computeIfAbsent(key,_=>new UnionPortMetrics(allocator)).observe(f,remote=true)
    }
    val monitor=StreamProtocol.thread(cancel){while(true){cancel.active();Thread.sleep(50)}}
    try {
      val owner=try dacp.union.Rpc.identity catch{case _:dacp.union.UnionFailure=>Option(ctx.peerIdentity()).getOrElse("anonymous")}
      record=config.executionHistory.begin("dacp_exchange",metadata=dacp.union.Json.obj().put("node",DacpUri.origin(config.uri(""))).put("owner",owner))
      val bytes=reader.getDescriptor.getCommand
      require(bytes.length<=1024*1024,"request exceeds 1 MiB")
      val request=Metadata.OperatorRequest.parseFrom(bytes)
      writer.start(root)
      def emit(f:OperatorFrame):Unit={metric("outputs",f);StreamProtocol.send(writer,root,f,cancel)}
      if(request.hasJobYaml) {
        require(jobs.tryAcquire(),"job capacity reached");acquired=jobs
        record.update("running",dacp.union.Json.obj().put("request_type","job"))
        coordinate(request.getJobYaml,emit,cancel,record)
      } else if(request.hasTaskJson) {
        require(permits.tryAcquire(),"node task capacity reached");acquired=permits
        val n=Data.mapper.readTree(request.getTaskJson.toByteArray)
        require(n.isObject && Set("id","operator","params","ports","sources").subsetOf(n.fieldNames.asScala.toSet) && n.fieldNames.asScala.toSet.subsetOf(Set("id","operator","params","ports","sources","parent_execution_id","title")),"invalid worker request")
        val details=dacp.union.Json.obj().put("request_type","task").put("task_id",n.path("id").asText)
        Option(n.get("title")).foreach(v=>details.set[com.fasterxml.jackson.databind.JsonNode]("title",v))
        details.put("operator",if(n.path("operator").isObject)"inline:"+n.path("operator").path("runtime").asText else n.path("operator").asText)
        if(n.has("parent_execution_id"))details.put("parent_execution_id",dacp.union.Json.id(dacp.union.Json.text(n,"parent_execution_id")))
        details.set[com.fasterxml.jackson.databind.JsonNode]("sources",n.path("sources"))
        record.update("running",details)
        val sources=n.path("sources").elements.asScala.map(_.asText).toVector
        require(sources.nonEmpty,"source item provenance required")
        val prefixes=if(config.taskInputPrefixes.nonEmpty) config.taskInputPrefixes else config.prefixes
        sources.foreach{uri=>
          val u=DacpUri.parse(uri);require(u.getPath.stripPrefix("/").split("/",2).length==2,"input must be an item")
          require(prefixes.exists(DacpUri.within(uri,_)),"FORBIDDEN: task input URI prefix denied")
        }
        val ports=n.path("ports").elements.asScala.map(_.asText).toVector
        require(ports.nonEmpty && ports.distinct.size==ports.size,"invalid worker ports")
        require(n.path("params").isObject,"params must be a mapping")
        val inline=if(n.path("operator").isObject)Some(Jobs.source(Data.mapper.convertValue(n.path("operator"),classOf[java.util.LinkedHashMap[String,Object]])))else None
        require(inline.isEmpty || config.debug,"FORBIDDEN: source submission requires DEBUG mode")
        val task=Task(n.path("id").asText,ports.map(p=>p->InputBinding(uri=Some(sources.head))).toMap,n.path("operator").asText,
          Data.mapper.convertValue(n.path("params"),classOf[java.util.LinkedHashMap[String,Object]]),source=inline,title=Option(n.get("title")).map(v=>{require(v.isTextual,"title must be a string");v.asText}))
        Jobs.validate(task)
        val queues=ports.map(p=>p->new FrameQueue(cancel)).toMap
        // Every input metadata frame precedes any payload, preventing startup head-of-line blocking.
        var started=Set.empty[String]
        ports.indices.foreach{_=>
          require(reader.next(),"missing input metadata")
          val f=StreamProtocol.receive(reader.getRoot);val h=StreamProtocol.header(f);val port=h.path("port").asText
          require(h.path("type").asText=="start" && queues.contains(port) && !started(port),"invalid input metadata")
          started+=port
          metric("inputs",f);queues(port).put(f)
        }
        val incoming=StreamProtocol.thread(cancel){
          var ended=Set.empty[String]
          while(reader.next()) {
            val f=StreamProtocol.receive(reader.getRoot);val h=StreamProtocol.header(f);val port=h.path("port").asText
            require(queues.contains(port) && !ended(port),"unexpected input port")
            require(Set("data","end")(h.path("type").asText),"invalid input event")
            metric("inputs",f);queues(port).put(f);if(h.path("type").asText=="end") ended+=port
          }
          require(ended==ports.toSet,"input stream truncated")
        }
        executor.execute(task,queues,emit,cancel);join(incoming,cancel)
      } else throw new IllegalArgumentException("missing exchange request")
      emit(StreamProtocol.frame("complete"));writer.completed();finalState="succeeded"
    } catch {
      case t:java.util.concurrent.CancellationException=>finalState="cancelled";failureMessage=Some(Option(t.getMessage).getOrElse("client disconnected"))
      case caught:Throwable=>
        val t=cancel.cause(caught)
        finalState=if(t.isInstanceOf[java.util.concurrent.TimeoutException])"expired"else if(t.isInstanceOf[java.util.concurrent.CancellationException])"cancelled"else "failed"
        failureMessage=Some(Option(t.getMessage).getOrElse(t.getClass.getSimpleName))
        org.slf4j.LoggerFactory.getLogger(classOf[TaskService]).warn("operator exchange failed",t)
        val status=if(Option(t.getMessage).exists(_.contains("FORBIDDEN"))) CallStatus.UNAUTHORIZED else if(t.isInstanceOf[IllegalArgumentException]) CallStatus.INVALID_ARGUMENT else CallStatus.INTERNAL
        if(!writer.isCancelled)writer.error(status.withDescription(Option(t.getMessage).getOrElse("operator execution failed")).toRuntimeException)
    } finally {
      try {
        if(record!=null){val detail=dacp.union.Json.obj();val ports=detail.putObject("ports");metrics.asScala.foreach{case(k,v)=>ports.set[com.fasterxml.jackson.databind.JsonNode](k,v.snapshot)};record.update(finalState,detail,failureMessage)}
      } finally {cancel.close();monitor.interrupt();if(acquired!=null)acquired.release();active.remove(cancel);root.close()}
    }
  }
  private def source(binding:InputBinding,c:StreamCancel): FrameSource = {
    val uri=binding.uri.get;val u=DacpUri.parse(uri);val split=u.getPath.stripPrefix("/").split("/",2)
    val client=c.add(NodeConnection.open(config,allocator,uri))
    val info=client.getInfo(FlightDescriptor.path("datasets",split(0),"/"+split(1)))
    val meta=Metadata.Item.parseFrom(info.getAppMetadata)
    require(meta.getKind=="data" && meta.getViewsList.contains(binding.view),"input must be a data item with the selected view")
    val ticket=Metadata.Ticket.newBuilder().setDataset(split(0)).setItem("/"+split(1)).setView(binding.view).build()
    val stream=c.add(client.getStream(new Ticket(ticket.toByteArray)))
    new FrameSource {
      private var began=false;private var ended=false;private var pending=Iterator.empty[Array[Byte]]
      def next():Option[OperatorFrame]={
        c.active()
        if(!began){began=true;return Some(StreamProtocol.start("",binding.view,meta.getFormat,if(binding.view=="records")Some(stream.getSchema)else None))}
        if(ended)return None
        if(binding.view=="bytes") {
          while(!pending.hasNext) {
            if(!stream.next()){ended=true;return Some(StreamProtocol.frame("end"))}
            val root=stream.getRoot;require(root.getSchema==PluginSchemas.bytes,"invalid byte input schema")
            pending=(0 until root.getRowCount).iterator.flatMap(i=>root.getVector(0).asInstanceOf[VarBinaryVector].get(i).grouped(65536))
          }
          Some(StreamProtocol.frame("data",data=pending.next()))
        } else if(stream.next())Some(StreamProtocol.frame("data",data=StreamProtocol.batch(stream.getRoot)))
        else {ended=true;Some(StreamProtocol.frame("end"))}
      }
      def close():Unit={stream.close();client.close()}
    }
  }
  private def coordinate(yaml:String,emit:OperatorFrame=>Unit,c:StreamCancel,record:ExecutionHistory#Record):Unit={
    val plan=Jobs.parse(yaml)
    val details=dacp.union.Json.obj();val tasks=details.putArray("tasks")
    plan.tasks.foreach { t=>val entry=tasks.addObject().put("id",t.id).put("operator",t.source.map(code=>"inline:"+code.runtime).getOrElse(t.operator));t.title.foreach(entry.put("title",_)) }
    record.update("running",details)
    plan.tasks.foreach(Jobs.authorizeSource(config,_))
    val local=DacpUri.origin(config.uri(""));val allowed=config.taskPeers.map(DacpUri.node).toSet+local
    var targets=Map.empty[String,String];var provenance=Map.empty[String,Vector[String]]
    val edges=plan.tasks.flatMap(t=>t.inputs.toVector.flatMap{case(port,b)=>b.task.map(up=>(up,b.port,t.id,port))})
    require(edges.map(e=>(e._1,e._2)).distinct.size==edges.size,"fan-out requires an explicit duplicating operator; one consumer per output port")
    require(plan.outputs.values.toVector.distinct.size==plan.outputs.size,"duplicate selected output")
    require(!plan.outputs.values.exists(p=>edges.exists(e=>(e._1,e._2)==p)),"a consumed output cannot also be a terminal result")
    val queues=edges.map(e=>(e._3,e._4)->new FrameQueue(c)).toMap
    plan.tasks.foreach{t=>
      val uris=t.inputs.values.toVector.flatMap(b=>b.uri.toVector++b.task.toVector.flatMap(provenance)).distinct
      provenance+=t.id->uris
      val anchor=t.inputs.toVector.sortBy(_._1).head._2
      val target=t.node.map(DacpUri.node).getOrElse(anchor.uri.map(DacpUri.origin).getOrElse(targets(anchor.task.get)))
      require(allowed(target),"FORBIDDEN: task target not in task_peers");targets+=t.id->target
    }
    require(targets.values.count(_==local)<=config.maxConcurrentTasks,"pipeline needs more local task slots; increase max_concurrent_tasks")
    val terminal=new FrameQueue(c)
    val threads=plan.tasks.map{t=>StreamProtocol.thread(c){
      val inputs=t.inputs.map{case(port,b)=>port->b.uri.map(_=>source(b,c)).getOrElse(queues(t.id,port))}
      var seen=Set.empty[String]
      peer(targets(t.id),t,provenance(t.id),inputs,f=>{
        val h=StreamProtocol.header(f);val port=h.path("port").asText
        if(h.path("type").asText=="start")seen+=port
        edges.find(e=>e._1==t.id && e._2==port) match {
          case Some(e)=>queues(e._3,e._4).put(StreamProtocol.rename(f,e._4))
          case None=>
            val label=if(plan.outputs.isEmpty)Some(t.id+"."+port)else plan.outputs.find(_._2==(t.id,port)).map(_._1)
            label.foreach(name=>terminal.put(StreamProtocol.rename(f,name,t.id)))
        }
      },c,record.id)
      val required=edges.filter(_._1==t.id).map(_._2).toSet++plan.outputs.values.filter(_._1==t.id).map(_._2)
      require(required.subsetOf(seen),"unknown upstream/selected output port")
    }}
    val completion=StreamProtocol.thread(c){threads.foreach(join(_,c));terminal.end()}
    var next=terminal.next();while(next.nonEmpty){emit(next.get);next=terminal.next()};join(completion,c)
  }
  private def peer(node:String,t:Task,sources:Vector[String],inputs:Map[String,FrameSource],emit:OperatorFrame=>Unit,c:StreamCancel,parentExecutionId:String):Unit={
    val client=c.add(NodeConnection.open(config,allocator,node))
    val n=Data.mapper.createObjectNode().put("id",t.id).put("operator",t.operator).put("parent_execution_id",parentExecutionId)
    t.title.foreach(n.put("title",_))
    n.set[com.fasterxml.jackson.databind.JsonNode]("params",Data.mapper.valueToTree(t.params))
    t.source.foreach { code =>
      val op=n.putObject("operator");op.put("runtime",code.runtime).put("source",code.source).put("entrypoint",code.entrypoint)
    }
    val ports=n.putArray("ports");inputs.keys.toVector.sorted.foreach(ports.add)
    val src=n.putArray("sources");sources.foreach(src.add)
    val request=Metadata.OperatorRequest.newBuilder().setTaskJson(com.google.protobuf.ByteString.copyFrom(Data.mapper.writeValueAsBytes(n))).build()
    val exchange=c.add(client.doExchange(FlightDescriptor.command(request.toByteArray)))
    val out=exchange.getWriter;val root=VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
    out.start(root)
    def send(f:OperatorFrame):Unit={while(!out.isReady){c.active();Thread.sleep(10)};c.active();root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,OperatorFrames.encode(f));root.setRowCount(1);out.putNext()}
    val feeding=StreamProtocol.thread(c){
      val ordered=inputs.toVector.sortBy(_._1)
      ordered.foreach{case(port,src)=>val first=src.next().getOrElse(throw new IllegalArgumentException("missing input start"));send(StreamProtocol.rename(first,port))}
      ordered.foreach{case(port,src)=>var f=src.next();while(f.nonEmpty){send(StreamProtocol.rename(f.get,port));if(StreamProtocol.header(f.get).path("type").asText=="end")f=None else f=src.next()}}
      out.completed()
    }
    try {
      var completed=false
      while(exchange.getReader.next()) {
        c.active();val f=StreamProtocol.receive(exchange.getReader.getRoot)
        require(!completed,"events after completion")
        if(StreamProtocol.header(f).path("type").asText=="complete")completed=true else emit(f)
      }
      require(completed,"worker stream truncated");join(feeding,c)
    } finally {root.close();exchange.close();client.close();inputs.values.foreach(_.close())}
  }
  def close():Unit=active.iterator.asScala.foreach(_.close())
}
