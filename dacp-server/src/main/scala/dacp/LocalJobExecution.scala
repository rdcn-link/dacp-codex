package dacp

import com.fasterxml.jackson.databind.JsonNode
import dacp.union.Json
import scala.jdk.CollectionConverters._

/** Single-node DAG execution. Storage and operator implementations remain plugins. */
private[dacp] final class LocalJobExecution(config:Config,executor:TaskExecutor,
  source:(InputBinding,StreamCancel)=>FrameSource,allocator:org.apache.arrow.memory.BufferAllocator) {
  def run(plan:JobPlan,policy:ExecutionPolicy,owner:String,record:ExecutionHistory#Record,
    replay:Option[String],emit:OperatorFrame=>Unit,root:StreamCancel):Unit={
    val store=config.materializationStore.map(config.openMaterialization)
    require(policy.materialized.isEmpty || store.nonEmpty,"materialization_store is not configured")
    store.foreach(_.cleanup())
    val connected=ExecutionPolicy.connected(plan)
    val outgoing=connected.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
    def signature(t:Task):JsonNode={
      val n=Json.obj().put("id",t.id).put("operator",t.operator)
      n.set[JsonNode]("params",Data.mapper.valueToTree(t.params))
      t.source.foreach(x=>n.putObject("source").put("runtime",x.runtime).put("source",x.source).put("entrypoint",x.entrypoint))
      val ins=n.putObject("inputs");t.inputs.toVector.sortBy(_._1).foreach{case(p,b)=>val v=ins.putObject(p).put("port",b.port).put("view",b.view);b.uri.foreach(v.put("uri",_));b.task.foreach(v.put("task",_))};n
    }
    val reusable=scala.collection.mutable.Map.empty[String,String]
    def artifact(t:Task,execution:String):Option[MaterializedTask]=store.flatMap(_.get(ExecutionPolicy.key(execution,t.id))).map { a=>
      try {
        require(a.metadata.path("owner").asText==owner,"FORBIDDEN: materialized output owner differs")
        require(a.metadata.path("signature")==signature(t),"replay task differs from materialized task")
        a
      }catch{case e:Throwable=>a.close();throw e}
    }
    def discover(execution:String):Unit=plan.tasks.foreach { t=>
      val ports=outgoing.getOrElse(t.id,Set.empty)
      if(ports.nonEmpty && ports.forall(p=>policy.materialized(t.id->p))) artifact(t,execution).foreach { a=>
        try {if(ports.subsetOf(a.metadata.path("ports").elements.asScala.map(_.asText).toSet))reusable(t.id)=execution} finally a.close()
      }
    }
    replay.foreach { previous=>
      val history=config.executionHistory.get(previous).get
      history.path("materialized_sources").fields.asScala.foreach { e=>
        val t=plan.tasks.find(_.id==e.getKey).get
        val ports=outgoing.getOrElse(t.id,Set.empty)
        if(ports.nonEmpty && ports.forall(p=>policy.materialized(t.id->p)))
          artifact(t,e.getValue.asText).foreach(a=>try {if(ports.subsetOf(a.metadata.path("ports").elements.asScala.map(_.asText).toSet))reusable(t.id)=e.getValue.asText}finally a.close())
      }
      require(reusable.nonEmpty,"no complete unexpired materialized tasks available for replay") }
    val attempts=Json.mapper.createArrayNode()
    var retry=0
    var attempt=Json.uuid()
    while(true){
      root.active()
      val token=new StreamCancel(()=>{root.checkState();false},policy.timeoutSeconds.getOrElse(config.executionTimeoutSeconds))
      root.add(token)
      val attemptReuse=reusable.toMap
      val threads=scala.collection.mutable.ArrayBuffer.empty[Thread]
      val committed=java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
      val attemptRecord=attempts.addObject().put("attempt_id",attempt).put("retry_index",retry).put("state","running").put("started_at_ms",Json.now)
      val details=Json.obj().put("attempt_id",attempt).put("retry_count",retry).put("max_retries",policy.maxRetries)
      details.set[JsonNode]("attempts",attempts)
      record.update(if(retry==0)"running"else "retrying",details)
      try {
        var needed=Set.empty[String]
        def visit(id:String):Unit=if(!needed(id)){needed+=id;if(!attemptReuse.contains(id))Jobs.dependencies(plan.tasks.find(_.id==id).get).foreach(visit)}
        Jobs.roots(plan).foreach(visit)
        val graph=Json.obj();val skipped=graph.putArray("skipped_tasks");plan.tasks.filterNot(t=>needed(t.id)).foreach(t=>skipped.add(t.id));record.update("running",graph)
        val edges=plan.tasks.filter(t=>needed(t.id) && !attemptReuse.contains(t.id)).flatMap(t=>t.inputs.toVector.flatMap{case(p,b)=>b.task.map(up=>(up,b.port,t.id,p))})
        require(edges.map(e=>e._1->e._2).distinct.size==edges.size,"fan-out requires an explicit duplicating operator")
        require(plan.outputs.values.toSet.size==plan.outputs.size && !plan.outputs.values.exists(p=>edges.exists(e=>(e._1,e._2)==p)),"output is consumed more than once")
        require(plan.tasks.count(t=>needed(t.id) && t.operator!=Jobs.ReadItem)<=config.maxConcurrentTasks,"pipeline needs more local task slots; increase max_concurrent_tasks")
        val queues=edges.map(e=>(e._3,e._4)->new FrameQueue(token)).toMap
        val terminal=new FrameQueue(token)
        val completed=java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
        plan.tasks.filter(t=>needed(t.id)).foreach { t=>
          val child=config.executionHistory.begin("job_task",metadata=Json.obj().put("parent_execution_id",record.id).put("task_id",t.id).put("attempt_id",attempt).put("operator",t.operator),state="waiting_inputs")
          val stepDetails=Json.obj();t.title.foreach(stepDetails.put("title",_));stepDetails.set[JsonNode]("inputs",signature(t).path("inputs"));child.update("waiting_inputs",stepDetails)
          threads+=StreamProtocol.thread(token){
            val counters=new java.util.concurrent.ConcurrentHashMap[String,UnionPortMetrics]()
            def observe(direction:String,port:String,f:OperatorFrame):Unit=counters.computeIfAbsent(direction+"/"+port,_=>new UnionPortMetrics(allocator)).observe(f,remote=false)
            def metrics:com.fasterxml.jackson.databind.node.ObjectNode={val n=Json.obj();val ps=n.putObject("ports");counters.asScala.foreach{case(k,v)=>ps.set[JsonNode](k,v.snapshot)};n}
            val selected=policy.materialized.collect{case(id,p)if id==t.id=>p}
            val metadata=Json.obj().put("owner",owner);metadata.set[JsonNode]("signature",signature(t));val ps=metadata.putArray("ports");selected.toVector.sorted.foreach(ps.add)
            val writer=if(selected.nonEmpty && !attemptReuse.contains(t.id))Some(token.add(store.get.begin(ExecutionPolicy.key(attempt,t.id),metadata,selected)))else None
            val begun=scala.collection.mutable.Set.empty[String];val ended=scala.collection.mutable.Set.empty[String]
            def output(f:OperatorFrame):Unit={
              token.active();val h=StreamProtocol.header(f);val port=h.path("port").asText
              begun.synchronized { h.path("type").asText match {
                case "start"=>require(!begun(port),"duplicate output start");begun+=port
                case "data"=>require(begun(port) && !ended(port),"invalid output data")
                case "end"=>require(begun(port) && !ended(port),"invalid output end");ended+=port
                case _=>throw new IllegalArgumentException("invalid operator output event")
              }
              }
              observe("outputs",port,f)
              if(selected(port))writer.foreach(_.append(port,OperatorFrames.encode(f)))
              edges.find(e=>e._1==t.id && e._2==port) match {
                case Some(e)=>queues(e._3,e._4).put(StreamProtocol.rename(f,e._4))
                case None=>val label=plan.outputs.find(_._2==(t.id,port)).map(_._1)
                  label.foreach(p=>terminal.put(StreamProtocol.rename(f,p,t.id)))
              }
            }
            try {
              child.update(if(attemptReuse.contains(t.id))"replaying"else "running")
              attemptReuse.get(t.id) match {
                case Some(previous)=>
                  val a=token.add(artifact(t,previous).getOrElse(throw new IllegalArgumentException("materialized output expired")))
                  try {
                    val readers=outgoing(t.id).toVector.map(p=>StreamProtocol.thread(token){
                      val in=token.add(a.open(p))
                      try {
                        var end=Option.empty[OperatorFrame];var f=in.next()
                        while(f.nonEmpty){
                          val decoded=OperatorFrames.decode(f.get);val h=StreamProtocol.header(decoded)
                          require(h.path("port").asText==p && end.isEmpty,"invalid replay port or events after end")
                          if(h.path("type").asText=="end")end=Some(decoded)else output(decoded)
                          f=in.next() // Verify checksum and EOF before downstream observes end.
                        }
                        end.foreach(output)
                      }finally in.close()
                    })
                    readers.foreach(waitFor(_,token))
                  }finally a.close()
                case None=>
                  val inputs=scala.collection.mutable.Map.empty[String,FrameSource]
                  val acquired=t.operator!=Jobs.ReadItem
                  if(acquired)require(config.taskSlots.tryAcquire(),"node task capacity reached")
                  try {
                    t.inputs.foreach{case(p,b)=>
                      val raw=b.uri.map(_=>source(b,token)).getOrElse(queues(t.id,p))
                      inputs(p)=new FrameSource {
                        def next():Option[OperatorFrame]={val f=raw.next();f.foreach(observe("inputs",p,_));f}
                        def close():Unit=raw.close()
                      }
                    }
                    Jobs.readBinding(t) match {
                      case Some(b)=>val stream=token.add(source(b,token));try {var f=stream.next();while(f.nonEmpty){output(StreamProtocol.rename(f.get,"result"));f=stream.next()}}finally stream.close()
                      case None=>executor.execute(t,inputs.toMap,output,token)
                    }
                  }finally {inputs.values.foreach(_.close());if(acquired)config.taskSlots.release()}
              }
              require(outgoing.getOrElse(t.id,Set.empty).subsetOf(ended.toSet),"missing output end")
              token.active();writer.foreach(_.commit());if(writer.nonEmpty){committed.add(t.id);reusable.synchronized {
                reusable(t.id)=attempt
                val saved=Json.obj();val sources=saved.putObject("materialized_sources");reusable.foreach{case(id,ex)=>sources.put(id,ex)};record.update("running",saved)
              }}
              child.update("succeeded",metrics.put("replayed",attemptReuse.contains(t.id)));completed.add(t.id)
            }catch{case e:Throwable=>child.update(token.cause(e) match {case _:java.util.concurrent.TimeoutException=>"expired";case _:java.util.concurrent.CancellationException=>"cancelled";case _=>"failed"},metrics,message=Some(Option(e.getMessage).getOrElse("step failed")));throw e}
            finally writer.foreach(_.close())
          }
        }
        val workers=threads.toVector
        val completion=StreamProtocol.thread(token){workers.foreach(waitFor(_,token));terminal.end()};threads+=completion
        var f=terminal.next();while(f.nonEmpty){emit(f.get);f=terminal.next()};waitFor(completion,token)
        val saved=Json.obj();val sources=saved.putObject("materialized_sources");reusable.foreach{case(t,id)=>sources.put(t,id)};committed.asScala.foreach(t=>sources.put(t,attempt));record.update("running",saved)
        attemptRecord.put("state","succeeded").put("finished_at_ms",Json.now)
        val finalDetails=Json.obj();finalDetails.set[JsonNode]("attempts",attempts);record.update("running",finalDetails)
        return
      }catch{case caught:Throwable=>
        val failure=token.cause(caught)
        attemptRecord.put("state",failure match {case _:java.util.concurrent.TimeoutException=>"expired";case _:java.util.concurrent.CancellationException=>"cancelled";case _=>"failed"}).put("finished_at_ms",Json.now).put("message",Option(failure.getMessage).getOrElse("attempt failed").take(2048))
        val failedDetails=Json.obj();failedDetails.set[JsonNode]("attempts",attempts);record.update("running",failedDetails)
        token.close();threads.foreach(_.join(5000));require(!threads.exists(_.isAlive),"previous attempt did not stop")
        root.active()
        val nonretry=failure.isInstanceOf[IllegalArgumentException] || failure.isInstanceOf[java.util.concurrent.TimeoutException] || failure.isInstanceOf[java.util.concurrent.CancellationException] || Option(failure.getMessage).exists(_.contains("FORBIDDEN"))
        if(retry>=policy.maxRetries || nonretry)throw failure
        discover(attempt)
        val previous=attempt;attempt=Json.uuid();retry+=1
        val h=Json.obj().put("type","attempt_failed").put("port","").put("attempt_id",previous).put("next_attempt_id",attempt).put("retry_count",retry).put("message",Option(failure.getMessage).getOrElse("attempt failed"))
        emit(OperatorFrame(Json.mapper.writeValueAsBytes(h)))
      }finally {token.close();threads.foreach(_.join(5000))}
    }
  }
  private def waitFor(t:Thread,c:StreamCancel):Unit={while(t.isAlive){c.active();t.join(50)};c.active()}
}
