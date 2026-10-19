package dacp.unionserver

import org.apache.arrow.flight.FlightStatusCode._

import dacp._
import dacp.union._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.{CallContext,StreamListener,ServerStreamListener}
import org.apache.arrow.memory.RootAllocator
import java.util.concurrent.{ConcurrentHashMap,Executors,TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** One Union, explicit placement, direct peer streams and transient executions. */
final class Coordinator(val settings:Settings,providedPlugins:Option[PluginEnvironment]=None) extends AutoCloseable {
  val members=new Members(settings)
  private val plugins=providedPlugins.getOrElse(new PluginEnvironment(settings.pluginDirectory))
  private val storeDefinition=settings.executionStore.getOrElse(ExecutionStoreDefinition("file",Map("directory"->settings.state.resolve("executions").toString)))
  private[unionserver] val history=new ExecutionHistory(plugins.byId(classOf[ExecutionStoreProvider],storeDefinition.provider).open(storeDefinition.settings,false))
  private val remote=members.remote
  private val jobs=new ConcurrentHashMap[String,Execution]()
  private val observed=new ConcurrentHashMap[String,JsonNode]()
  private val timer=Executors.newScheduledThreadPool(2)
  private val cleanup=Executors.newFixedThreadPool(4)
  private val allocator=new RootAllocator()
  private val producer=new NoOpFlightProducer {
    override def doAction(ctx:CallContext,a:Action,out:StreamListener[Result]):Unit=Rpc.action(ctx,a,out)(action)
    override def getStream(ctx:CallContext,t:Ticket,out:ServerStreamListener):Unit=Rpc.stream(ctx,t,out)((n,who)=>stream(n,who,out))
  }
  private val endpoint=new Endpoint(settings.host,settings.port,members.tls,allocator,producer)
  private val dashboard=settings.web.map(w=>new Dashboard(w,this))
  private val closed=new AtomicBoolean()
  private[unionserver] def memberSnapshots:Vector[JsonNode]=members.all.map { m=>
    Option(observed.get(m.path("identity").asText)).getOrElse(m.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("state","unknown").put("observed_at_ms",0))
  }.sortBy(_.path("url").asText)
  private[unionserver] def jobSnapshots:Vector[JsonNode]=jobs.values.asScala.toVector.sortBy(j=> -j.created).take(100).map { j=>
    val n=j.dashboardSnapshot.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode];n.remove("nodes");n
  }
  private[unionserver] def jobSnapshot(id:String):Option[JsonNode]=history.get(id).orElse(Option(jobs.get(Json.id(id))).map(_.dashboardSnapshot))
  private[unionserver] def historyPage(offset:Int,limit:Int):JsonNode={
    val n=Json.obj().put("offset",offset).put("limit",limit);val records=n.putArray("jobs");history.list(offset,limit).foreach { record=>
      val summary=record.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();summary.remove("nodes");summary.remove("events");records.add(summary)
    };n
  }
  private[unionserver] def overview:JsonNode={
    val n=Json.obj().put("union_id",members.unionId).put("flight_uri",settings.publicUrl).put("at_ms",Json.now)
      .put("poll_interval_ms",settings.pollMs).put("retention_seconds",settings.retention).put("job_count",jobs.size).put("job_limit",100)
      .put("active_jobs",jobs.values.asScala.count(!_.terminal)).put("succeeded_jobs",jobs.values.asScala.count(_.snapshot.path("state").asText=="succeeded"))
    val ms=n.putArray("members");memberSnapshots.foreach(ms.add)
    val js=n.putArray("jobs");jobSnapshots.foreach(js.add);n
  }
  private def denied():Nothing=throw UnionFailure(UNAUTHORIZED,"client identity denied")
  private def owned(id:String,who:String):Execution={val j=Option(jobs.get(Json.id(id))).getOrElse(throw UnionFailure(NOT_FOUND,"job unavailable"));if(j.owner!=who)denied();j}
  private def action(op:String,n:JsonNode,who:String):JsonNode={
    if(!settings.clients(who))denied()
    op match {
      case "members"=>Json.fields(n,Set.empty);val result=Json.obj().put("union_id",members.unionId);val a=result.putArray("members");observed.values.asScala.foreach(a.add);result
      case "submit"=>Json.fields(n,Set("job_yaml"));submit(Jobs.parse(Json.text(n,"job_yaml")),who,Json.text(n,"job_yaml")).snapshot
      case "job_status"|"cancel_job"=>
        Json.fields(n,Set("job_id"));val j=owned(Json.text(n,"job_id"),who)
        if(op=="cancel_job")j.stop("cancelled");j.snapshot
      case _=>throw UnionFailure(UNIMPLEMENTED,"unknown Union action")
    }
  }
  private def stream(n:JsonNode,who:String,listener:ServerStreamListener):Unit={
    if(!settings.clients(who))denied()
    val kind=Json.text(n,"type")
    kind match {
      case "dacp.union.events"=>
        Json.fields(n,Set("type","job_id","after"));val after=Json.number(n,"after",0,Long.MaxValue);val j=owned(Json.text(n,"job_id"),who);j.validateCursor(after)
        val sink=new Rpc.Sink(allocator,listener,Rpc.Events)
        try {sink.start();j.events(after,sink);sink.completed()}finally sink.close()
      case "dacp.union.results"=>
        Json.fields(n,Set("type","job_id"));val j=owned(Json.text(n,"job_id"),who)
        if(!j.claimed.compareAndSet(false,true))throw UnionFailure(ALREADY_EXISTS,"results already consumed")
        val complete=new AtomicBoolean()
        listener.setOnCancelHandler(()=>if(!complete.get())j.stop("cancelled"))
        val sink=new Rpc.Sink(allocator,listener,Rpc.Frames)
        try {
          sink.start();var f=j.output.next()
          while(f.nonEmpty){sink.frame(f.get);f=j.output.next()}
          complete.set(true);sink.completed()
        }finally {sink.close();if(!complete.get())j.stop("cancelled")}
      case _=>throw UnionFailure(UNIMPLEMENTED,"unknown Union ticket")
    }
  }
  private def submit(plan:JobPlan,owner:String,script:String):Execution=synchronized {
    if(jobs.size>=4096 || jobs.values.asScala.count(!_.terminal)>=64)throw UnionFailure(RESOURCE_EXHAUSTED,"union job capacity unavailable")
    val j=new Execution(Json.uuid(),owner,plan,script)
    jobs.put(j.id,j);j.start();j
  }
  private[unionserver] final class Execution(val id:String,val owner:String,plan:JobPlan,script:String) {
    require(plan.outputs.nonEmpty,"explicit final outputs required")
    require(plan.tasks.forall(t=>t.node.nonEmpty && t.source.isEmpty),"union tasks require explicit node and versioned operator")
    private val known=members.all.map(n=>n.path("data_origin").asText->n).toMap
    private val placements=plan.tasks.map(t=>t.id->known.getOrElse(DacpUri.node(t.node.get),throw new IllegalArgumentException("task node is not invited"))).toMap
    private def identity(n:JsonNode)=n.path("identity").asText
    private def url(n:JsonNode)=n.path("url").asText
    private val nodes=placements.values.toVector.groupBy(identity).values.map(_.head).toVector
    private val consumers=mutable.Map.empty[(String,String),String]
    private def connect(key:(String,String),who:String):Unit={require(!consumers.contains(key),"output fan-out unsupported");consumers(key)=who}
    plan.tasks.foreach { t=>t.inputs.values.foreach { b=>
      b.task.foreach(parent=>connect((parent,b.port),identity(placements(t.id))))
      b.uri.foreach(u=>require(DacpUri.origin(u)==placements(t.id).path("data_origin").asText,"raw input must belong to the executing node"))
    }}
    plan.outputs.values.foreach(key=>connect(key,members.tls.identity))
    require(plan.tasks.map(_.inputs.size).sum+consumers.size<=1024,"union plan exceeds 1024 connected ports")
    val cancel=new StreamCancel(timeoutSeconds=settings.timeout)
    val output=new FrameQueue(cancel)
    val claimed=new AtomicBoolean()
    val created=Json.now
    @volatile var finished=0L
    @volatile private var state="queued"
    private val prepared=new ConcurrentHashMap[String,JsonNode]()
    private val nodeStates=new ConcurrentHashMap[String,JsonNode]()
    private val ring=mutable.Queue.empty[JsonNode]
    private var sequence=0L
    private var ringBytes=0L
    private val historyRecord=history.begin("union_job",id,dashboardSnapshot,"queued")
    private def event(kind:String):Unit=synchronized {
      historyRecord.update(state,dashboardSnapshot)
      sequence+=1
      val n=Json.obj().put("sequence",sequence).put("type",kind).put("at_ms",Json.now)
      n.set[JsonNode]("snapshot",snapshot);ring.enqueue(n);ringBytes+=Json.mapper.writeValueAsBytes(n).length
      while(ring.size>128 || ringBytes>4*1024*1024 && ring.size>1)ringBytes-=Json.mapper.writeValueAsBytes(ring.dequeue()).length
      notifyAll()
    }
    def terminal:Boolean=Set("succeeded","failed","cancelled","expired")(state)
    def snapshot:JsonNode={
      val n=Json.obj().put("job_id",id).put("state",state).put("created_at_ms",created).put("finished_at_ms",finished)
      val a=n.putObject("nodes");nodeStates.asScala.foreach{case(k,v)=>a.set[JsonNode](k,v)};n
    }
    def dashboardSnapshot:JsonNode={
      val n=snapshot.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      n.put("job_yaml",script)
      n.put("owner",owner).put("elapsed_ms",math.max(0L,(if(finished>0)finished else Json.now)-created))
      val ts=n.putArray("tasks")
      plan.tasks.foreach { t=>
        val node=placements(t.id);val task=ts.addObject().put("id",t.id).put("node",url(node)).put("node_identity",identity(node)).put("operator",t.operator)
        t.title.foreach(task.put("title",_))
        task.put("state",Option(nodeStates.get(identity(node))).map(_.path("tasks").path(t.id).asText("unknown")).getOrElse(if(state=="queued" || state=="waiting_resources")"waiting"else state))
        val inputs=task.putArray("inputs")
        t.inputs.toVector.sortBy(_._1).foreach {case(port,b)=>
          val input=inputs.addObject().put("port",port)
          b.task match {case Some(parent)=>input.put("task",parent).put("output_port",b.port);case None=>input.put("uri",b.uri.get).put("view",b.view)}
        }
      }
      val outputs=n.putObject("outputs");plan.outputs.foreach{case(label,(task,port))=>outputs.putObject(label).put("task",task).put("port",port)}
      n
    }
    private def change(s:String):Unit=synchronized {if(!terminal){state=s;event("state")}}
    private def request(n:JsonNode,op:String,body:JsonNode):JsonNode=
      remote.action(url(n),op,body,Some(identity(n)))
    private def executionBody:JsonNode=Json.obj().put("execution_id",id)
    private def unionBody:JsonNode=Json.obj().put("union_id",members.unionId)
    private def rollback():Unit=prepared.values.asScala.foreach { n=>
      try remote.action(url(n),"cancel_execution",executionBody,Some(identity(n)),timeout=5)catch{case scala.util.control.NonFatal(_)=>()}
    }
    def stop(reason:String,message:Option[String]=None):Unit={
      var changed=false
      try synchronized {if(!terminal){changed=true;state=reason;finished=Json.now;historyRecord.update(reason,dashboardSnapshot,message);event("state")}}
      finally if(changed){
        // Cleanup must also run when persistence fails or interrupts execution.
        cleanup.execute(new Runnable{def run():Unit=rollback()})
        cancel.fail(new java.util.concurrent.CancellationException(reason))
      }
    }
    private def definition(n:JsonNode):JsonNode={
      val body=Json.obj().put("union_id",members.unionId).put("execution_id",id);val tasks=body.putArray("tasks")
      plan.tasks.filter(t=>identity(placements(t.id))==identity(n)).foreach { t=>
        val j=tasks.addObject().put("id",t.id).put("operator",t.operator)
        t.title.foreach(j.put("title",_))
        j.set[JsonNode]("params",Json.mapper.valueToTree[JsonNode](t.params));val inputs=j.putObject("inputs")
        t.inputs.foreach {case(port,b)=>
          val v=inputs.putObject(port)
          b.uri match {
            case Some(u)=>v.put("uri",u).put("view",b.view)
            case None=>
              val parent=b.task.get;v.put("task",parent).put("port",b.port)
              if(identity(placements(parent))!=identity(n))v.put("origin",url(placements(parent))).put("identity",identity(placements(parent)))
          }
        }
        val outputs=j.putObject("outputs");consumers.foreach {case((task,port),who)=>if(task==t.id)outputs.put(port,who)}
      };body
    }
    private def capacity():Boolean=nodes.forall { n=>
      cancel.active()
      val m=request(n,"membership",unionBody)
      if(m.path("state").asText!="approved" || m.path("expires_at_ms").asLong<=Json.now)throw UnionFailure(UNAUTHORIZED,"member is not approved")
      val wanted=plan.tasks.count(t=>identity(placements(t.id))==identity(n))
      require(wanted<=m.path("max_concurrent_tasks").asInt,"subjob exceeds its authorized task capacity")
      val s=request(n,"node_status",unionBody)
      require(wanted<=s.path("capacity").asInt,"subjob exceeds node capacity")
      s.path("capacity").asInt-s.path("reserved_tasks").asInt>=wanted && s.path("union_capacity").asInt-s.path("union_reserved_tasks").asInt>=wanted
    }
    private def pull(label:String,task:String,port:String):Unit={
      val n=placements(task)
      val in=cancel.add(remote.stream(url(n),"output",Json.obj().put("execution_id",id).put("task",task).put("port",port),pin=Some(identity(n)),timeout=settings.timeout))
      try {
        var complete=false;var ended=false;var started=false
        while(!complete){
          cancel.active();val f=OperatorFrames.decode(in.next().getOrElse(throw new IllegalArgumentException("truncated result")))
          StreamProtocol.header(f).path("type").asText match {
            case "complete"=>require(ended,"result missing end");require(in.next().isEmpty,"events after completion");complete=true
            case "error"=>throw new IllegalArgumentException("node execution failed")
            case "start"=>require(!started && !ended,"duplicate result start");started=true;output.put(StreamProtocol.rename(f,label,task))
            case "data"=>require(started && !ended,"data outside result");output.put(StreamProtocol.rename(f,label,task))
            case "end"=>require(started && !ended,"invalid result end");ended=true;output.put(StreamProtocol.rename(f,label,task))
            case _=>throw new IllegalArgumentException("invalid result frame")
          }
        }
      }finally in.close()
    }
    def start():Unit={
      event("submitted")
      StreamProtocol.thread(cancel){
        try {
          change("waiting_resources")
          while(!capacity()){cancel.active();Thread.sleep(settings.pollMs)}
          change("preparing")
          nodes.foreach { n=>
            cancel.active()
            // Track uncertain prepare requests as well, so cancellation includes them.
            prepared.put(identity(n),n);request(n,"prepare",definition(n));cancel.active()
          }
          nodes.foreach(n=>{cancel.active();request(n,"start",executionBody)})
          change("running")
          val poll=StreamProtocol.thread(cancel){
            while(!terminal){
              nodes.foreach { n=>
                cancel.active()
                try {
                  val s=request(n,"execution_status",executionBody);s.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("observed_at_ms",Json.now);nodeStates.put(identity(n),s)
                  if(Set("failed","cancelled","expired")(s.path("state").asText))throw UnionFailure(ALREADY_EXISTS,"node execution failed")
                }catch{
                  case f:UnionFailure=>throw f
                  case scala.util.control.NonFatal(_)=>nodeStates.put(identity(n),Json.obj().put("state","unknown").put("observed_at_ms",Json.now))
                }
              }
              event("progress");Thread.sleep(settings.pollMs)
            }
          }
          val pulls=plan.outputs.toVector.map{case(label,(task,port))=>StreamProtocol.thread(cancel){pull(label,task,port)}}
          pulls.foreach(t=>while(t.isAlive){cancel.active();t.join(100)})
          while(!nodes.forall(n=>Option(nodeStates.get(identity(n))).exists(_.path("state").asText=="succeeded"))){cancel.active();Thread.sleep(100)}
          cancel.active();output.put(StreamProtocol.frame("complete"));output.end()
          synchronized {if(!terminal){state="succeeded";finished=Json.now;event("state")}}
        }catch{case scala.util.control.NonFatal(t)=>stop(if(cancel.cause(t).isInstanceOf[java.util.concurrent.TimeoutException] || Json.now-created>=settings.timeout*1000)"expired"else "failed",Some(Option(cancel.cause(t).getMessage).getOrElse(t.getClass.getSimpleName)))}
      }
    }
    def validateCursor(after:Long):Unit=synchronized {require(after>=0 && after<=sequence,"invalid event cursor")}
    def events(after:Long,sink:Rpc.Sink):Unit={
      var cursor=after;var done=false
      while(!done){
        sink.active()
        val batch=synchronized {
          require(cursor<=sequence,"event cursor exceeds current sequence")
          if(ring.nonEmpty && cursor<ring.head.path("sequence").asLong-1){
            val gap=Json.obj().put("type","gap").put("sequence",sequence);gap.set[JsonNode]("snapshot",snapshot);Vector(gap)
          }else ring.filter(_.path("sequence").asLong>cursor).toVector
        }
        batch.foreach { n=>sink.event(n);cursor=n.path("sequence").asLong }
        done=synchronized {if(terminal && cursor>=sequence)true else {wait(1000);false}}
      }
    }
    def check():Unit={
      if(!terminal && !nodes.forall(n=>members.all.exists(_.path("identity").asText==identity(n))))stop("cancelled")
      if(!terminal){try cancel.active()catch{case scala.util.control.NonFatal(t)=>stop(if(cancel.cause(t).isInstanceOf[java.util.concurrent.TimeoutException] || Json.now-created>=settings.timeout*1000)"expired"else "failed")}}
    }
    def release():Unit=cancel.close()
  }
  def start():Unit={
    try {endpoint.start();dashboard.foreach(_.start())}catch{case scala.util.control.NonFatal(t)=>close();throw t}
    timer.scheduleWithFixedDelay(new Runnable{def run():Unit={
      try {
        val current=members.all;val ids=current.map(_.path("identity").asText).toSet
        observed.keySet.asScala.filterNot(ids).foreach(observed.remove)
        current.foreach(m=>observed.put(m.path("identity").asText,members.inspect(m)))
      }catch{case scala.util.control.NonFatal(t)=>System.err.println(s"union polling failed: ${t.getClass.getSimpleName}")}
    }},0,settings.pollMs,TimeUnit.MILLISECONDS)
    timer.scheduleWithFixedDelay(new Runnable{def run():Unit=jobs.values.asScala.foreach { j=>
      j.check();if(j.terminal && Json.now-j.finished>settings.retention*1000){jobs.remove(j.id,j);j.release()}
    }},100,100,TimeUnit.MILLISECONDS)
    println(s"union ${members.unionId} listening on ${settings.publicUrl}; identity ${members.tls.identity}")
  }
  def close():Unit=if(closed.compareAndSet(false,true)){
    dashboard.foreach(_.close());timer.shutdownNow();jobs.values.asScala.foreach(_.stop("cancelled"));endpoint.close();jobs.values.asScala.foreach(_.release())
    cleanup.shutdown();if(!cleanup.awaitTermination(5,TimeUnit.SECONDS))cleanup.shutdownNow()
    members.close();history.close();if(providedPlugins.isEmpty)plugins.close();allocator.close()
  }
}
