package dacp

import org.apache.arrow.flight.FlightStatusCode._

import dacp.union.{Rpc,Json,Remote,Tls,UnionFailure}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.{CallContext,StreamListener}
import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicBoolean,AtomicLong}
import java.util.concurrent.{ConcurrentHashMap,Executors,TimeUnit}
import org.apache.arrow.memory.{ArrowBuf,BufferAllocator}
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader}
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import scala.jdk.CollectionConverters._

/** Payload counters count each input/output independently, excluding framing/TLS overhead. */
private[dacp] final class UnionPortMetrics(allocator:BufferAllocator) {
  private val bytes=new AtomicLong();private val remoteBytes=new AtomicLong();private val rows=new AtomicLong()
  @volatile private var records=false
  private val last=new AtomicLong()
  def observe(f:OperatorFrame,remote:Boolean):Unit={
    val h=StreamProtocol.header(f);last.set(Json.now)
    if(h.path("type").asText=="start")records=h.path("representation").asText=="records"
    if(h.path("type").asText=="data") {
      bytes.addAndGet(f.data.length);if(remote)remoteBytes.addAndGet(f.data.length)
      if(records){val r=new ArrowStreamReader(new java.io.ByteArrayInputStream(f.data),allocator);try {require(r.loadNextBatch(),"missing records");rows.addAndGet(r.getVectorSchemaRoot.getRowCount)}finally r.close()}
    }
  }
  def summary(h:JsonNode,remote:Boolean=false):Unit={
    last.set(Json.now)
    if(h.path("phase").asText=="start")records=h.path("representation").asText=="records"
    val count=h.path("payload_bytes").asLong;require(count>=0,"invalid group metric")
    bytes.addAndGet(count);if(remote)remoteBytes.addAndGet(count)
    if(h.has("records"))rows.addAndGet(h.path("records").asLong)
  }
  def snapshot:JsonNode={val n=Json.obj().put("payload_bytes",bytes.get).put("remote_payload_bytes",remoteBytes.get).put("last_activity_ms",last.get);if(records)n.put("records",rows.get);n}
}

/** Optional execution transport. Local data never traverses the coordinator. */
final class UnionWorker(config:Config,settings:NodeUnionSettings,allocator:BufferAllocator,
  runner:Option[(Task,Map[String,FrameSource],OperatorFrame=>Unit,StreamCancel)=>Unit]=None) extends AutoCloseable {
  private val tls=new Tls(config.cert,config.key,config.ca)
  private val remote=new dacp.collaboration.CollaborationClient(tls)
  private val membership=new UnionMembership(config.credentials,config.maxConcurrentTasks)
  private val sessions=new ConcurrentHashMap[String,Session]()
  private val materialization=settings.materializationStore.map(d=>config.openMaterialization(d))
  private val executor=new TaskExecutor(config,allocator)
  private val monitor=Executors.newSingleThreadScheduledExecutor()
  private val stopped=new AtomicBoolean()
  private def reserved:Int=config.maxConcurrentTasks-config.taskSlots.availablePermits()
  private def grant(id:String,who:String):JsonNode=membership.member(id,who)
  private def denied(message:String):Nothing=throw UnionFailure(UNAUTHORIZED,message)
  private def validateInput(b:JsonNode,policy:JsonNode):Unit={
    if(b.has("uri")) {
      Json.fields(b,Set("uri","view"),Set("origin","identity"));val uri=Json.text(b,"uri");val u=DacpUri.parse(uri)
      require(u.getPath.stripPrefix("/").split("/",2).length==2,"input must identify an item")
      require(Set("bytes","records")(Json.text(b,"view")),"invalid view")
      if(DacpUri.origin(uri)==DacpUri.origin(config.uri(""))) {
        require(!b.has("origin") && !b.has("identity"),"unexpected local source pin")
      }else {
        require(b.has("origin") && b.has("identity") && DacpUri.origin(uri)==Json.origin(Json.text(b,"origin")),"remote source needs matching origin and identity")
        Json.identity(Json.text(b,"identity"))
      }
    } else {
      Json.fields(b,Set("task","port"),Set("origin","identity"))
      Json.id(Json.text(b,"task"));Json.id(Json.text(b,"port"))
      require(b.has("origin")==b.has("identity"),"remote input needs origin and identity")
      if(b.has("origin")){Json.origin(Json.text(b,"origin"));Json.identity(Json.text(b,"identity"))}
    }
  }
  private def prepare(body:JsonNode,who:String):Session=synchronized {
    Json.fields(body,Set("union_id","execution_id","tasks"),Set("allow_fusion","timeout_seconds"))
    require(!body.has("allow_fusion") || body.path("allow_fusion").isBoolean,"allow_fusion must be boolean")
    val timeout=if(body.has("timeout_seconds"))math.min(Json.number(body,"timeout_seconds",1,86400),settings.executionTimeoutSeconds)else settings.executionTimeoutSeconds
    val union=Json.id(Json.text(body,"union_id"));val id=Json.id(Json.text(body,"execution_id"));val policy=grant(union,who)
    val tasks=Json.array(body,"tasks");require(tasks.nonEmpty && tasks.size<=64,"invalid task count")
    require(tasks.map(t=>Json.text(t,"id")).distinct.size==tasks.size,"duplicate task")
    require(tasks.map(t=>t.path("inputs").size()+t.path("outputs").size()).sum<=1024,"subjob exceeds 1024 connected ports")
    tasks.foreach { t=>
      Json.fields(t,Set("id","operator","params","inputs","outputs"),Set("title","materialize","replay_from","read_source"));Json.id(Json.text(t,"id"))
      if(t.has("title"))require(t.path("title").isTextual && t.path("title").asText.trim.nonEmpty && t.path("title").asText.length<=256 && !t.path("title").asText.exists(_.isControl),"invalid task title")
      val selected=if(t.has("materialize"))Json.strings(t,"materialize")else Vector.empty
      require(selected.distinct.size==selected.size && selected.forall(t.path("outputs").has),"invalid materialization ports")
      if(selected.nonEmpty || t.has("replay_from"))require(materialization.nonEmpty,"node materialization_store is not configured")
      if(t.has("replay_from")){
        val previous=Json.id(Json.text(t,"replay_from"))
        val artifact=materialization.get.get(ExecutionPolicy.key(previous,Json.text(t,"id"))).getOrElse(throw UnionFailure(NOT_FOUND,"materialized task missing or expired"))
        try {
          val m=artifact.metadata
          if(m.path("owner").asText!=who || m.path("union_id").asText!=union)denied("materialized task owner denied")
          require(m.path("signature")==ExecutionPolicy.signature(t),"replay task definition differs from materialized task")
          require(t.path("outputs").fieldNames.asScala.forall(m.path("stored_ports").has),"replay needs all connected outputs materialized")
        }finally artifact.close()
      }
      val op=Json.text(t,"operator")
      if(op==Jobs.ReadItem){
        val source=Task(t.path("id").asText,Map.empty,op,Json.mapper.convertValue(t.path("params"),classOf[java.util.LinkedHashMap[String,Object]]))
        val binding=Jobs.readBinding(source).get
        require(t.path("inputs").isObject && t.path("inputs").size()==0,"read_item has no input ports")
        if(DacpUri.origin(binding.uri.get)==DacpUri.origin(config.uri(""))){
          require(!t.has("read_source"),"local read_item must not have a remote source")
        }else {
          val source=t.path("read_source");Json.fields(source,Set("origin","identity"))
          require(Json.text(source,"identity")==who && Json.origin(Json.text(source,"origin"))==DacpUri.origin(binding.uri.get),"remote read_item must use the scheduler's pinned stream namespace")
        }
      }
      require(t.path("params").isObject && t.path("inputs").isObject && t.path("inputs").size()<=32 && t.path("outputs").isObject && t.path("outputs").size()<=32,"invalid task ports")
      t.path("inputs").fields.asScala.foreach(e=>{Json.id(e.getKey);validateInput(e.getValue,policy)})
      t.path("outputs").fields.asScala.foreach(e=>{Json.id(e.getKey);Json.identity(e.getValue.asText)})
    }
    tasks.filterNot(_.has("replay_from")).foreach(t=>t.path("inputs").elements.asScala.filter(b=> !b.has("uri") && !b.has("origin")).foreach { b=>
      val parent=tasks.find(_.path("id").asText==b.path("task").asText).getOrElse(throw new IllegalArgumentException("unknown local upstream task"))
      require(parent.path("outputs").path(b.path("port").asText).asText==tls.identity,"local output consumer mismatch")
    })
    var pending=tasks;var done=Set.empty[String]
    while(pending.nonEmpty){
      val ready=pending.filter(t=>(t.has("replay_from") || t.path("inputs").elements.asScala.filter(b=> !b.has("uri") && !b.has("origin")).forall(b=>done(b.path("task").asText))))
      require(ready.nonEmpty,"cyclic local task DAG");done++=ready.map(_.path("id").asText);pending=pending.filterNot(ready.contains)
    }
    val localEdges=tasks.filterNot(_.has("replay_from")).flatMap(_.path("inputs").elements.asScala.filter(b=> !b.has("uri") && !b.has("origin")).map(b=>(b.path("task").asText,b.path("port").asText)))
    require(localEdges.distinct.size==localEdges.size,"output fan-out unsupported")
    if(sessions.size>=4096)throw UnionFailure(RESOURCE_EXHAUSTED,"execution retention capacity reached")
    if(sessions.containsKey(id))throw UnionFailure(ALREADY_EXISTS,"execution already exists")
    val own=sessions.values.asScala.filter(s=> !s.terminal && s.owner==who && s.union==union).map(_.size).sum
    if(reserved+tasks.size>config.maxConcurrentTasks || own+tasks.size>policy.path("max_concurrent_tasks").asInt)throw UnionFailure(RESOURCE_EXHAUSTED,"node capacity unavailable")
    if(!config.taskSlots.tryAcquire(tasks.size))throw UnionFailure(RESOURCE_EXHAUSTED,"node capacity unavailable")
    try {val s=new Session(id,union,who,tasks,body.path("allow_fusion").asBoolean(false),timeout);sessions.put(id,s);s}catch{case scala.util.control.NonFatal(t)=>config.taskSlots.release(tasks.size);throw t}
  }
  private def find(id:String,who:String,owner:Boolean=true):Session={
    val s=Option(sessions.get(Json.id(id))).getOrElse(throw UnionFailure(NOT_FOUND,"execution unavailable"))
    grant(s.union,s.owner)
    if(owner && s.owner!=who)denied("execution owner denied")
    s
  }
  def doAction(ctx:CallContext,a:Action,out:StreamListener[Result]):Unit=Rpc.action(ctx,a,out) { (op,n,who)=>
    {
      val union=if(n.has("union_id"))Json.text(n,"union_id")else find(Json.text(n,"execution_id"),who).union
      Rpc.requireUnion(union)
    }
    op match {
      case "membership"=>
        Json.fields(n,Set("union_id"));val id=Json.text(n,"union_id");Rpc.requireUnion(id)
        membership.member(id,who).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]().put("node_identity",tls.identity).put("data_origin",DacpUri.origin(config.uri("")))
      case "node_status"=>
        Json.fields(n,Set("union_id"));val id=Json.text(n,"union_id");val policy=grant(id,who)
        val ownReserved=sessions.values.asScala.filter(s=> !s.terminal && s.union==id && s.owner==who).map(_.size).sum
        val result=Json.obj().put("node_identity",tls.identity).put("data_origin",DacpUri.origin(config.uri(""))).put("reserved_tasks",reserved).put("capacity",config.maxConcurrentTasks).put("union_reserved_tasks",ownReserved).put("union_capacity",policy.path("max_concurrent_tasks").asInt).put("started_at_ms",java.lang.management.ManagementFactory.getRuntimeMXBean.getStartTime).put("uptime_ms",java.lang.management.ManagementFactory.getRuntimeMXBean.getUptime).put("observed_at_ms",Json.now)
        result.set[JsonNode]("resources",NodeResources.snapshot())
        result.put("web_enabled",config.runningWebUrl.nonEmpty);config.runningWebUrl.foreach(result.put("web_url",_))
        val ids=result.putArray("executors");config.plugins.services(classOf[OperatorExecutor]).foreach(x=>ids.add(x.id));result
      case "materialized_status"=>
        Json.fields(n,Set("union_id","execution_id","task"));val union=Json.id(Json.text(n,"union_id"));grant(union,who)
        val result=Json.obj().put("committed",false)
        materialization.flatMap(_.get(ExecutionPolicy.key(Json.text(n,"execution_id"),Json.text(n,"task")))).foreach { a=>
          try {val m=a.metadata;if(m.path("owner").asText!=who || m.path("union_id").asText!=union)denied("materialized task owner denied")
            result.put("committed",true).put("expires_at_ms",m.path("expires_at_ms").asLong);result.set[JsonNode]("ports",m.path("stored_ports"))
          }finally a.close()
        };result
      case "prepare"=>prepare(n,who).snapshot
      case "start"|"execution_status"|"cancel_execution"=>
        Json.fields(n,Set("execution_id"));val id=Json.text(n,"execution_id")
        val session=if(op=="cancel_execution"){
          val s=Option(sessions.get(Json.id(id))).getOrElse(throw UnionFailure(NOT_FOUND,"execution unavailable"))
          if(s.owner!=who)denied("execution owner denied");s
        }else find(id,who)
        if(op=="start")session.start() else if(op=="cancel_execution")session.stop("cancelled")
        session.snapshot
      case _=>throw UnionFailure(UNIMPLEMENTED,"unknown Union action")
    }
  }
  def getStream(ctx:CallContext,ticket:Ticket,listener:ServerStreamListener):Unit=Rpc.stream(ctx,ticket,listener) { (n,who)=>
    Json.fields(n,Set("type","execution_id","task","port"));require(Json.text(n,"type")==Rpc.Prefix+"output","unknown Union ticket")
    val session=find(Json.text(n,"execution_id"),who,owner=false)
    val out=session.output(Json.id(Json.text(n,"task")),Json.id(Json.text(n,"port")));out.claim(who)
    listener.setOnCancelHandler(()=>if(!out.delivered.get())session.stop("cancelled"))
    val sink=new Rpc.Sink(allocator,listener,Rpc.Frames)
    try {
      sink.start();var f=out.queue.next()
      while(f.nonEmpty){sink.frame(f.get);f=out.queue.next()}
      sink.frame(StreamProtocol.frame("complete"));out.delivered.set(true);sink.completed()
    }catch{case scala.util.control.NonFatal(t)=>session.stop("failed");throw t}
    finally {sink.close();if(!out.delivered.get())session.stop("cancelled")}
  }
  private final class Output(val consumer:String,c:StreamCancel) {
    val queue=new FrameQueue(c);private val claimed=new AtomicBoolean();val delivered=new AtomicBoolean()
    def claim(who:String):Unit={if(who!=consumer)denied("output consumer denied");if(!claimed.compareAndSet(false,true))throw UnionFailure(ALREADY_EXISTS,"output already consumed")}
  }
  private final class Session(val id:String,val union:String,val owner:String,definitions:Vector[JsonNode],allowFusion:Boolean,timeoutSeconds:Long) {
    private val serviceCredential=if(Rpc.isAuthenticatedCall)Rpc.principal.credentialId else None
    private val proxyPath=Rpc.proxyHops
    val size=definitions.size
    val created=Json.now
    val cancel=new StreamCancel(timeoutSeconds=timeoutSeconds)
    @volatile private var state="prepared"
    private val historyRecord=config.executionHistory.begin("union_execution",metadata=Json.obj().put("execution_id",id).put("union_id",union).put("owner",owner).put("node",DacpUri.origin(config.uri(""))),state="prepared")
    historyRecord.update("prepared",{val n=Json.obj();n.set[JsonNode]("task_definitions",Json.mapper.valueToTree[JsonNode](definitions.asJava));n})
    private val replayArtifacts=scala.collection.mutable.Map.empty[String,MaterializedTask]
    try definitions.filter(_.has("replay_from")).foreach { t=>
      val artifact=materialization.get.get(ExecutionPolicy.key(t.path("replay_from").asText,t.path("id").asText)).getOrElse(throw UnionFailure(NOT_FOUND,"materialized task expired during prepare"))
      replayArtifacts(t.path("id").asText)=cancel.add(artifact)
    }catch{case t:Throwable=>cancel.close();throw t}
    private val taskRecords=definitions.map { t=>
      val metadata=Json.obj().put("execution_id",id).put("parent_record_id",historyRecord.id).put("union_id",union).put("owner",owner).put("node",DacpUri.origin(config.uri(""))).put("task_id",t.path("id").asText).put("operator",t.path("operator").asText)
      if(t.has("title"))metadata.put("title",t.path("title").asText)
      metadata.set[JsonNode]("inputs",t.path("inputs"));metadata.set[JsonNode]("outputs",t.path("outputs"))
      t.path("id").asText->config.executionHistory.begin("union_task",metadata=metadata,state="prepared")
    }.toMap
    @volatile var finished=0L
    private val released=new AtomicBoolean()
    private def releaseSlots():Unit=if(released.compareAndSet(false,true))config.taskSlots.release(size)
    private val started=new AtomicBoolean()
    private val states=new ConcurrentHashMap[String,String]()
    private val metrics=new ConcurrentHashMap[String,UnionPortMetrics]()
    private val outputs=definitions.flatMap(t=>t.path("outputs").fields.asScala.map(e=>(t.path("id").asText,e.getKey)->new Output(e.getValue.asText,cancel))).toMap
    definitions.foreach(t=>states.put(t.path("id").asText,"prepared"))
    def terminal:Boolean=Set("succeeded","failed","cancelled","expired")(state)
    def output(task:String,port:String):Output=outputs.getOrElse((task,port),throw UnionFailure(NOT_FOUND,"output unavailable"))
    def stop(reason:String,message:Option[String]=None):Unit=synchronized {
      try {
        if(!terminal){state=reason;finished=Json.now;states.keySet.asScala.foreach(k=>if(states.get(k)!="succeeded")states.put(k,reason));historyRecord.update(reason,snapshot,message);taskRecords.foreach{case(k,r)=>r.update(states.get(k),snapshot,if(states.get(k)=="succeeded")None else message)}}
      }finally {cancel.fail(new java.util.concurrent.CancellationException(reason));releaseSlots()}
    }
    private def observed(task:String,direction:String,port:String,f:OperatorFrame,remote:Boolean):Unit=
      metrics.computeIfAbsent(s"$task/$direction/$port",_=>new UnionPortMetrics(allocator)).observe(f,remote)
    private def remoteInput(b:JsonNode):FrameSource={
      val in=cancel.add(remote.stream(Json.text(b,"origin"),"output",Json.obj().put("execution_id",id).put("task",Json.text(b,"task")).put("port",Json.text(b,"port")),pin=Some(Json.text(b,"identity")),timeout=timeoutSeconds))
      new FrameSource {
        private var complete=false
        def next():Option[OperatorFrame]={
          if(complete)return None
          cancel.active()
          val f=OperatorFrames.decode(in.next().getOrElse(throw new IllegalArgumentException("remote output truncated")))
          StreamProtocol.header(f).path("type").asText match {
            case "complete"=>require(in.next().isEmpty,"events after completion");complete=true;in.close();None
            case "error"=>throw new IllegalArgumentException("upstream execution failed")
            case _=>Some(f)
          }
        }
        def close():Unit=in.close()
      }
    }
    private def datasetInput(b:JsonNode):FrameSource={
      val uri=Json.text(b,"uri");val parts=DacpUri.parse(uri).getPath.stripPrefix("/").split("/",2);val view=Json.text(b,"view")
      val client=remote.dataClient(Json.text(b,"origin"),Some(Json.text(b,"identity")))
      val info=client.getInfo(FlightDescriptor.path("datasets",parts(0),"/"+parts(1)),CallOptions.timeout(10,TimeUnit.SECONDS))
      val meta=dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata)
      require(meta.getViewsList.contains(view),"source view unavailable")
      val ticket=dacp.protocol.Metadata.Ticket.newBuilder().setDataset(parts(0)).setItem("/"+parts(1)).setView(view).build()
      val reader=client.getStream(new Ticket(ticket.toByteArray),CallOptions.timeout(settings.executionTimeoutSeconds,TimeUnit.SECONDS))
      cancel.add(new AutoCloseable{def close():Unit={reader.cancel("dataset input cancelled",null);reader.close()}})
      new FrameSource {
        private var started=false;private var ended=false;private var pending=Iterator.empty[Array[Byte]]
        def next():Option[OperatorFrame]={
          cancel.active()
          if(!started){started=true;return Some(StreamProtocol.start("",view,meta.getFormat,if(view=="records")Some(reader.getSchema)else None))}
          if(ended)return None
          if(view=="records"){
            if(reader.next())Some(StreamProtocol.frame("data",data=StreamProtocol.batch(reader.getRoot)))else {ended=true;Some(StreamProtocol.frame("end"))}
          }else {
            while(!pending.hasNext){if(!reader.next()){ended=true;return Some(StreamProtocol.frame("end"))};val root=reader.getRoot;pending=(0 until root.getRowCount).iterator.flatMap(i=>root.getVector(0).asInstanceOf[VarBinaryVector].get(i).grouped(65536))}
            Some(StreamProtocol.frame("data",data=pending.next()))
          }
        }
        def close():Unit={reader.cancel("dataset input closed",null);reader.close()}
      }
    }
    private def localInput(b:JsonNode):FrameSource={
      if(b.has("origin"))return datasetInput(b)
      val u=DacpUri.parse(Json.text(b,"uri"));val split=u.getPath.drop(1).split("/",2);val path="/"+split(1);val view=Json.text(b,"view")
      val access=cancel.add(config.openDataset(split(0),()=>cancel.active()))
      val meta=access.describe(path)
      require(meta.kind=="data" && config.plugins.views(meta).contains(view),"local input view unavailable")
      val queue=new FrameQueue(cancel)
      StreamProtocol.thread(cancel){
        var root:VectorSchemaRoot=null
        val listener=new ServerStreamListener {
          def isReady:Boolean=true
          def isCancelled:Boolean=try {cancel.active();false}catch{case _:Throwable=>true}
          def setOnCancelHandler(r:Runnable):Unit=()
          def start(r:VectorSchemaRoot,d:DictionaryProvider,o:IpcOption):Unit={require(d==null,"dictionary input unsupported");root=r;queue.put(StreamProtocol.start("",view,meta.format,if(view=="records")Some(r.getSchema)else None))}
          def putNext():Unit=if(view=="records")queue.put(StreamProtocol.frame("data",data=StreamProtocol.batch(root)))else {
            require(root.getSchema==PluginSchemas.bytes,"invalid bytes view")
            (0 until root.getRowCount).foreach(i=>root.getVector(0).asInstanceOf[VarBinaryVector].get(i).grouped(65536).foreach(b=>queue.put(StreamProtocol.frame("data",data=b))))
          }
          def putNext(b:ArrowBuf):Unit={require(b==null);putNext()}
          def putMetadata(b:ArrowBuf):Unit=require(b==null)
          def error(t:Throwable):Unit=throw t
          def completed():Unit=()
        }
        try {access.stream(path,view,allocator,listener,()=>cancel.active());queue.put(StreamProtocol.frame("end"));queue.end()}finally access.close()
      }
      queue
    }
    private final class TrackedInput(source:FrameSource,task:String,port:String,remote:Boolean) extends FrameSource {
      // Drain and verify the upstream complete marker even after the operator sees end.
      private var ended=false
      def next():Option[OperatorFrame]={
        val f=source.next();f.foreach { x=>observed(task,"inputs",port,x,remote);if(StreamProtocol.header(x).path("type").asText=="end")ended=true };f
      }
      def finish():Unit={require(ended,"input not fully consumed");require(source.next().isEmpty,"events after input end")}
      def close():Unit=source.close()
    }
    @volatile private var runtimePid=0L
    @volatile private var executionMode="isolated_steps"
    @volatile private var fusionReason="not_planned"
    private def grouped():Option[(PreparedOperatorGroup,Map[String,(String,String,JsonNode)],Map[String,(String,String)])]={
      if(definitions.exists(t=>t.has("replay_from") || t.path("materialize").size()>0)){fusionReason="materialization_boundary";return None}
      if(!allowFusion){fusionReason="node_partition_not_fusible";return None}
      if(runner.nonEmpty || definitions.size<2){fusionReason="single_step_or_test_runner";return None}
      val readers=definitions.filter(_.path("operator").asText==Jobs.ReadItem).map(t=>t.path("id").asText->t).toMap
      if(readers.values.exists(t=>t.path("outputs").size()==0 || t.path("outputs").elements.asScala.exists(_.asText!=tls.identity))){fusionReason="selected_transport_source";return None}
      val external=scala.collection.mutable.Map.empty[String,(String,String,JsonNode)]
      val tasks=definitions.filterNot(t=>readers.contains(t.path("id").asText)).map { t=>
        val id=t.path("id").asText
        val bindings=t.path("inputs").fields.asScala.map { e=>
          val original=e.getValue
          val b=readers.get(original.path("task").asText).map { r=>
            require(original.path("port").asText=="result","read_item has only result output")
            val b=Json.obj().put("uri",r.path("params").path("uri").asText).put("view",r.path("params").path("view").asText).put("source_task",r.path("id").asText)
            if(r.has("read_source"))b.put("origin",r.path("read_source").path("origin").asText).put("identity",r.path("read_source").path("identity").asText)
            b
          }.getOrElse(original)
          val binding=if(b.has("uri") || b.has("origin")){
            val marker=config.uri("union","/"+id+"_"+e.getKey);external(marker)=(id,e.getKey,b)
            InputBinding(uri=Some(marker))
          }else InputBinding(task=Some(b.path("task").asText),port=b.path("port").asText)
          e.getKey->binding
        }.toMap
        Task(id,bindings,t.path("operator").asText,Json.mapper.convertValue(t.path("params"),classOf[java.util.LinkedHashMap[String,Object]]),title=Option(t.get("title")).map(_.asText))
      }
      val connected=definitions.filterNot(t=>readers.contains(t.path("id").asText)).flatMap(t=>t.path("outputs").fields.asScala.filter(_.getValue.asText!=tls.identity).map(e=>(t.path("id").asText,e.getKey))).zipWithIndex.map{case(pair,i)=>s"p$i"->pair}.toMap
      OperatorGroups.prepare(config,JobPlan(tasks,connected)) match {
        case Left(reason)=>fusionReason=reason;None
        case Right(group)=>executionMode="shared_process";fusionReason="compatible";Some((group,external.toMap,connected))
      }
    }
    private def startGrouped(group:PreparedOperatorGroup,external:Map[String,(String,String,JsonNode)],selected:Map[String,(String,String)]):Unit={
      StreamProtocol.thread(cancel){
        try {
          val inputs=group.inputs.map {case(port,binding)=>
            val (task,name,b)=external(binding.uri.get)
            val raw=if(b.has("uri")){if(b.has("origin"))datasetInput(b)else localInput(b)}else remoteInput(b)
            val src=if(!b.has("source_task"))raw else new FrameSource {
              val read=b.path("source_task").asText;states.put(read,"running");taskRecords(read).update("running")
              def next():Option[OperatorFrame]={val f=raw.next();f.foreach { x=>observed(read,"outputs","result",x,false);if(StreamProtocol.header(x).path("type").asText=="end"){states.put(read,"succeeded");taskRecords(read).update("succeeded")} };f}
              def close():Unit=raw.close()
            }
            port->new TrackedInput(src,task,name,b.has("origin"))
          }
          def event(h:JsonNode):Unit={
            val id=h.path("task").asText;require(taskRecords.contains(id),"unknown group task")
            if(h.path("type").asText=="task_state"){
              val value=h.path("state").asText;require(Set("running","succeeded")(value),"invalid task state")
              runtimePid=h.path("runtime_pid").asLong
              states.put(id,value);taskRecords(id).update(value,snapshot)
            }else {
              val direction=h.path("direction").asText;val port=h.path("port").asText
              val remote=direction=="outputs" && outputs.get((id,port)).exists(_.consumer!=tls.identity)
              metrics.computeIfAbsent(s"$id/$direction/$port",_=>new UnionPortMetrics(allocator)).summary(h,remote)
            }
          }
          try {
            executor.execute(group.task,inputs,f=>{
              val name=StreamProtocol.header(f).path("port").asText
              val (_,label)=group.outputs(name);val pair=selected(label)
              outputs(pair).queue.put(StreamProtocol.rename(f,pair._2))
            },cancel,Some(group.operator),Some(event))
            inputs.values.foreach(_.finish())
            outputs.values.foreach(_.queue.end())
            synchronized{if(!terminal){state="succeeded";finished=Json.now;historyRecord.update(state,snapshot);releaseSlots()}}
          }finally inputs.values.foreach(_.close())
        }catch{case t:Throwable=>stop("failed",Some(Option(t.getMessage).getOrElse(t.getClass.getSimpleName)));throw t}
      }
    }
    private[dacp] def executionSnapshots:Vector[JsonNode]=sessions.values.asScala.toVector.map(_.snapshot)
  def start():Unit={
      if(!started.compareAndSet(false,true))throw UnionFailure(ALREADY_EXISTS,"execution already started")
      val group=try grouped()catch{case t:Throwable=>stop("failed",Some(Option(t.getMessage).getOrElse("group planning failed")));throw t}
      synchronized {require(state=="prepared","execution cannot start");state="running";historyRecord.update(state,snapshot)}
      group.foreach{case(plan,external,selected)=>startGrouped(plan,external,selected);return}
      val threads=definitions.map { t=>StreamProtocol.thread(cancel){
        val taskId=t.path("id").asText
        try {
          if(t.has("replay_from")){
            states.put(taskId,"replaying");taskRecords(taskId).update("replaying")
            val artifact=replayArtifacts(taskId)
            val replayThreads=t.path("outputs").fieldNames.asScala.toVector.sorted.map { port=>StreamProtocol.thread(cancel){
              val source=cancel.add(artifact.open(port));val out=output(taskId,port)
              try {
                var phase=0;var end=Option.empty[OperatorFrame];var chunk=source.next()
                while(chunk.nonEmpty){
                  cancel.active();val f=OperatorFrames.decode(chunk.get);val h=StreamProtocol.header(f)
                  require(h.path("port").asText==port,"replay port mismatch")
                  h.path("type").asText match {
                    case "start"=>require(phase==0,"invalid replay start");phase=1
                    case "data"=>require(phase==1,"invalid replay data")
                    case "end"=>require(phase==1,"invalid replay end");phase=2
                    case _=>throw new IllegalArgumentException("invalid replay frame")
                  }
                  observed(taskId,"outputs",port,f,out.consumer!=tls.identity);if(h.path("type").asText=="end")end=Some(f)else out.queue.put(f);chunk=source.next()
                }
                require(phase==2,"incomplete replay output");end.foreach(out.queue.put);out.queue.end()
              }finally source.close()
            }}
            replayThreads.foreach(thread=>while(thread.isAlive){cancel.active();thread.join(100)})
            cancel.active()
            states.put(taskId,"succeeded");taskRecords(taskId).update("succeeded",snapshot)
          }else {
          val selected=if(t.has("materialize"))Json.strings(t,"materialize").toSet else Set.empty[String]
          val writer=if(selected.nonEmpty){
            val metadata=Json.obj().put("execution_id",id).put("task",taskId).put("union_id",union).put("owner",owner)
            metadata.set[JsonNode]("signature",ExecutionPolicy.signature(t))
            Some(cancel.add(materialization.get.begin(ExecutionPolicy.key(id,taskId),metadata,selected)))
          }else None
          try {
          states.put(taskId,"waiting_inputs");taskRecords(taskId).update("waiting_inputs")
          val inputs=t.path("inputs").fields.asScala.map { e=>
            val b=e.getValue
            val src=if(b.has("uri"))localInput(b)else if(b.has("origin"))remoteInput(b)else {val out=output(b.path("task").asText,b.path("port").asText);out.claim(tls.identity);out.queue}
            e.getKey->new TrackedInput(src,taskId,e.getKey,b.has("origin"))
          }.toMap
          val bindings=inputs.keys.map(p=>p->InputBinding(uri=Some(config.uri("union","/item")))).toMap
          val task=Task(taskId,bindings,t.path("operator").asText,Json.mapper.convertValue(t.path("params"),classOf[java.util.LinkedHashMap[String,Object]]),title=Option(t.get("title")).map(_.asText),outputPorts=t.path("outputs").fieldNames.asScala.toVector,proxyPath=proxyPath)
          states.put(taskId,"running");taskRecords(taskId).update("running")
          var seen=Set.empty[String];val phases=scala.collection.mutable.Map.empty[String,Int]
          val emit:OperatorFrame=>Unit=f=>{
            val h=StreamProtocol.header(f);val port=h.path("port").asText;seen+=port
            if(selected(port)){
              h.path("type").asText match {
                case "start"=>require(!phases.contains(port),"duplicate materialized start");phases(port)=1
                case "data"=>require(phases.get(port).contains(1),"invalid materialized data")
                case "end"=>require(phases.get(port).contains(1),"invalid materialized end");phases(port)=2
                case _=>throw new IllegalArgumentException("invalid materialized frame")
              }
              writer.get.append(port,OperatorFrames.encode(f))
            }
            outputs.get((taskId,port)).foreach(out=>{observed(taskId,"outputs",port,f,out.consumer!=tls.identity);out.queue.put(f)})
          }
          try {
            Jobs.readBinding(task) match {
              case Some(binding)=>
                val descriptor=Json.obj().put("uri",binding.uri.get).put("view",binding.view)
                val source=if(t.has("read_source")){
                  descriptor.put("origin",t.path("read_source").path("origin").asText).put("identity",t.path("read_source").path("identity").asText)
                  datasetInput(descriptor)
                }else localInput(descriptor)
                try {var f=source.next();while(f.nonEmpty){emit(StreamProtocol.rename(f.get,"result"));f=source.next()}}finally source.close()
              case None=>runner match {case Some(run)=>run(task,inputs,emit,cancel);case None=>executor.execute(task,inputs,emit,cancel)}
            }
            // Worker transports must validate upstream success, not only an end frame.
            inputs.values.foreach { s=>
              // End has been consumed by the executor; the next item is completion/queue EOF.
              s.finish()
            }
            require(t.path("outputs").fieldNames.asScala.toSet.subsetOf(seen),"declared connection port not produced")
            require(selected.forall(p=>phases.get(p).contains(2)),"incomplete materialized outputs")
            cancel.active();writer.foreach(_.commit())
            outputs.filter(_._1._1==taskId).values.foreach(_.queue.end())
            states.put(taskId,"succeeded");taskRecords(taskId).update("succeeded",snapshot)
          } finally inputs.values.foreach(_.close())
          }finally writer.foreach(_.close())
          }
        } catch {case t:Throwable=>stop("failed",Some(Option(t.getMessage).getOrElse(t.getClass.getSimpleName)));throw t}
      }}
      StreamProtocol.thread(cancel){
        threads.foreach(t=>while(t.isAlive){cancel.active();t.join(100)})
        synchronized {if(!terminal){state="succeeded";finished=Json.now;historyRecord.update(state,snapshot);releaseSlots()}}
      }
    }
    def snapshot:JsonNode={
      val n=Json.obj().put("execution_id",id).put("union_id",union).put("state",state).put("created_at_ms",created).put("finished_at_ms",finished).put("execution_mode",executionMode).put("fusion_reason",fusionReason).put("runtime_pid",runtimePid)
      val ts=n.putObject("tasks");states.asScala.toSeq.sortBy(_._1).foreach{case(k,v)=>ts.put(k,v)}
      val artifacts=n.putObject("materialized_tasks")
      definitions.filter(t=>t.has("replay_from") || t.path("materialize").size()>0).foreach { t=>
        val task=t.path("id").asText;val entry=artifacts.putObject(task).put("source_execution_id",t.path("replay_from").asText(id)).put("replayed",t.has("replay_from"))
        entry.put("committed",states.get(task)=="succeeded")
      }
      val ms=n.putObject("ports");metrics.asScala.toSeq.sortBy(_._1).foreach{case(k,v)=>ms.set[JsonNode](k,v.snapshot)};n
    }
    def check():Unit={
      if(Json.now-created>=timeoutSeconds*1000 && !terminal)stop("expired")
      if(!terminal){try {require(serviceCredential.forall(config.credentials.valid),"Union service credential revoked or expired");grant(union,owner);cancel.active()}catch{
        case _:java.util.concurrent.TimeoutException=>stop("expired")
        case scala.util.control.NonFatal(_)=>stop("failed")
      }}
    }
    def close():Unit=cancel.close()
  }
  private[dacp] def executionSnapshots:Vector[JsonNode]=sessions.values.asScala.toVector.map(_.snapshot)
  def start():Unit={
    monitor.scheduleWithFixedDelay(new Runnable{def run():Unit=try {
      sessions.values.asScala.foreach{s=>s.check();if(s.terminal && Json.now-s.finished>settings.retentionSeconds*1000){sessions.remove(s.id,s);s.close()}}
      materialization.foreach(_.cleanup())
    }catch{case scala.util.control.NonFatal(t)=>System.err.println("union worker cleanup failed: "+t.getClass.getSimpleName)}},1,1,TimeUnit.SECONDS)
    println(s"union worker listening on ${config.uri("")}; identity ${tls.identity}")
  }
  def close():Unit=if(stopped.compareAndSet(false,true)){monitor.shutdownNow();sessions.values.asScala.foreach(_.stop("cancelled"));sessions.values.asScala.foreach(_.close());remote.close();()}
}
