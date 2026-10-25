package dacp.unionserver

import dacp._
import dacp.protocol.Metadata
import dacp.union.{Json,Rpc,UnionFailure}
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import java.util.concurrent.{Executors,TimeUnit,Semaphore}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** A request-time namespace. Only approved members are readable; no catalog snapshots are cached. */
private[unionserver] final class ProxyCatalog(members:Members,settings:Settings,routes:DatasetRoutes,uploads:StreamInputs) extends DatasetCatalog {
  private val directories=new MemberDatasets(members)
  private val monitor=Executors.newScheduledThreadPool(2)
  private val slots=new Semaphore(32)
  private def hops:Vector[String]={
    val previous=Rpc.proxyHops
    if(previous.contains(members.unionId) || previous.size>=8)throw CallStatus.INVALID_ARGUMENT.withDescription("proxy cycle or depth limit").toRuntimeException
    previous:+members.unionId
  }
  private def generic(t:Throwable):Nothing=t match {
    case _:java.util.concurrent.CancellationException=>throw t
    case f:FlightRuntimeException if f.status.code==FlightStatusCode.NOT_FOUND=>throw CallStatus.NOT_FOUND.withDescription("dataset content not found").toRuntimeException
    case f:FlightRuntimeException if f.status.code==FlightStatusCode.INVALID_ARGUMENT=>throw CallStatus.INVALID_ARGUMENT.withDescription("invalid dataset request").toRuntimeException
    case _=>throw CallStatus.UNAVAILABLE.withDescription("dataset source unavailable").toRuntimeException
  }
  def list():Seq[DatasetInfo]={val result=directory(()=>());if(!result.complete)throw CallStatus.UNAVAILABLE.withDescription("directory incomplete").toRuntimeException;result.entries}
  override def directory(check:()=>Unit):DatasetDirectory=synchronized {
    val deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30)
    val path=hops;val current=members.all;routes.retainMembers(current.map(_.path("identity").asText).toSet)
    var failures=0
    val entries=current.flatMap { member=>
      check()
      try {
        if(System.nanoTime()>deadline)throw new java.util.concurrent.TimeoutException("directory deadline")
        val inspection=members.inspect(member);check()
        if(inspection.path("state").asText!="approved") {failures+=1;Vector.empty}
        else {
          val id=inspection.path("identity").asText
          val values=directories.entries(id,check,path).getOrElse(throw new IllegalStateException("member removed"))
          require(values.size<=10000,"member catalog exceeds limit")
          routes.reconcile(id,values.map(_.getName).toSet)
          values.map { m=>DatasetInfo(routes.assign(id,m.getName),m.getDescription) }
        }
      }catch{case _:java.util.concurrent.CancellationException=>throw new java.util.concurrent.CancellationException();case NonFatal(_)=>failures+=1;Vector.empty}
    }
    DatasetDirectory((entries++uploads.entries(scala.util.Try(Rpc.identity).getOrElse(""))).sortBy(_.name),failures==0,failures)
  }
  private[unionserver] def target(name:String,check:()=>Unit=()=>()):(com.fasterxml.jackson.databind.JsonNode,String)={
    val row=routes.resolve(name).getOrElse(throw new java.io.FileNotFoundException("dataset not found"))
    val member=members.all.find(_.path("identity").asText==row.path("member").asText).getOrElse(throw new java.io.FileNotFoundException("dataset not found"))
    check()
    val state=members.inspect(member).path("state").asText
    if(state!="approved")throw CallStatus.UNAVAILABLE.withDescription("dataset source unavailable").toRuntimeException
    check();(member,row.path("local_name").asText)
  }
  def open(name:String):PublishedDataset=open(name,()=>())
  override def open(name:String,check:()=>Unit):PublishedDataset={
    if(uploads.contains(name))return uploads.open(name)
    val path=hops
    val(member,local)=target(name,check)
    new PublishedDataset {
      def open(active:()=>Unit):DatasetAccess=new DatasetAccess {
        private val stopped=new java.util.concurrent.atomic.AtomicBoolean()
        private val readers=java.util.concurrent.ConcurrentHashMap.newKeySet[FlightStream]()
        @volatile private var validatedAt=0L
        private def ready():Unit={
          check();active();if(stopped.get())throw new java.util.concurrent.CancellationException()
          if(System.currentTimeMillis()-validatedAt>=1000)synchronized {
            if(System.currentTimeMillis()-validatedAt>=1000){target(name,check);validatedAt=System.currentTimeMillis()}
          }
        }
        private val client=members.remote.dataClient(member.path("url").asText,Some(member.path("identity").asText))
        private def options=Array[CallOption](CallOptions.timeout(30,TimeUnit.SECONDS),Rpc.hopOptions(path))
        private def info(parts:String*):FlightInfo={ready();try client.getInfo(FlightDescriptor.path(parts:_*),options:_*)catch{case NonFatal(t)=>generic(t)}}
        private def item(m:Metadata.Item):Item={
          LogicalPath.validate(m.getPath)
          Item(m.getPath,m.getFormat,if(m.getArrowSchema.isEmpty)None else Some(org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(java.nio.ByteBuffer.wrap(m.getArrowSchema.toByteArray))),m.getKind,
            if(m.hasLastModifiedTimeMs)Some(m.getLastModifiedTimeMs)else None,if(m.hasTotalSizeBytes)Some(m.getTotalSizeBytes)else None,if(m.hasRecordCount)Some(m.getRecordCount)else None,Some(m.getViewsList.asScala.toVector))
        }
        def list():Seq[Item]=Metadata.DataSet.parseFrom(info("datasets",local).getAppMetadata).getItemsList.asScala.map(item).toVector
        def list(p:String):Seq[Item]={LogicalPath.validate(p);Metadata.DataSet.parseFrom(info("datasets",local,"list",p).getAppMetadata).getItemsList.asScala.map(item).toVector}
        def describe(p:String):Item={LogicalPath.validate(p);item(Metadata.Item.parseFrom(info("datasets",local,p).getAppMetadata))}
        def count(p:String):Option[Long]={LogicalPath.validate(p);val m=Metadata.Item.parseFrom(info("datasets",local,"count",p).getAppMetadata);if(m.hasRecordCount)Some(m.getRecordCount)else None}
        def stream(p:String,view:String,a:BufferAllocator,out:FlightProducer.ServerStreamListener,demand:()=>Unit):Unit=read(p,view,None,out,demand)
        override def scan(p:String,scan:Scan,a:BufferAllocator,out:FlightProducer.ServerStreamListener,demand:()=>Unit):Unit=read(p,"records",Some(scan),out,demand)
        private def read(p:String,view:String,scan:Option[Scan],out:FlightProducer.ServerStreamListener,demand:()=>Unit):Unit={
          LogicalPath.validate(p);ready()
          if(!slots.tryAcquire())throw CallStatus.RESOURCE_EXHAUSTED.withDescription("proxy download capacity reached").toRuntimeException
          var reader:FlightStream=null;var guard:java.util.concurrent.ScheduledFuture[_]=null
          try {
            val ticket=Metadata.Ticket.newBuilder().setDataset(local).setItem(p).setView(view);scan.foreach(s=>ticket.setScan(ScanProtocol.encode(s)))
            reader=client.getStream(new Ticket(ticket.build().toByteArray),CallOptions.timeout(settings.timeout,TimeUnit.SECONDS),Rpc.hopOptions(path));readers.add(reader)
            val current=reader
            guard=monitor.scheduleWithFixedDelay(new Runnable {def run():Unit=try {ready();if(out.isCancelled)current.cancel("proxy consumer cancelled",null)}catch{case NonFatal(_)=>current.cancel("proxy cancelled",null)}},0,50,TimeUnit.MILLISECONDS)
            def start():Unit={val dictionaries=reader.getDictionaryProvider;out.start(reader.getRoot,if(dictionaries==null || dictionaries.getDictionaryIds.isEmpty)null else dictionaries,org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT)}
            var started=false
            while({ready();demand();reader.next()}) {
              if(!started){start();started=true}
              ready();demand();out.putNext()
            }
            if(!started)start()
            out.completed()
          }catch{case NonFatal(t)=>generic(t)}finally {if(guard!=null)guard.cancel(false);if(reader!=null){reader.cancel("proxy read ended",null);try reader.close()finally readers.remove(reader)};slots.release()}
        }
        override def close():Unit={stopped.set(true);readers.asScala.foreach(_.cancel("dataset closed",null))}
      }
    }
  }
  override def close():Unit=monitor.shutdownNow()
}
