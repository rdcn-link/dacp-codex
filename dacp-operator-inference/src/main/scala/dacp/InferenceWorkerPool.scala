package dacp

import _root_.xstream.data._

import java.util.concurrent.{FutureTask,TimeUnit,TimeoutException,ExecutionException,Executors}
import scala.collection.mutable

case class InferenceWorkerSettings(maxWorkers:Int=4,idleSeconds:Int=60,timeoutSeconds:Int=60) {
  require(maxWorkers>=1 && maxWorkers<=64,"inference_workers.max_workers must be between 1 and 64")
  require(idleSeconds>=1 && idleSeconds<=86400,"invalid inference_workers.idle_seconds")
  require(timeoutSeconds>=1 && timeoutSeconds<=120,"invalid inference_workers.timeout_seconds")
}
object InferenceWorkerSettings {
  def read(raw:Option[Object]):InferenceWorkerSettings=raw.map { v=>
    val n=Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v)
    dacp.union.Json.fields(n,Set.empty,Set("max_workers","idle_seconds","timeout_seconds"))
    def number(k:String,d:Int,max:Int)=if(n.has(k))dacp.union.Json.number(n,k,1,max).toInt else d
    InferenceWorkerSettings(number("max_workers",4,64),number("idle_seconds",60,86400),number("timeout_seconds",60,120))
  }.getOrElse(InferenceWorkerSettings())
}
private[dacp] trait InferenceWorker extends AutoCloseable {def phase:String="inference";def request(bytes:Array[Byte]):Array[Byte]}

/** Keeps processes, never results. A lease covers startup and one serialized request. */
private[dacp] final class InferenceWorkerPool(settings:InferenceWorkerSettings) extends AutoCloseable {
  private class Slot {var busy=true;var used=System.nanoTime();var worker:InferenceWorker=null;var retired=false}
  private val slots=mutable.Map.empty[String,Slot]
  private var closed=false
  private var retiring=0
  private val timer=Executors.newSingleThreadScheduledExecutor((r:Runnable)=>{val t=new Thread(r,"dacp-inference-reaper");t.setDaemon(true);t})
  timer.scheduleWithFixedDelay(new Runnable {def run():Unit=reap()},1,1,TimeUnit.SECONDS)
  private def destroy(slot:Slot):Unit={val w=slot.synchronized{if(slot.retired)None else {slot.retired=true;Option(slot.worker)}};w.foreach(x=>try x.close()catch{case scala.util.control.NonFatal(_)=>()})}
  private def retire(slot:Slot):Unit=try destroy(slot)finally synchronized{retiring-=1;notifyAll()}
  private def remove(key:String,slot:Slot):Unit={
    val tracked=synchronized{if(slots.get(key).contains(slot)){slots.remove(key);retiring+=1;true}else false}
    if(tracked)retire(slot)else destroy(slot)
  }
  private[dacp] def reap():Unit={
    val expired=synchronized{val now=System.nanoTime();val old=slots.toVector.filter{case(_,s)=> !s.busy && now-s.used>=TimeUnit.SECONDS.toNanos(settings.idleSeconds)};old.foreach(p=>slots.remove(p._1));retiring+=old.size;old}
    expired.foreach(p=>retire(p._2))
  }
  def request(key:String,bytes:Array[Byte],cancel:StreamCancel)(create:()=>InferenceWorker):Array[Byte]={
    val deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(settings.timeoutSeconds)
    var slot:Slot=null
    def active():Unit={cancel.active();synchronized{require(!closed,"inference workers are closed")};if(System.nanoTime()>=deadline)throw new TimeoutException("structure inference worker timed out during "+Option(slot).flatMap(s=>s.synchronized{Option(s.worker)}).map(_.phase).getOrElse("worker queue"))}
    while(slot==null){
      active()
      val evicted=synchronized {
        require(!closed,"inference workers are closed")
        slots.get(key) match {
          case Some(s) if !s.busy=>s.busy=true;slot=s;None
          case Some(_)=>wait(25);None
          case None if slots.size+retiring<settings.maxWorkers=>slot=new Slot;slots(key)=slot;None
          case None=>slots.toVector.filterNot(_._2.busy).sortBy(_._2.used).headOption match {
            case Some((k,s))=>slots.remove(k);retiring+=1;Some(s)
            case None=>wait(25);None
          }
        }
      }
      evicted.foreach(retire)
    }
    val lease=slot
    val work=new FutureTask[Array[Byte]](()=>{
      var w=lease.synchronized{Option(lease.worker)}
      if(w.isEmpty){
        val created=create()
        lease.synchronized{if(lease.retired){created.close();throw new java.util.concurrent.CancellationException("inference worker retired")};lease.worker=created}
        w=Some(created)
      }
      w.get.request(bytes)
    })
    val thread=new Thread(work,"dacp-inference-request");thread.setDaemon(true);thread.start()
    try {
      var result:Option[Array[Byte]]=None
      while(result.isEmpty){active();try result=Some(work.get(25,TimeUnit.MILLISECONDS))catch{case _:TimeoutException=>()}}
      active();result.get
    }catch{
      case e:ExecutionException=>remove(key,lease);throw e.getCause
      case t:Throwable=>remove(key,lease);work.cancel(true);throw t
    }finally synchronized {if(slots.get(key).contains(lease)){lease.busy=false;lease.used=System.nanoTime()};notifyAll()}
  }
  def close():Unit={val old=synchronized{closed=true;val all=slots.values.toVector;slots.clear();notifyAll();all};timer.shutdownNow();old.foreach(destroy)}
}
