package xstream.designer

import com.fasterxml.jackson.databind.JsonNode
import dacp.{InferenceResult,StructureInference}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

/** Reuse the current canvas's unchanged steps, never share results between browser sessions. */
private[designer] final class InferenceEdits {
  private val sessions=mutable.LinkedHashMap.empty[String,(Long,InferenceEdit)]
  def session(id:Option[String]):Option[InferenceEdit]=synchronized {
    id.map {value=>
      require(value.matches("[a-f0-9]{32}"),"invalid inference session")
      val now=System.currentTimeMillis()
      sessions.filterInPlace((_,entry)=>now-entry._1<30*60*1000)
      val edit=sessions.remove(value).map(_._2).getOrElse(new InferenceEdit)
      sessions(value)=now->edit
      while(sessions.size>32)sessions.remove(sessions.head._1)
      edit
    }
  }
}
private[designer] final class InferenceEdit {
  private case class Entry(key:String,result:InferenceResult,size:Int)
  private val results=mutable.LinkedHashMap.empty[(String,String),Entry]
  private var bytes=0
  def retain(ids:Set[String]):Unit=synchronized {
    results.keys.filterNot(k=>ids(k._1)).toVector.foreach(k=>{bytes-=results.remove(k).get.size})
  }
  def reuse(id:String,phase:String,key:String)(compute: => InferenceResult):InferenceResult=synchronized {
    results.get(id->phase).filter(_.key==key).map(_.result).getOrElse {
      val result=compute
      val size=StructureInference.json(result).toString.getBytes(UTF_8).length
      results.remove(id->phase).foreach(e=>bytes-=e.size)
      // Only the latest result per step/phase, at most 2 MiB per editing session.
      if(!result.diagnostics.exists(_.severity=="error") && size<=2*1024*1024){
        while(results.nonEmpty && bytes+size>2*1024*1024)bytes-=results.remove(results.head._1).get.size
        results(id->phase)=Entry(key,result,size);bytes+=size
      }
      result
    }
  }
}
private[designer] object InferenceEdits {
  def canonical(n:JsonNode):String={
    if(n.isObject)n.fields.asScala.toVector.sortBy(_.getKey).map(e=>StructureInference.mapper.writeValueAsString(e.getKey)+":"+canonical(e.getValue)).mkString("{",",","}")
    else if(n.isArray)n.elements.asScala.map(canonical).mkString("[",",","]")
    else n.toString
  }
  def key(n:JsonNode):String=MessageDigest.getInstance("SHA-256").digest(canonical(n).getBytes(UTF_8)).map(b=>f"${b & 255}%02x").mkString
}
