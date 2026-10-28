package dacp.designer

import com.fasterxml.jackson.databind.JsonNode
import dacp._
import scala.jdk.CollectionConverters._

/** Editing-time inference ignores execution placement. Only read_item uses remote metadata. */
final class DesignerInference(
  operator:(String,InferenceRequest,StreamCancel)=>InferenceResult,
  item:(String,String,StreamCancel)=>StreamDescriptor,
  sampleItem:(String,String,PreviewLimits,StreamCancel)=>Option[OutputSample]=(_,_,_,_)=>None) {
  def infer(job:JsonNode,cancel:StreamCancel,preview:Option[PreviewLimits]=None,onStep:(String,InferenceResult)=>Unit=(_,_)=>()):JsonNode={
    // Reserve half of the 256 KiB response budget for selected-output copies.
    var sampleBudget=131072
    def binding(n:JsonNode):(String,String)={val p=n.asText.split(":",-1);p(0)->(if(p.length==2)p(1)else "result")}
    val nodes=job.path("tasks").elements.asScala.map(t=>InferenceNode(t.path("id").asText,
      t.path("inputs").fields.asScala.map(e=>e.getKey->binding(e.getValue)).toMap,t)).toVector
    val outputs=job.path("outputs").fields.asScala.map(e=>e.getKey->binding(e.getValue)).toMap
    InferenceGraph.propagate(nodes,outputs,collectErrors=true,onStep=onStep){(step,inputs)=>
      cancel.active()
      val reference=step.path("operator").asText
      val params=Option(step.get("params")).getOrElse(StructureInference.mapper.createObjectNode())
      val limits=preview.filter(_=>sampleBudget>=256).map(p=>p.copy(maxBytes=math.min(p.maxBytes,sampleBudget)))
      val result=if(reference=="read_item@1.0"){
        require(inputs.isEmpty,"read_item has no inputs")
        require(params.fieldNames.asScala.toSet.subsetOf(Set("uri","view")),"unsupported read_item parameter")
        val uri=params.path("uri");require(uri.isTextual && uri.asText.nonEmpty,"read_item requires params.uri")
        val view=params.path("view").asText("auto");require(Set("bytes","records","auto")(view) && (!params.has("view") || view!="auto"),"unsupported read_item view")
        val descriptor=item(uri.asText,view,cancel)
        val base=InferenceResult(Map("result"->descriptor))
        limits.map{limit=>
          try OutputSamples.attach(base,sampleItem(uri.asText,descriptor.representations.head,limit,cancel).map("result"->_).toMap,limit)
          catch{
            case e:java.util.concurrent.CancellationException=>throw e
            case e:java.util.concurrent.TimeoutException=>throw e
            case scala.util.control.NonFatal(e)=>base.copy(diagnostics=Vector(InferenceDiagnostic("warning","sample_unavailable",Option(e.getMessage).getOrElse("Sample unavailable"))))
          }
        }.getOrElse(base)
      }else operator(reference,InferenceRequest(inputs,params,limits),cancel)
      val bounded=result.copy(outputs=result.outputs.toVector.sortBy(_._1).map{case(port,d)=>
        val sample=d.sample.filter(_=>sampleBudget>=256).map(s=>s.bounded(PreviewLimits(preview.map(_.maxRows).getOrElse(10),math.min(preview.map(_.maxBytes).getOrElse(16384),sampleBudget))))
        sample.foreach(s=>sampleBudget-=OutputSamples.size(s));port->d.copy(sample=sample)
      }.toMap)
      bounded
    }
  }
}
