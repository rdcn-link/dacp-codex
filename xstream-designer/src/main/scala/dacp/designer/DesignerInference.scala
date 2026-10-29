package xstream.designer

import com.fasterxml.jackson.databind.JsonNode
import dacp._
import scala.jdk.CollectionConverters._

/** Editing-time inference ignores execution placement. Only read_item uses remote metadata. */
final class DesignerInference(
  operator:(String,InferenceRequest,StreamCancel)=>InferenceResult,
  item:(String,String,StreamCancel)=>StreamDescriptor,
  sampleItem:(String,String,PreviewLimits,StreamCancel)=>Option[OutputSample]=(_,_,_,_)=>None,
  queryItem:Option[(String,String,Scan,StreamCancel)=>StreamDescriptor]=None,
  querySample:Option[(String,String,Scan,PreviewLimits,StreamCancel)=>Option[OutputSample]]=None,origin:Option[String]=None) {
  private val edits=new InferenceEdits
  def infer(job:JsonNode,cancel:StreamCancel,preview:Option[PreviewLimits]=None,onStep:(String,InferenceResult)=>Unit=(_,_)=>(),editSession:Option[String]=None):JsonNode={
    // Reserve half of the 256 KiB response budget for selected-output copies.
    var sampleBudget=131072
    def binding(n:JsonNode):(String,String)={val p=n.asText.split(":",-1);p(0)->(if(p.length==2)p(1)else "result")}
    val nodes=job.path("tasks").elements.asScala.map(t=>InferenceNode(t.path("id").asText,
      t.path("inputs").fields.asScala.map(e=>e.getKey->binding(e.getValue)).toMap,t)).toVector
    val edit=edits.session(editSession)
    edit.foreach(_.retain(nodes.map(_.id).toSet))
    val fingerprints=scala.collection.mutable.Map.empty[String,String]
    val outputs=job.path("outputs").fields.asScala.map(e=>e.getKey->binding(e.getValue)).toMap
    InferenceGraph.propagate(nodes,outputs,collectErrors=true,onStep=onStep){(step,inputs)=>
      cancel.active()
      val reference=step.path("operator").asText
      val params=Option(step.get("params")).getOrElse(StructureInference.mapper.createObjectNode())
      val limits=preview.filter(_=>sampleBudget>=256).map(p=>p.copy(maxBytes=math.min(p.maxBytes,sampleBudget)))
      val id=step.path("id").asText
      val signature=StructureInference.mapper.createObjectNode().put("operator",reference)
      signature.set[JsonNode]("params",params)
      val bindings=signature.putObject("bindings")
      step.path("inputs").fields.asScala.foreach {e=>
        val (parent,port)=binding(e.getValue)
        bindings.putObject(e.getKey).put("step",parent).put("port",port).put("upstream",fingerprints.getOrElse(parent,""))
      }
      // Fingerprints use resolved parameter values and upstream ancestry, not titles, placement or output selection.
      fingerprints(id)=InferenceEdits.key(signature)
      val request=signature.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      request.set[JsonNode]("inputs",StructureInference.portsJson(inputs))
      limits.foreach(p=>request.set[JsonNode]("preview",OutputSamples.json(p)))
      def compute():InferenceResult={
      val inferred=if(reference=="read_item@1.0"){
        require(inputs.isEmpty,"read_item has no inputs")
        val parameters=StructureInference.mapper.convertValue(params,classOf[java.util.LinkedHashMap[String,Object]])
        val uri=ReadItemParameters.address(parameters,origin)
        require(!uri.startsWith("/"),"Item path requires a configured target service")
        val view=ReadItemParameters.view(parameters)
        val scan=ReadItemParameters.query(parameters)
        val descriptor=scan.map(q=>queryItem.getOrElse(throw new IllegalArgumentException("Item query inference unavailable"))(uri,view,q,cancel)).getOrElse(item(uri,view,cancel))
        val base=InferenceResult(Map("result"->descriptor))
        limits.map{limit=>
          try OutputSamples.attach(base,scan.map(q=>querySample.getOrElse(throw new IllegalArgumentException("Item query preview unavailable"))(uri,descriptor.representations.head,q,limit,cancel)).getOrElse(sampleItem(uri,descriptor.representations.head,limit,cancel)).map("result"->_).toMap,limit)
          catch{
            case e:java.util.concurrent.CancellationException=>throw e
            case e:java.util.concurrent.TimeoutException=>throw e
            case scala.util.control.NonFatal(e)=>base.copy(diagnostics=Vector(InferenceDiagnostic("warning","sample_unavailable",Option(e.getMessage).getOrElse("Sample unavailable"))))
          }
        }.getOrElse(base)
      }else operator(reference,InferenceRequest(inputs,params,limits),cancel)
      cancel.active()
      inferred
      }
      val result=edit.map(_.reuse(id,if(preview.isDefined)"sample"else "schema",InferenceEdits.key(request))(compute())).getOrElse(compute())
      val bounded=result.copy(outputs=result.outputs.toVector.sortBy(_._1).map{case(port,d)=>
        val sample=d.sample.filter(_=>sampleBudget>=256).map(s=>s.bounded(PreviewLimits(preview.map(_.maxRows).getOrElse(10),math.min(preview.map(_.maxBytes).getOrElse(16384),sampleBudget))))
        sample.foreach(s=>sampleBudget-=OutputSamples.size(s));port->d.copy(sample=sample)
      }.toMap)
      bounded
    }
  }
}
