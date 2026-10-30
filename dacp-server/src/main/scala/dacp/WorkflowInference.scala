package dacp

import _root_.xstream.data._

import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.{Action,FlightDescriptor,CallOptions}
import java.util.concurrent.TimeUnit

/** Pure DAG propagation; Union supplies its own member-specific inference callback. */
object WorkflowInference {
  def propagate(plan:JobPlan)(infer:(Task,Map[String,StreamDescriptor])=>InferenceResult):JsonNode={
    val nodes=plan.tasks.map(t=>InferenceNode(t.id,t.inputs.map{case(port,b)=>
      require(b.task.nonEmpty,"workflow inputs must reference step outputs");port->(b.task.get,b.port)
    },t))
    InferenceGraph.propagate(nodes,plan.outputs)(infer)
  }

  def taskJson(task:Task,inputs:Map[String,StreamDescriptor]):JsonNode={
    val n=StructureInference.json(InferenceRequest(inputs,Data.mapper.valueToTree(task.params))).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    task.source match {
      case None=>n.put("operator",task.operator)
      case Some(s)=>val op=n.putObject("operator");op.put("runtime",s.runtime).put("source",s.source).put("entrypoint",s.entrypoint)
    };n
  }
  def parseTask(n:JsonNode):Task={
    require(n!=null && n.isObject && n.fieldNames.asScala.toSet==Set("operator","inputs","params"),"inference requires operator, inputs and params")
    require(n.path("params").isObject,"inference params must be an object")
    val params=Data.mapper.convertValue(n.path("params"),classOf[java.util.LinkedHashMap[String,Object]])
    val source=if(n.path("operator").isObject)Some(Jobs.source(Data.mapper.convertValue(n.path("operator"),classOf[java.util.LinkedHashMap[String,Object]])))else None
    val t=Task("infer",Map.empty,if(source.isEmpty)n.path("operator").asText else "",params,source=source)
    Jobs.validate(t);t
  }
  def task(config:Config,allocator:BufferAllocator,t:Task,inputs:Map[String,StreamDescriptor],cancel:StreamCancel):InferenceResult={
    Jobs.authorizeSource(config,t)
    Jobs.readBinding(Jobs.resolveReads(JobPlan(Vector(t),Map.empty),config.uri("")).tasks.head) match {
      case Some(binding)=>
        require(inputs.isEmpty,"read_item has no inputs")
        require(config.taskInputPrefixes.isEmpty || config.taskInputPrefixes.exists(DacpUri.within(binding.uri.get,_)),"FORBIDDEN: task input URI prefix denied")
        val uri=DacpUri.parse(binding.uri.get);val parts=uri.getPath.stripPrefix("/").split("/",2)
        val client=cancel.add(NodeConnection.open(config,allocator,binding.uri.get))
        try {
          val info=client.getInfo(FlightDescriptor.path("datasets",parts(0),"/"+parts(1)),CallOptions.timeout(15,TimeUnit.SECONDS))
          val meta=dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata)
          require(meta.getKind=="data","input must be a data item")
          val view=ItemViews.select(binding.view,meta.getViewsList.asScala)
          val schema=if(view=="records" && !meta.getArrowSchema.isEmpty)Some(binding.scan.map(_.bind(info.getSchema).outputSchema).getOrElse(info.getSchema))else None
          if(Set("bytes","records")(view))InferenceResult(Map("result"->StreamDescriptor(Set(view),Set(meta.getFormat),schema,if(view=="bytes" || schema.nonEmpty)"known"else "unknown")))
          else {
            val codec=DataViewCodecs.named(config.plugins,view)
            val selection=codec.selection(binding.selection.getOrElse(Data.mapper.createObjectNode()))
            val ticket=dacp.protocol.Metadata.Ticket.newBuilder().setDataset(parts(0)).setItem("/"+parts(1)).setView(view)
              .setSelectionJson(com.google.protobuf.ByteString.copyFrom(Data.mapper.writeValueAsBytes(codec.selectionJson(selection)))).build()
            val typed=codec.remoteView(codec.kind){(_,out,c)=>
              val reader=client.getStream(new org.apache.arrow.flight.Ticket(ticket.toByteArray))
              try dacp.union.ScientificWire.consume(reader,view,out,c.check)finally {reader.cancel("metadata read complete",null);reader.close()}
            }.get.open(selection,StreamContext(allocator,()=>cancel.active()))
            InferenceResult(Map("result"->DataViewInference.inspect(codec)(typed,selection,meta.getFormat,None,()=>cancel.active())))
          }
        }finally client.close()
      case None=>OperatorInference.withDocker { docker=>
        val tmp=java.nio.file.Files.createTempDirectory("dacp-infer-source-")
        try {
          val op=t.source match {
            case None=>OperatorLibrary.resolve(config,t.operator,docker)
            case Some(code)=>
              val runtime=config.sourceRuntimes.getOrElse(code.runtime,throw new IllegalArgumentException("source runtime is not configured"))
              val file=tmp.resolve("operator.source");java.nio.file.Files.writeString(file,code.source)
              val contract=PortContract(Set("bytes","records"),Set.empty,None)
              ResolvedOperator(docker.inspectImageCmd(runtime.image).exec().getId,Some(file),Some("/operator/operator.source"),OperatorCommand(),
                inputs.keys.map(_->contract).toMap,Map.empty,runtime.executor,Some(OperatorProgram(code.runtime,code.entrypoint,Some(file))))
          }
          OperatorInference.run(config,op,InferenceRequest(inputs,Data.mapper.valueToTree(t.params)),cancel,t.source.nonEmpty)
        }finally Data.deleteTree(tmp)
      }
    }
  }
  def plan(config:Config,allocator:BufferAllocator,plan:JobPlan,cancel:StreamCancel):JsonNode={
    val local=DacpUri.origin(config.uri(""));val allowed=config.taskPeers.map(DacpUri.node).toSet+local
    val placement=mutable.Map.empty[String,String]
    propagate(Jobs.resolveReads(plan,config.uri(""))){(t,inputs)=>
      cancel.active()
      val target=t.node.map(DacpUri.node).getOrElse(t.inputs.toVector.sortBy(_._1).headOption.flatMap(_._2.task).flatMap(placement.get).getOrElse(local))
      require(allowed(target),"FORBIDDEN: inference target not in task_peers");placement(t.id)=target
      if(target==local)task(config,allocator,t,inputs,cancel)
      else {
        val client=cancel.add(NodeConnection.open(config,allocator,target))
        try {
          val results=client.doAction(new Action("dacp.infer",Data.mapper.writeValueAsBytes(taskJson(t,inputs))),CallOptions.timeout(math.min(120L,cancel.remainingSeconds),TimeUnit.SECONDS))
          require(results.hasNext,"missing remote inference result");val raw=results.next().getBody
          require(raw.length<=StructureInference.MaxBytes && !results.hasNext,"invalid remote inference response")
          StructureInference.result(Data.mapper.readTree(raw))
        }finally client.close()
      }
    }
  }
}
