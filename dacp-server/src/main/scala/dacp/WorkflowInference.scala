package dacp

import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.{Action,FlightDescriptor,CallOptions}
import java.util.concurrent.TimeUnit

/** Pure DAG propagation; Union supplies its own member-specific inference callback. */
object WorkflowInference {
  def propagate(plan:JobPlan)(infer:(Task,Map[String,StreamDescriptor])=>InferenceResult):JsonNode={
    val inferred=mutable.Map.empty[String,InferenceResult]
    val result=Data.mapper.createObjectNode();val steps=result.putObject("tasks")
    plan.tasks.foreach { task=>
      val inputs=task.inputs.map{case(port,b)=>
        require(b.task.nonEmpty,"workflow inputs must reference step outputs")
        val upstream=inferred.getOrElse(b.task.get,throw new IllegalArgumentException("upstream structure not available"))
        port->upstream.outputs.getOrElse(b.port,throw new IllegalArgumentException(s"unknown output ${b.task.get}:${b.port}"))
      }
      val output=infer(task,inputs);output.requireValid();inferred(task.id)=output
      steps.set[JsonNode](task.id,StructureInference.json(output))
    }
    val selected=plan.outputs.map{case(label,(task,port))=>label->inferred(task).outputs.getOrElse(port,throw new IllegalArgumentException(s"unknown selected output $task:$port"))}
    result.set[JsonNode]("outputs",StructureInference.portsJson(selected));result
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
    Jobs.readBinding(t) match {
      case Some(binding)=>
        require(inputs.isEmpty,"read_item has no inputs")
        require(config.taskInputPrefixes.isEmpty || config.taskInputPrefixes.exists(DacpUri.within(binding.uri.get,_)),"FORBIDDEN: task input URI prefix denied")
        val uri=DacpUri.parse(binding.uri.get);val parts=uri.getPath.stripPrefix("/").split("/",2)
        val client=cancel.add(NodeConnection.open(config,allocator,binding.uri.get))
        try {
          val info=client.getInfo(FlightDescriptor.path("datasets",parts(0),"/"+parts(1)),CallOptions.timeout(15,TimeUnit.SECONDS))
          val meta=dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata)
          require(meta.getKind=="data" && meta.getViewsList.contains(binding.view),"Item does not support requested view")
          val schema=if(binding.view=="records" && !meta.getArrowSchema.isEmpty)Some(info.getSchema)else None
          InferenceResult(Map("result"->StreamDescriptor(Set(binding.view),Set(meta.getFormat),schema,if(binding.view=="bytes" || schema.nonEmpty)"known"else "unknown")))
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
          OperatorInference.run(config,docker,op,InferenceRequest(inputs,Data.mapper.valueToTree(t.params)),cancel,t.source.nonEmpty)
        }finally Data.deleteTree(tmp)
      }
    }
  }
  def plan(config:Config,allocator:BufferAllocator,plan:JobPlan,cancel:StreamCancel):JsonNode={
    val local=DacpUri.origin(config.uri(""));val allowed=config.taskPeers.map(DacpUri.node).toSet+local
    val placement=mutable.Map.empty[String,String]
    propagate(plan){(t,inputs)=>
      cancel.active()
      val target=t.node.map(DacpUri.node).getOrElse(Jobs.readBinding(t).flatMap(_.uri).map(DacpUri.origin)
        .orElse(t.inputs.toVector.sortBy(_._1).headOption.flatMap(_._2.task).flatMap(placement.get)).getOrElse(local))
      require(allowed(target),"FORBIDDEN: inference target not in task_peers");placement(t.id)=target
      if(target==local)task(config,allocator,t,inputs,cancel)
      else {
        val client=cancel.add(NodeConnection.open(config,allocator,target))
        try {
          val results=client.doAction(new Action("dacp.infer",Data.mapper.writeValueAsBytes(taskJson(t,inputs))),CallOptions.timeout(20,TimeUnit.SECONDS))
          require(results.hasNext,"missing remote inference result");val raw=results.next().getBody
          require(raw.length<=StructureInference.MaxBytes && !results.hasNext,"invalid remote inference response")
          StructureInference.result(Data.mapper.readTree(raw))
        }finally client.close()
      }
    }
  }
}
