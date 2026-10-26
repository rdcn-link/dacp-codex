package dacp

import com.github.dockerjava.core.{DefaultDockerClientConfig,DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient

case class PreparedOperatorGroup(task:Task,operator:ResolvedOperator,inputs:Map[String,InputBinding],outputs:Map[String,(String,String)],transportSources:Map[String,String]=Map.empty)
/** Placement and exact environment identity are core policy; fusion belongs to launch plugins. */
object OperatorGroups {
  def prepare(config:Config,plan:JobPlan):Either[String,PreparedOperatorGroup]={
    val readers=plan.tasks.filter(_.operator==Jobs.ReadItem).map(t=>t.id->t).toMap
    if(readers.nonEmpty){
      val consumed=plan.tasks.flatMap(_.inputs.values.flatMap(_.task)).toSet
      if(!readers.keySet.subsetOf(consumed))return Left("unconsumed_transport_source")
      if(plan.outputs.values.exists(p=>readers.contains(p._1)))return Left("selected_transport_source")
      val lowered=plan.copy(tasks=plan.tasks.filterNot(t=>readers.contains(t.id)).map { t=>t.copy(inputs=t.inputs.map{case(port,b)=>
        port->b.task.filter(readers.contains).map { id=>require(b.port=="result","read_item has only result output");Jobs.readBinding(readers(id)).get }.getOrElse(b)
      })})
      val external=lowered.tasks.flatMap(t=>t.inputs.toVector.sortBy(_._1).collect{case(p,b)if b.uri.nonEmpty=>(t.id,p)})
      val sources=external.zipWithIndex.flatMap {case((id,port),i)=>plan.tasks.find(_.id==id).get.inputs(port).task.filter(readers.contains).map(read=>s"i$i"->read)}.toMap
      return prepare(config,lowered).map(_.copy(transportSources=sources))
    }
    if(!config.operatorFusionEnabled)return Left("disabled_by_configuration")
    if(plan.outputs.isEmpty)return Left("no_selected_outputs")
    if(plan.tasks.size<2)return Left("single_step")
    if(plan.tasks.exists(_.source.nonEmpty))return Left("inline_source")
    val dc=DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val transport=new ApacheDockerHttpClient.Builder().dockerHost(dc.getDockerHost).sslConfig(dc.getSSLConfig).build()
    val docker=DockerClientImpl.getInstance(dc,transport)
    try {
      // Connected ports determine routing; actual stream headers supply output metadata.
      // Planning must never invoke operator code or open input streams.
      val contracts=plan.tasks.map { t=>
        val op=OperatorLibrary.resolve(config,t.operator,docker)
        val ports=plan.tasks.flatMap(_.inputs.values.collect{case b if b.task.contains(t.id)=>b.port}).toSet ++ plan.outputs.values.collect{case(id,p) if id==t.id=>p}
        val outputs=ports.map(_->PortContract(Set("bytes","records"),Set.empty,None)).toMap
        t.id->op.copy(outputs=outputs,program=op.program.map(_.copy(outputs=outputs.view.mapValues(_.representations).toMap)))
      }.toMap
      prepareResolved(config,plan,contracts)
    }finally{docker.close();transport.close()}
  }
  private[dacp] def prepareResolved(config:Config,plan:JobPlan,operators:Map[String,ResolvedOperator]):Either[String,PreparedOperatorGroup]={
      if(operators.values.exists(_.outputs.isEmpty))return Left("sink_boundary")
      val all=operators.values.toVector
      if(all.map(_.image).distinct.size!=1)return Left("different_runtime_images")
      if(all.map(_.executor).distinct.size!=1)return Left("different_executors")
      if(all.map(_.command.environment).distinct.size!=1)return Left("different_process_environments")
      val external=plan.tasks.flatMap(t=>t.inputs.toVector.sortBy(_._1).collect{case(port,b)if b.uri.nonEmpty=>(t.id,port,b)})
      val names=external.zipWithIndex.map{case((id,port,_),i)=>(id,port)->s"i$i"}.toMap
      val bindings=external.map{case(id,port,b)=>names((id,port))->b}.toMap
      val selected=plan.outputs
      val outputs=selected.toVector.sortBy(_._1).zipWithIndex.map{case((label,pair),i)=>s"o$i"->pair}.toMap
      require(outputs.nonEmpty,"job must expose an output")
      val labels=selected.map(_.swap)
      val artifactEntries=all.flatMap(op=>op.artifact.map(file=>file->op.artifactPath.get.stripPrefix("/operator/"))).groupBy(_._1).toVector
      val filenameCounts=artifactEntries.map(_._2.head._2).groupBy(identity).view.mapValues(_.size).toMap
      // Wheel filenames are package metadata: prepending a slot changes the distribution name.
      // Preserve original names when unique; only disambiguate colliding artifact filenames.
      val artifacts=artifactEntries.zipWithIndex.map{case((file,entries),i)=>
        val name=entries.head._2
        file->("/operator/"+(if(filenameCounts(name)==1)name else s"g$i-$name"))
      }.toMap
      def contract(c:PortContract)=OperatorGroupContract(c.representations,c.formats,c.schema.map(s=>java.util.Base64.getEncoder.encodeToString(s.serializeAsMessage())))
      val steps=plan.tasks.map { t=>
        val op=operators(t.id);require(t.inputs.keySet==op.inputs.keySet,"group input ports do not match declaration")
        val inputs=t.inputs.map{case(port,b)=>
          b.task.foreach { upstream=>require(operators(upstream).outputs.contains(b.port),"unknown upstream port") }
          port->OperatorGroupBinding(b.task,if(b.task.nonEmpty)b.port else names((t.id,port)))
        }
        OperatorGroupStep(t.id,op.program.get,op.artifact.map(artifacts),t.params,inputs,op.inputs.view.mapValues(contract).toMap,op.outputs.view.mapValues(contract).toMap)
      }
      config.plugins.byId(classOf[OperatorExecutor],all.head.executor).groupCommand(OperatorGroup(steps,outputs)) match {
        case None=>Left("executor_or_operator_not_fusible")
        case Some(cmd)=>
          val ins=external.map{case(id,port,_)=>names((id,port))->operators(id).inputs(port)}.toMap
          val outs=outputs.map{case(port,(task,name))=>port->operators(task).outputs(name)}
          val op=ResolvedOperator(all.head.image,None,None,cmd,ins,outs)
          Right(PreparedOperatorGroup(Task("group",bindings,"group@1",new java.util.LinkedHashMap[String,Object]()),op,bindings,outputs.map{case(port,pair)=>port->(pair._1,labels(pair))}))
      }
  }
}
