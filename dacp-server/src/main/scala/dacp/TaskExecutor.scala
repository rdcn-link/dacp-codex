package dacp

import java.nio.file.{Files,Path,StandardOpenOption}
import java.nio.channels.{FileChannel,Channels}
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector,VectorLoader,VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ArrowStreamWriter}
import com.github.dockerjava.core.{DefaultDockerClientConfig,DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import com.github.dockerjava.api.model.{Bind,Volume,HostConfig,AccessMode,Frame,StreamType,LogConfig}
import com.github.dockerjava.api.async.ResultCallback

/** Receives already-authorized named item streams; never creates/publishes an output dataset. */
class TaskExecutor(config: Config,allocator: BufferAllocator) {
  def execute(task: Task,inputs: Map[String,FrameSource],emit: OperatorFrame=>Unit,cancel: StreamCancel, resolved:Option[ResolvedOperator]=None,onEvent:Option[com.fasterxml.jackson.databind.JsonNode=>Unit]=None): Unit = {
    Jobs.authorizeSource(config,task)
    Jobs.readBinding(task).foreach { binding=>
      require(inputs.isEmpty,"read_item has no input ports")
      val stream=cancel.add(ItemSource.open(config,allocator,binding,cancel))
      try {var f=stream.next();while(f.nonEmpty){cancel.active();emit(StreamProtocol.rename(f.get,"result"));f=stream.next()}}
      finally stream.close()
      return
    }
    val dc=DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val transport=new ApacheDockerHttpClient.Builder().dockerHost(dc.getDockerHost).sslConfig(dc.getSSLConfig).build()
    val docker=DockerClientImpl.getInstance(dc,transport)
    val temporary=Files.createTempDirectory("dacp-stream-input-")
    var container: String=null
    val handles=scala.collection.mutable.ArrayBuffer.empty[FileChannel]
    val pipe=new java.io.PipedInputStream(65536);val stdout=new java.io.PipedOutputStream(pipe)
    var callback: ResultCallback.Adapter[Frame]=null
    val stderr=new StringBuilder
    try {
      val op=resolved.getOrElse(task.source match {
        case None=>OperatorLibrary.resolve(config,task.operator,docker)
        case Some(code)=>
          val runtime=config.sourceRuntimes.getOrElse(code.runtime,throw new IllegalArgumentException("source runtime is not configured"))
          val artifact=temporary.resolve("operator.source")
          Files.writeString(artifact,code.source)
          val command=config.plugins.byId(classOf[OperatorExecutor],runtime.executor).sourceCommand(OperatorProgram(code.runtime,code.entrypoint,Some(artifact)),"/operator/operator.source")
          val contract=PortContract(Set("bytes","records"),Set.empty,None)
          ResolvedOperator(docker.inspectImageCmd(runtime.image).exec().getId,Some(artifact),Some("/operator/operator.source"),command,inputs.keys.map(_->contract).toMap,Map("result"->contract))
      })
      require(inputs.keySet==op.inputs.keySet,"input ports do not match operator declaration")
      val starts=inputs.toVector.sortBy(_._1).map {case(port,source)=>
        val f=source.next().getOrElse(throw new IllegalArgumentException("missing input metadata"));val h=StreamProtocol.header(f)
        require(h.path("type").asText=="start","input must start with metadata");op.inputs(port).accept(h)
        (port,source,h)
      }
      val manifest=Data.mapper.createArrayNode()
      starts.zipWithIndex.foreach {case((port,_,h),i)=>
        val filename=s"$i.arrow";val path=temporary.resolve(filename)
        require(new ProcessBuilder("mkfifo",path.toString).start().waitFor()==0,"mkfifo failed")
        val channel=FileChannel.open(path,StandardOpenOption.READ,StandardOpenOption.WRITE);handles+=channel;cancel.add(channel)
        val e=manifest.addObject();e.put("port",port).put("file",filename).put("format",h.path("format").asText).put("representation",h.path("representation").asText)
        if(h.has("schema"))e.put("schema",h.path("schema").asText)
      }
      Files.write(temporary.resolve("manifest.json"),Data.mapper.writeValueAsBytes(manifest))
      Files.write(temporary.resolve("params.json"),Data.mapper.writeValueAsBytes(task.params))
      op.command.files.foreach {case(name,bytes)=>
        require(name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"invalid runtime resource name")
        require(!Files.exists(temporary.resolve(name)),"runtime resource shadows input")
        Files.write(temporary.resolve(name),bytes)
      }
      op.command.mounts.keys.foreach(p=>require(p.matches("/operator/[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"invalid runtime mount"))
      val binds=Seq(new Bind(temporary.toAbsolutePath.toString,new Volume("/input"),AccessMode.ro)) ++
        op.artifact.zip(op.artifactPath).map{case(file,path)=>new Bind(file.toAbsolutePath.toString,new Volume(path),AccessMode.ro)}
      val extraBinds=op.command.mounts.toVector.map{case(path,file)=>new Bind(file.toAbsolutePath.toString,new Volume(path),AccessMode.ro)}
      val scratch=Map("/tmp"->"rw,noexec,nosuid,nodev,size=512m") ++
        (if(op.command.executableScratch) Map("/operator-work"->"rw,exec,nosuid,nodev,size=512m") else Map.empty[String,String])
      val host=HostConfig.newHostConfig().withBinds((binds++extraBinds).asJava).withNetworkMode("none").withMemory(1024L*1024*1024).withPidsLimit(128L)
        .withReadonlyRootfs(true).withTmpFs(scratch.asJava)
        .withLogConfig(new LogConfig(LogConfig.LoggingType.NONE))
      val create=docker.createContainerCmd(op.image).withHostConfig(host)
        .withLabels(Map("dacp.execution_mode"->(if(onEvent.nonEmpty)"shared_process"else "isolated_step"),"dacp.task"->task.id).asJava)
        .withEnv((op.command.environment+("DACP_OPERATOR"->task.operator)).map{case(k,v)=>s"$k=$v"}.toSeq.asJava)
        .withCmd(op.command.arguments.asJava)
      op.command.entrypoint.foreach(e=>create.withEntrypoint(e.asJava))
      container=create.exec().getId
      val id=container
      cancel.add(new AutoCloseable{def close():Unit=try docker.removeContainerCmd(id).withForce(true).exec() catch{case _:Throwable=>()}})
      cancel.add(pipe);cancel.add(stdout)
      callback=new ResultCallback.Adapter[Frame] {
        override def onNext(f: Frame): Unit = if(f.getStreamType==StreamType.STDOUT) stdout.write(f.getPayload) else stderr.synchronized {
          if(stderr.length<65536) stderr.append(new String(f.getPayload,java.nio.charset.StandardCharsets.UTF_8))
        }
        override def onComplete(): Unit = {stdout.close();super.onComplete()}
        override def onError(t:Throwable): Unit={cancel.fail(t);try stdout.close() finally super.onError(t)}
      }
      docker.attachContainerCmd(container).withStdOut(true).withStdErr(true).withFollowStream(true).exec(callback).awaitStarted()
      docker.startContainerCmd(container).exec()
      val writers=starts.zipWithIndex.map{case((port,source,h),i)=>StreamProtocol.thread(cancel){
        val repr=h.path("representation").asText
        val schema=if(repr=="records") StreamProtocol.schema(h) else PluginSchemas.bytes
        val root=VectorSchemaRoot.create(schema,allocator)
        val output=new java.io.FilterOutputStream(Channels.newOutputStream(handles(i))){
          override def write(bytes:Array[Byte],offset:Int,length:Int):Unit=out.write(bytes,offset,length)
          override def close():Unit=flush()
        }
        val writer=new ArrowStreamWriter(root,null,output)
        try {
          writer.start();var ended=false
          while(!ended) {
            cancel.active();val f=source.next().getOrElse(throw new IllegalArgumentException("truncated input"));val meta=StreamProtocol.header(f)
            meta.path("type").asText match {
              case "data" if repr=="bytes"=> root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,f.data);root.setRowCount(1);writer.writeBatch()
              case "data"=>
                val reader=new ArrowStreamReader(new java.io.ByteArrayInputStream(f.data),allocator)
                try {require(reader.getVectorSchemaRoot.getSchema==schema,"input schema changed");require(reader.loadNextBatch(),"empty record frame")
                  val batch=new VectorUnloader(reader.getVectorSchemaRoot).getRecordBatch
                  try new VectorLoader(root).load(batch) finally batch.close()
                  writer.writeBatch();require(!reader.loadNextBatch(),"one record batch per frame required")
                } finally reader.close()
              case "end"=>ended=true
              case _=>throw new IllegalArgumentException("unexpected input event")
            }
          }
          writer.end()
        } finally {writer.close();root.close()}
      }}
      var begun=Set.empty[String];var ended=Set.empty[String]
      var next=OperatorFrames.read(pipe)
      while(next.nonEmpty) {
        cancel.active();val f=next.get;val h=StreamProtocol.header(f);val port=h.path("port").asText
        if(Set("task_state","task_metric")(h.path("type").asText) && onEvent.nonEmpty)onEvent.get(h)
        else {
        require(op.outputs.contains(port),"undeclared output port")
        h.path("type").asText match {
          case "start"=>require(!begun(port),"duplicate output start");op.outputs(port).accept(h);begun+=port
          case "data"=>require(begun==op.outputs.keySet && !ended(port),"all output metadata must precede data")
          case "end"=>require(begun(port) && !ended(port),"invalid output end");ended+=port
          case _=>throw new IllegalArgumentException("invalid operator output event")
        }
        emit(f)
        }
        next=OperatorFrames.read(pipe)
      }
      while(docker.inspectContainerCmd(container).exec().getState.getRunning){cancel.active();Thread.sleep(25)}
      if(docker.inspectContainerCmd(container).exec().getState.getExitCodeLong!=0L)throw new RuntimeException("operator failed: "+stderr.synchronized(stderr.toString))
      require(ended==op.outputs.keySet,"operator output stream incomplete")
      writers.foreach(t=>{while(t.isAlive){cancel.active();t.join(100)}});cancel.active()
    } catch {
      case t:Throwable => cancel.fail(t); throw t
    } finally {
      if(container!=null)try docker.removeContainerCmd(container).withForce(true).exec() catch{case _:Throwable=>()}
      if(callback!=null)try callback.close() catch{case _:Throwable=>()}
      try pipe.close() finally stdout.close()
      handles.foreach(c=>try c.close() catch{case _:Throwable=>()})
      docker.close();transport.close();Data.deleteTree(temporary)
    }
  }
}
