package dacp

import java.nio.file.{Files,Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.{Bind,Volume,HostConfig,AccessMode,Frame,StreamType,LogConfig}
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.core.{DefaultDockerClientConfig,DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient

/** Untrusted inference methods run in short-lived, networkless containers, never the host JVM. */
object OperatorInference {
  def withDocker[T](f:DockerClient=>T):T={
    val dc=DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val transport=new ApacheDockerHttpClient.Builder().dockerHost(dc.getDockerHost).sslConfig(dc.getSSLConfig).build()
    val docker=DockerClientImpl.getInstance(dc,transport)
    try f(docker)finally{docker.close();transport.close()}
  }
  def contract(d:StreamDescriptor):PortContract=PortContract(
    if(d.representations.isEmpty)Set("bytes","records")else d.representations,d.formats,d.schema)
  def descriptor(c:PortContract):StreamDescriptor=StreamDescriptor(c.representations,c.formats,c.schema,
    if(c.representations==Set("bytes") || (c.representations==Set("records") && c.schema.nonEmpty))"known"else "unknown")
  def validateInputs(op:ResolvedOperator,inputs:Map[String,StreamDescriptor]):Unit={
    require(op.inputs.keySet==inputs.keySet,"input ports do not match operator declaration")
    inputs.foreach{case(p,d)=>
      val expected=op.inputs(p)
      require(d.representations.isEmpty || d.representations.intersect(expected.representations).nonEmpty,s"incompatible representation on input $p")
      require(d.formats.isEmpty || expected.formats.isEmpty || d.formats.intersect(expected.formats).nonEmpty,s"incompatible format on input $p")
      for(a<-d.schema;b<-expected.schema)require(a==b,s"incompatible schema on input $p")
    }
  }
  def apply(config:Config,op:ResolvedOperator,request:InferenceRequest,cancel:StreamCancel,
    source:Boolean=false):InferenceResult=withDocker(d=>run(config,d,op,request,cancel,source))
  def run(config:Config,docker:DockerClient,op:ResolvedOperator,request:InferenceRequest,cancel:StreamCancel,
    source:Boolean=false):InferenceResult={
    validateInputs(op,request.inputs)
    val program=op.program.getOrElse(throw new IllegalArgumentException("operator has no inference program"))
    val plugin=config.plugins.byId(classOf[OperatorExecutor],op.executor)
    val command=if(source)plugin.sourceInferenceCommand(program,op.artifactPath.get)else plugin.inferenceCommand(program,op.artifactPath)
    val tmp=Files.createTempDirectory("dacp-infer-");var id:String=null
    // Container UIDs may differ from the host service account. Only metadata is
    // mounted here; credentials and actual input data are never copied into it.
    Files.setPosixFilePermissions(tmp,java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"))
    val stdout=new java.io.ByteArrayOutputStream();val stderr=new java.io.ByteArrayOutputStream()
    val failure=new AtomicReference[Throwable]()
    try {
      val body=StructureInference.mapper.writeValueAsBytes(StructureInference.json(request))
      require(body.length<=StructureInference.MaxBytes,"inference request exceeds 1 MiB")
      Files.write(tmp.resolve("inference.json"),body)
      command.files.foreach{case(name,bytes)=>require(name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*") && name!="inference.json","invalid inference resource");Files.write(tmp.resolve(name),bytes)}
      val resources=Files.list(tmp)
      try resources.iterator.asScala.foreach(p=>Files.setPosixFilePermissions(p,java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--")))finally resources.close()
      command.mounts.keys.foreach(p=>require(p.matches("/operator/[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"invalid inference mount"))
      // Cache files and user artifacts may be 0600/0400. Dropped container
      // capabilities must not require changing permissions on the originals.
      val artifacts=Files.createDirectory(tmp.resolve("artifacts"))
      Files.setPosixFilePermissions(artifacts,java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"))
      val mounts=op.artifact.zip(op.artifactPath).toVector++command.mounts.toVector.map{case(path,file)=>file->path}
      val binds=Vector(new Bind(tmp.toAbsolutePath.toString,new Volume("/input"),AccessMode.ro))++mounts.zipWithIndex.map{case((file,path),index)=>
        val staged=artifacts.resolve(index.toString)
        Files.copy(file,staged)
        Files.setPosixFilePermissions(staged,java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"))
        new Bind(staged.toAbsolutePath.toString,new Volume(path),AccessMode.ro)
      }
      val host=HostConfig.newHostConfig().withBinds(binds.asJava).withNetworkMode("none").withReadonlyRootfs(true)
        .withMemory(512L*1024*1024).withPidsLimit(64L).withNanoCPUs(1000000000L)
        .withTmpFs(Map("/tmp"->"rw,noexec,nosuid,nodev,size=128m").asJava).withCapDrop(com.github.dockerjava.api.model.Capability.ALL)
        .withSecurityOpts(Vector("no-new-privileges:true").asJava).withLogConfig(new LogConfig(LogConfig.LoggingType.NONE))
      val create=docker.createContainerCmd(op.image).withHostConfig(host).withCmd(command.arguments.asJava)
        .withEnv((command.environment+("DACP_OPERATOR"->program.reference)).map{case(k,v)=>s"$k=$v"}.toVector.asJava)
        .withLabels(Map("dacp.purpose"->"structure_inference").asJava)
      command.entrypoint.foreach(v=>create.withEntrypoint(v.asJava))
      id=create.exec().getId;val container=id
      val cleanup=new AutoCloseable{def close():Unit=try docker.removeContainerCmd(container).withForce(true).exec()catch{case _:Throwable=>()}}
      cancel.add(cleanup)
      val callback=new ResultCallback.Adapter[Frame]{
        override def onNext(frame:Frame):Unit=try {
          val out=if(frame.getStreamType==StreamType.STDOUT)stdout else stderr
          out.synchronized {require(out.size.toLong+frame.getPayload.length<=StructureInference.MaxBytes,"inference output exceeds 1 MiB");out.write(frame.getPayload)}
        }catch{case t:Throwable=>failure.compareAndSet(null,t)}
        override def onError(t:Throwable):Unit={failure.compareAndSet(null,t);super.onError(t)}
      }
      try {
        docker.attachContainerCmd(id).withStdOut(true).withStdErr(true).withFollowStream(true).exec(callback).awaitStarted()
        docker.startContainerCmd(id).exec()
        val deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15)
        while(docker.inspectContainerCmd(id).exec().getState.getRunning){
          cancel.active();Option(failure.get()).foreach(throw _)
          require(System.nanoTime()<deadline,"structure inference timed out after 15 seconds");Thread.sleep(25)
        }
        require(callback.awaitCompletion(2,TimeUnit.SECONDS),"inference output did not complete")
        Option(failure.get()).foreach(throw _)
        require(docker.inspectContainerCmd(id).exec().getState.getExitCodeLong==0L,
          "operator must implement successful structure inference: "+stderr.toString("UTF-8").take(4096))
        StructureInference.result(StructureInference.mapper.readTree(stdout.toByteArray))
      }finally callback.close()
    }finally{if(id!=null)try docker.removeContainerCmd(id).withForce(true).exec()catch{case _:Throwable=>()};Data.deleteTree(tmp)}
  }
}
