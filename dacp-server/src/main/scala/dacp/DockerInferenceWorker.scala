package dacp

import java.nio.file.{Files,Path,StandardOpenOption}
import java.nio.channels.{Channels,FileChannel}
import java.io.{DataInputStream,DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._
import com.github.dockerjava.core.{DefaultDockerClientConfig,DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import com.github.dockerjava.api.model.{Bind,Volume,HostConfig,AccessMode,Frame,LogConfig,Capability}
import com.github.dockerjava.api.async.ResultCallback

/** FIFOs carry only bounded metadata. No dataset, credential, or result storage is mounted. */
private[dacp] final class DockerInferenceWorker(op:ResolvedOperator,command:OperatorCommand) extends InferenceWorker {
  private val stopped=new AtomicBoolean()
  private val lock=new Object
  private val dc=DefaultDockerClientConfig.createDefaultConfigBuilder().build()
  private val transport=new ApacheDockerHttpClient.Builder().dockerHost(dc.getDockerHost).sslConfig(dc.getSSLConfig)
    .connectionTimeout(java.time.Duration.ofSeconds(5)).responseTimeout(java.time.Duration.ofSeconds(5)).build()
  private val docker=DockerClientImpl.getInstance(dc,transport)
  private var directory:Path=null
  private var container:String=null
  private var callback:ResultCallback.Adapter[Frame]=null
  private val channels=scala.collection.mutable.ArrayBuffer.empty[FileChannel]
  private var input:DataInputStream=null
  private var output:DataOutputStream=null
  private val diagnostics=new StringBuilder
  private var ready=false
  private def active():Unit=require(!stopped.get(),"inference worker is closed")
  private def closePipes():Unit=lock.synchronized{channels.foreach(c=>try c.close()catch{case _:Throwable=>()})}
  private def channel(path:Path,options:StandardOpenOption*):FileChannel={val c=FileChannel.open(path,options:_*);channels+=c;c}
  private def start():Unit=lock.synchronized {
    active()
    directory=Files.createTempDirectory("dacp-inference-worker-")
    def permissions(p:Path,mode:String):Unit=Files.setPosixFilePermissions(p,java.nio.file.attribute.PosixFilePermissions.fromString(mode))
    permissions(directory,"rwxr-xr-x")
    Seq("requests.fifo","responses.fifo").foreach { name=>
      val path=directory.resolve(name)
      val process=new ProcessBuilder("mkfifo",path.toString).start()
      try require(process.waitFor()==0,"cannot create inference pipe")finally process.destroy()
      permissions(path,"rw-rw-rw-")
    }
    output=new DataOutputStream(Channels.newOutputStream(channel(directory.resolve("requests.fifo"),StandardOpenOption.READ,StandardOpenOption.WRITE)))
    input=new DataInputStream(Channels.newInputStream(channel(directory.resolve("responses.fifo"),StandardOpenOption.READ,StandardOpenOption.WRITE)))
    command.files.foreach {case(name,bytes)=>
      require(name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*") && !Set("requests.fifo","responses.fifo","artifacts")(name),"invalid inference worker resource")
      val file=directory.resolve(name);Files.write(file,bytes);permissions(file,"rw-r--r--")
    }
    command.mounts.keys.foreach(p=>require(p.matches("/operator/[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"invalid inference mount"))
    val artifacts=Files.createDirectory(directory.resolve("artifacts"));permissions(artifacts,"rwxr-xr-x")
    val mounts=op.artifact.zip(op.artifactPath).toVector++command.mounts.toVector.map{case(path,file)=>file->path}
    val binds=Vector(new Bind(directory.toAbsolutePath.toString,new Volume("/input"),AccessMode.ro))++mounts.zipWithIndex.map {case((file,path),i)=>
      val staged=artifacts.resolve(i.toString);Files.copy(file,staged);permissions(staged,"rw-r--r--")
      new Bind(staged.toAbsolutePath.toString,new Volume(path),AccessMode.ro)
    }
    val host=HostConfig.newHostConfig().withBinds(binds.asJava).withNetworkMode("none").withReadonlyRootfs(true).withAutoRemove(true)
      .withMemory(512L*1024*1024).withPidsLimit(64L).withNanoCPUs(1000000000L)
      .withTmpFs(Map("/tmp"->"rw,noexec,nosuid,nodev,size=128m").asJava).withCapDrop(Capability.ALL)
      .withSecurityOpts(Vector("no-new-privileges:true").asJava).withLogConfig(new LogConfig(LogConfig.LoggingType.NONE))
    val create=docker.createContainerCmd(op.image).withHostConfig(host).withCmd(command.arguments.asJava)
      .withEnv((command.environment+("DACP_OPERATOR"->op.program.get.reference)).map{case(k,v)=>s"$k=$v"}.toVector.asJava)
      .withLabels(Map("dacp.purpose"->"structure_inference","dacp.inference_mode"->"worker").asJava)
    command.entrypoint.foreach(v=>create.withEntrypoint(v.asJava))
    container=create.exec().getId
    callback=new ResultCallback.Adapter[Frame] {
      override def onNext(f:Frame):Unit=diagnostics.synchronized {
        if(diagnostics.length<65536)diagnostics.append(new String(f.getPayload,java.nio.charset.StandardCharsets.UTF_8).take(65536-diagnostics.length))
      }
      override def onComplete():Unit={closePipes();super.onComplete()}
      override def onError(t:Throwable):Unit={closePipes();super.onError(t)}
    }
    docker.attachContainerCmd(container).withStdOut(true).withStdErr(true).withFollowStream(true).exec(callback).awaitStarted()
    docker.startContainerCmd(container).exec()
  }
  def request(bytes:Array[Byte]):Array[Byte]={
    if(!ready){
      start()
      require(input.readInt()==0,"invalid inference worker readiness")
      lock.synchronized {
        active()
        // Drop the bootstrap read/write holders. A crashed worker now produces EOF/EPIPE.
        val in=new DataInputStream(Channels.newInputStream(channel(directory.resolve("responses.fifo"),StandardOpenOption.READ)))
        val out=new DataOutputStream(Channels.newOutputStream(channel(directory.resolve("requests.fifo"),StandardOpenOption.WRITE)))
        input.close();output.close();input=in;output=out;ready=true
      }
    }
    active()
    try {
      require(bytes.nonEmpty && bytes.length<=StructureInference.MaxBytes,"inference request exceeds limits")
      output.writeInt(bytes.length);output.write(bytes);output.flush()
      val size=input.readInt();require(size>0 && size<=StructureInference.MaxBytes,"inference response exceeds limits")
      val response=new Array[Byte](size);input.readFully(response)
      StructureInference.result(StructureInference.mapper.readTree(response))
      response
    }catch{case scala.util.control.NonFatal(e)=>throw new IllegalArgumentException("inference worker failed: "+diagnostics.synchronized(diagnostics.toString.take(4096)),e)}
  }
  def close():Unit=if(stopped.compareAndSet(false,true)) {
    lock.synchronized {
      closePipes()
      if(container!=null)try docker.removeContainerCmd(container).withForce(true).exec()catch{case _:Throwable=>()}
      if(callback!=null)try callback.close()catch{case _:Throwable=>()}
      try docker.close()finally transport.close()
      if(directory!=null)Data.deleteTree(directory)
    }
  }
}
