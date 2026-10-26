package dacp

import java.nio.file.{Files,Path}
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.{DefaultDockerClientConfig,DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient

/** Explicit inference reuses isolated workers; operator code never loads into the host JVM. */
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
    source:Boolean=false):InferenceResult=run(config,op,request,cancel,source)
  def run(config:Config,op:ResolvedOperator,request:InferenceRequest,cancel:StreamCancel,
    source:Boolean=false):InferenceResult={
    validateInputs(op,request.inputs)
    val program=op.program.getOrElse(throw new IllegalArgumentException("operator has no inference program"))
    val plugin=config.plugins.byId(classOf[OperatorExecutor],op.executor)
    val command=if(source)plugin.sourceInferenceWorkerCommand(program,op.artifactPath.get)else plugin.inferenceWorkerCommand(program,op.artifactPath)
    val body=StructureInference.mapper.writeValueAsBytes(StructureInference.json(request))
    require(body.length<=StructureInference.MaxBytes,"inference request exceeds 1 MiB")
    val key=workerKey(op,command,source)
    val response=config.inferenceWorkerPool.request(key,body,cancel)(()=>new DockerInferenceWorker(op,command))
    StructureInference.result(StructureInference.mapper.readTree(response))
  }
  private[dacp] def workerKey(op:ResolvedOperator,command:OperatorCommand,source:Boolean):String={
    val digest=java.security.MessageDigest.getInstance("SHA-256")
    def add(bytes:Array[Byte]):Unit={digest.update(java.nio.ByteBuffer.allocate(8).putLong(bytes.length.toLong).array());digest.update(bytes)}
    def text(s:String):Unit=add(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    def file(path:Path):Unit={val hash=java.security.MessageDigest.getInstance("SHA-256");val in=Files.newInputStream(path);try {val b=new Array[Byte](65536);var n=in.read(b);while(n>=0){if(n>0)hash.update(b,0,n);n=in.read(b)}}finally in.close();add(hash.digest())}
    text(op.image);text(op.executor);text(source.toString)
    val program=op.program.get;text(program.reference);text(program.entrypoint)
    if(dacp.union.Rpc.isAuthenticatedCall){val p=dacp.union.Rpc.principal;text("authenticated");text(p.kind);text(p.subject);text(p.unionId.getOrElse(""));text(p.certificate.getOrElse(""))}else text("embedded")
    text(op.artifactPath.getOrElse(""));text(op.artifact.isDefined.toString);op.artifact.foreach(file)
    text(command.executableScratch.toString)
    def strings(values:Seq[String]):Unit={text(values.size.toString);values.foreach(text)}
    text(command.entrypoint.isDefined.toString);strings(command.entrypoint.toVector.flatten);strings(command.arguments)
    text(command.environment.size.toString);command.environment.toVector.sortBy(_._1).foreach{case(k,v)=>text(k);text(v)}
    text(command.files.size.toString);command.files.toVector.sortBy(_._1).foreach{case(k,v)=>text(k);add(v)}
    text(command.mounts.size.toString);command.mounts.toVector.sortBy(_._1).foreach{case(k,v)=>text(k);file(v)}
    digest.digest().map(b=>f"${b&255}%02x").mkString
  }
}
