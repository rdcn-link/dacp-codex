package dacp

import java.net.URI
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, SynchronousQueue, ThreadPoolExecutor, TimeUnit, RejectedExecutionException}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import dacp.protocol.Metadata

object DacpUri {
  def parse(value: String, datasetRequired: Boolean = true): URI = {
    val u = new URI(value)
    require(u.getScheme == "dacp" && u.getHost != null && u.getUserInfo == null && u.getQuery == null && u.getFragment == null,
      "expected dacp://host[:port]/dataset[/item]")
    require(u.getPort == -1 || (u.getPort > 0 && u.getPort <= 65535),"invalid node port")
    val path = Option(u.getPath).getOrElse("")
    require(!path.contains("\\") && !path.contains("\u0000"),"invalid URI path")
    val segments = path.stripPrefix("/").stripSuffix("/").split("/",-1)
    require(!segments.exists(s => s == "." || s == ".."),"invalid URI path")
    if (datasetRequired) require(segments.head.nonEmpty && !segments.contains(""),"missing dataset or empty path segment")
    u
  }
  def origin(value: String): String = {
    val u = parse(value,false)
    val host = u.getHost.toLowerCase(java.util.Locale.ROOT)
    val authority = if (host.contains(":") && !host.startsWith("[")) s"[$host]" else host
    s"dacp://$authority:${if (u.getPort < 0) 8815 else u.getPort}"
  }
  def node(value: String): String = {
    val u = parse(value,false)
    require(u.getPath == "" || u.getPath == "/","node must be a dacp URI without dataset path")
    origin(value)
  }
  def canonical(value: String): String = {
    val u = parse(value)
    origin(value) + u.getRawPath.stripSuffix("/")
  }
  def within(value: String, prefix: String): Boolean = {
    val v = canonical(value)
    val root = parse(prefix,false)
    if (root.getPath == "" || root.getPath == "/") origin(value) == origin(prefix)
    else { val p = canonical(prefix); v == p || v.startsWith(p + "/") }
  }
}

object NodeConnection {
  def open(config: Config, allocator: BufferAllocator, value: String): FlightClient = {
    val u = DacpUri.parse(value,false)
    val ca = Files.newInputStream(config.ca); val cert = Files.newInputStream(config.cert); val key = Files.newInputStream(config.key)
    try FlightClient.builder(allocator,Location.forGrpcTls(u.getHost,if (u.getPort < 0) 8815 else u.getPort))
      .trustedCertificates(ca).clientCertificate(cert,key).build()
    finally {ca.close();cert.close();key.close()}
  }
}

object TaskWire {
  def validate(task: Task): Unit = {
    require(task.id.matches("[a-zA-Z0-9_-]{1,128}"),"invalid task id")
    require(task.output.matches("[a-zA-Z0-9_-]{1,128}"),"invalid output name")
    require(task.operator.matches("[a-zA-Z0-9_-][a-zA-Z0-9_.-]*@[a-zA-Z0-9_-][a-zA-Z0-9_.-]*"),"operator must be name@version")
    require(task.inputs.nonEmpty,"task inputs required")
    require(task.params != null,"params must be a mapping")
  }
  def encode(task: Task): Metadata.ExecuteTaskRequest = {
    validate(task)
    Metadata.ExecuteTaskRequest.newBuilder().setId(task.id).addAllInputs(task.inputs.asJava)
      .setOperator(task.operator).setOutput(task.output)
      .setParamsJson(com.google.protobuf.ByteString.copyFrom(Data.mapper.writeValueAsBytes(task.params))).build()
  }
  def decode(request: Metadata.ExecuteTaskRequest): Task = {
    val bytes = request.getParamsJson.toByteArray
    val json = if (bytes.isEmpty) Data.mapper.createObjectNode() else Data.mapper.readTree(bytes)
    require(json.isObject,"params must be a JSON mapping")
    val params = Data.mapper.convertValue(json,classOf[java.util.LinkedHashMap[String,Object]])
    val task = Task(request.getId,request.getInputsList.asScala.toSeq,request.getOperator,params,request.getOutput)
    validate(task); task.inputs.foreach(DacpUri.parse(_));task
  }
}

case class Placement(task: Task, node: String, outputUri: String)
class JobCoordinator(config: Config, dispatch: (String,Task) => String) {
  private val local = DacpUri.origin(config.uri(""))
  private val allowed = config.taskPeers.map(DacpUri.node).toSet + local
  // Architecture: URI placement needs no central registry. A worker executes a single resolved task;
  // it never forwards it. Only the job coordinator walks the DAG, preventing forwarding loops.
  def plan(yaml: String): Seq[Placement] = {
    var outputs = Map.empty[String,String]
    Jobs.parse(yaml).map { t =>
      TaskWire.validate(t)
      val inputs = t.inputs.map { value =>
        if (value.startsWith("dacp://")) DacpUri.canonical(value)
        else {
          val p = value.split("/",2)
          DacpUri.canonical(outputs.getOrElse(p(0),throw new IllegalArgumentException("unknown upstream output")) + p.lift(1).map("/" + _).getOrElse(""))
        }
      }
      // Multi-input placement defaults to the first input; an explicit node chooses the data anchor.
      val target = t.node.map(DacpUri.node).getOrElse(DacpUri.origin(inputs.head))
      require(allowed(target),s"FORBIDDEN: task target $target is not in task_peers")
      val result = s"$target/${t.output}"
      outputs += t.output -> result
      Placement(t.copy(inputs=inputs,node=None),target,result)
    }
  }
  def run(yaml: String): Map[String,String] = {
    val placements = plan(yaml) // Validate the whole graph and all destinations before starting work.
    var results = Map.empty[String,String]
    placements.foreach { p =>
      val actual = try dispatch(p.node,p.task)
        catch { case NonFatal(e) => throw new IllegalStateException(s"task ${p.task.id} on ${p.node} failed: ${e.getMessage}",e) }
      require(DacpUri.canonical(actual) == p.outputUri,"worker returned an unexpected output URI")
      results += p.task.output -> actual
    }
    results
  }
}

class TaskService(config: Config, allocator: BufferAllocator, execute: Task => String) extends AutoCloseable {
  private val local = DacpUri.origin(config.uri(""))
  private val active = ConcurrentHashMap.newKeySet[String]()
  private val permits = new java.util.concurrent.Semaphore(config.maxConcurrentTasks)
  private val pool = new ThreadPoolExecutor(config.maxConcurrentTasks,config.maxConcurrentTasks,0L,TimeUnit.MILLISECONDS,
    new SynchronousQueue[Runnable](), (r: Runnable) => new Thread(r,"dacp-control-task"))
  private def executeHere(task: Task): String = {
    TaskWire.validate(task)
    task.inputs.foreach { input =>
      DacpUri.parse(input)
      val prefixes = if (config.taskInputPrefixes.nonEmpty) config.taskInputPrefixes else config.prefixes
      require(prefixes.exists(p => DacpUri.within(input,p)),s"FORBIDDEN: task input $input is not in task_input_prefixes")
    }
    require(task.node.isEmpty,"worker accepts resolved tasks only")
    require(!config.hasDataset(task.output),"output shadows configured dataset")
    config.authorize(task.output,"")
    if (!permits.tryAcquire()) throw CallStatus.UNAVAILABLE.withDescription("node task capacity reached").toRuntimeException
    try {
      require(active.add(task.output),s"output ${task.output} is already executing")
      try execute(task) finally active.remove(task.output)
    } finally permits.release()
  }
  private def dispatch(node: String, task: Task): String = {
    if (node == local) executeHere(task)
    else {
      val client = NodeConnection.open(config,allocator,node)
      try {
        // No automatic retry: a lost response may leave a completed output on the worker until TTL.
        val results = client.doAction(new Action(TaskService.ExecuteAction,TaskWire.encode(task).toByteArray),
          CallOptions.timeout(35,TimeUnit.MINUTES))
        require(results.hasNext,"worker returned no task result")
        val uri = Metadata.ExecuteTaskResult.parseFrom(results.next().getBody).getOutputUri
        require(!results.hasNext,"worker returned multiple task results")
        uri
      } finally client.close()
    }
  }
  def runJob(yaml: String): Map[String,String] = new JobCoordinator(config,dispatch).run(yaml)
  private def error(e: Throwable): FlightRuntimeException = e match {
    case f: FlightRuntimeException => f
    case e if Option(e.getMessage).exists(_.contains("FORBIDDEN")) => CallStatus.UNAUTHORIZED.withDescription(e.getMessage).toRuntimeException
    case e: IllegalArgumentException => CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage).toRuntimeException
    case e => CallStatus.INTERNAL.withDescription(Option(e.getMessage).getOrElse(e.toString)).toRuntimeException
  }
  def handle(action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    try {
      require(action.getBody.length <= 1024*1024,"control request exceeds 1 MiB")
      val work: () => Array[Byte] = action.getType match {
        case TaskService.ExecuteAction =>
          val task = TaskWire.decode(Metadata.ExecuteTaskRequest.parseFrom(action.getBody))
          () => Metadata.ExecuteTaskResult.newBuilder().setOutputUri(executeHere(task)).build().toByteArray
        case TaskService.JobAction =>
          val yaml = Metadata.RunJobRequest.parseFrom(action.getBody).getYaml
          () => Metadata.RunJobResult.newBuilder().putAllOutputs(runJob(yaml).asJava).build().toByteArray
        case _ => throw CallStatus.UNIMPLEMENTED.withDescription("unknown dacp action").toRuntimeException
      }
      pool.execute(() => try { listener.onNext(new Result(work()));listener.onCompleted() }
        catch { case NonFatal(e) => listener.onError(error(e)) })
    } catch {
      case _: RejectedExecutionException => listener.onError(CallStatus.UNAVAILABLE.withDescription("node task capacity reached; request was not queued").toRuntimeException)
      case NonFatal(e) => listener.onError(error(e))
    }
  }
  def close(): Unit = {
    pool.shutdown()
    if (!pool.awaitTermination(30,TimeUnit.SECONDS)) {pool.shutdownNow();pool.awaitTermination(5,TimeUnit.SECONDS)}
  }
}
object TaskService {
  val ExecuteAction = "dacp.execute_task"
  val JobAction = "dacp.run_job"
}
