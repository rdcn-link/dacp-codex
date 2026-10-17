package dacp
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.channels.{Channels, FileChannel}
import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight._
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Source, Sink}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import com.github.dockerjava.api.model.{Bind, Volume, HostConfig, AccessMode}
import dacp.protocol.Metadata
import org.yaml.snakeyaml.Yaml

case class Task(id: String, inputs: Seq[String], operator: String, params: java.util.Map[String,Object], output: String, node: Option[String] = None)
object Jobs {
  def load(path: Path): Seq[Task] = {
    parse(Files.readString(path))
  }
  def parse(yaml: String): Seq[Task] = {
    val doc = new Yaml().load[java.util.Map[String,Object]](yaml).asScala
    require(doc.contains("job"),"job name required")
    val tasks = doc("tasks").asInstanceOf[java.util.List[java.util.Map[String,Object]]].asScala.map { raw =>
      val m = raw.asScala
      val inputs = m.get("inputs").map(_.asInstanceOf[java.util.List[String]].asScala.toSeq).getOrElse(Seq(m("input").toString))
      require(inputs.nonEmpty,"empty inputs")
      Task(m("id").toString,inputs,m("operator").toString,m.getOrElse("params",new java.util.LinkedHashMap[String,Object]()).asInstanceOf[java.util.Map[String,Object]],m("output").toString,m.get("node").map(_.toString))
    }.toSeq
    require(tasks.map(_.id).distinct.size == tasks.size,"duplicate task ids")
    require(tasks.map(_.output).distinct.size == tasks.size,"duplicate outputs")
    tasks.foreach(t => require(t.output.matches("[a-zA-Z0-9_-]+"),"invalid output name"))
    val outputs = tasks.map(_.output).toSet
    tasks.foreach(t => t.inputs.filterNot(_.startsWith("dacp://")).foreach(i => require(outputs(i.takeWhile(_ != '/')),"unknown upstream output")))
    var remaining = tasks; var done = Set.empty[String]; val ordered = Vector.newBuilder[Task]
    while (remaining.nonEmpty) {
      val ready = remaining.filter(t => t.inputs.filterNot(_.startsWith("dacp://")).forall(i => done(i.takeWhile(_ != '/'))))
      require(ready.nonEmpty,"cyclic task DAG")
      ready.foreach { t => ordered += t; done += t.output }; remaining = remaining.filterNot(ready.contains)
    }
    ordered.result()
  }
}
/** Task inputs use the same published namespace as SDK clients. */
private[dacp] object TaskInputs {
  def discover(client: FlightClient, dataset: String, path: Option[String]): Seq[(String,String)] = path match {
    case Some(logical) =>
      LogicalPath.validate(logical)
      val info = client.getInfo(FlightDescriptor.path("datasets",dataset,logical))
      val item = Metadata.Item.parseFrom(info.getAppMetadata)
      require(item.getKind == "data", "task input must be a data item")
      Seq(item.getPath -> item.getFormat)
    case None =>
      val info = client.getInfo(FlightDescriptor.path("datasets",dataset))
      Metadata.DataSet.parseFrom(info.getAppMetadata).getItemsList.asScala
        .filter(_.getKind == "data").map(i => i.getPath -> i.getFormat).toVector
  }
}
class TaskExecutor(config: Config, allocator: BufferAllocator, registry: OutputRegistry) {
  private case class Remote(client: FlightClient, dataset: String, items: Seq[(String,String)])
  private def remote(value: String): Remote = {
    val uri = new URI(if (value.startsWith("dacp://")) value else config.uri(value))
    require(uri.getScheme == "dacp" && uri.getHost != null && uri.getQuery == null && uri.getFragment == null,"invalid input URI")
    val parts = uri.getPath.stripPrefix("/").split("/",2)
    require(parts.nonEmpty && parts(0).nonEmpty,"missing input dataset")
    val client = NodeConnection.open(config,allocator,value)
    try {
      val items = TaskInputs.discover(client, parts(0), parts.lift(1).map("/" + _))
      Remote(client,parts(0),items)
    } catch { case t: Throwable => client.close(); throw t }
  }
  def execute(task: Task): String = {
    require(registry.lookup(task.output).isEmpty,"output name already used")
    implicit val system: ActorSystem = ActorSystem("dacp-tasks")
    implicit val ec: ExecutionContext = system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")
    val dockerConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build()
    val transport = new ApacheDockerHttpClient.Builder().dockerHost(dockerConfig.getDockerHost).sslConfig(dockerConfig.getSSLConfig).build()
    val docker = DockerClientImpl.getInstance(dockerConfig,transport)
    try {
      require(!config.hasDataset(task.output),"output shadows configured dataset")
      val image = OperatorLibrary.resolve(config,task.operator,docker)
      val temp = Files.createTempDirectory("dacp-input-")
      Files.createDirectories(config.outputRoot)
      val output = Files.createDirectory(config.outputRoot.resolve(task.output + "-" + UUID.randomUUID().toString))
      var container: String = null
      var published = false
      val sources = scala.collection.mutable.ArrayBuffer.empty[Remote]
      val channels = scala.collection.mutable.ArrayBuffer.empty[FileChannel]
      try {
        task.inputs.foreach(i => sources += remote(i))
        val inputs = sources.flatMap(r => r.items.map { case (p,f) => (r,p,f) }).toVector
        require(inputs.nonEmpty,"empty input dataset")
        val json = Data.mapper.createArrayNode()
        inputs.zipWithIndex.foreach { case ((r,p,f),i) => val n = json.addObject(); n.put("file",s"$i.arrow"); n.put("path",p); n.put("format",f) }
        Files.write(temp.resolve("manifest.json"),Data.mapper.writeValueAsBytes(json))
        val paramsFile = temp.resolve("params.json"); Files.write(paramsFile,Data.mapper.writeValueAsBytes(task.params))
        val multi = task.inputs.size > 1
        def transfer(index: Int): Unit = {
          val (r,item,_) = inputs(index)
          val ticket = Metadata.Ticket.newBuilder().setDataset(r.dataset).setItem(item).setView("bytes").build()
          val stream = r.client.getStream(new Ticket(ticket.toByteArray))
          val channel = if (multi) FileChannel.open(temp.resolve(s"$index.arrow"),StandardOpenOption.WRITE,StandardOpenOption.CREATE_NEW)
            else channels(index)
          val sink = Channels.newOutputStream(channel)
          // Keep FIFO descriptor alive through container startup: tiny streams may finish before its reader opens.
          val outputStream = if (multi) sink else new java.io.FilterOutputStream(sink) { override def close(): Unit = flush() }
          val writer = new ArrowStreamWriter(stream.getRoot,null,outputStream)
          try { writer.start(); while (stream.next()) writer.writeBatch(); writer.end() }
          finally { writer.close(); stream.close(); if (multi) channel.close() }
        }
        if (multi) {
          // Architecture: all inputs are completely materialized before a multi-input operator starts.
          inputs.indices.foreach(transfer)
        } else inputs.indices.foreach { i =>
          val pipe = temp.resolve(s"$i.arrow")
          require(new ProcessBuilder("mkfifo",pipe.toString).start().waitFor() == 0,"mkfifo failed")
          // RDWR avoids an uninterruptible FIFO open if the container fails before opening its reader.
          channels += FileChannel.open(pipe,StandardOpenOption.READ,StandardOpenOption.WRITE)
        }
        val binds = List(new Bind(temp.toAbsolutePath.toString,new Volume("/input"),AccessMode.ro),
          new Bind(output.toAbsolutePath.toString,new Volume("/output"),AccessMode.rw))
        val host = HostConfig.newHostConfig().withBinds(binds.asJava).withNetworkMode("none").withMemory(1024L*1024*1024).withPidsLimit(128L)
        container = docker.createContainerCmd(image).withHostConfig(host)
          .withEnv(s"DACP_OPERATOR=${task.operator}")
          .withCmd("operator","/input","/output","/input/params.json").exec().getId
        docker.startContainerCmd(container).exec()
        // Pekko pulls one transfer at a time; FIFO and Flight transport readiness bound buffering.
        val pumping = if (multi) Future.successful(()) else Source(inputs.indices.toList).mapAsync(1)(i => Future(transfer(i))).runWith(Sink.ignore).map(_ => ())
        val deadline = System.nanoTime() + 30.minutes.toNanos
        while (docker.inspectContainerCmd(container).exec().getState.getRunning && System.nanoTime() < deadline) {
          if (pumping.isCompleted) Await.result(pumping,Duration.Zero)
          Thread.sleep(100)
        }
        require(System.nanoTime() < deadline,"operator timed out")
        if (docker.inspectContainerCmd(container).exec().getState.getExitCodeLong != 0L) {
          val logs = new StringBuilder()
          val callback = new com.github.dockerjava.core.command.LogContainerResultCallback() {
            override def onNext(frame: com.github.dockerjava.api.model.Frame): Unit = {
              if (logs.length < 65536) logs.append(new String(frame.getPayload,java.nio.charset.StandardCharsets.UTF_8))
            }
          }
          docker.logContainerCmd(container).withStdOut(true).withStdErr(true).withTail(100).exec(callback).awaitCompletion()
          throw new IllegalStateException("operator failed: " + logs.toString)
        }
        Await.result(pumping,30.seconds)
        OperatorOutput.decode(output,allocator)
        registry.register(task.output,output); published = true
        println(s"task ${task.id}: ${config.uri(task.output)} (TTL ${config.ttlSeconds}s)")
        config.uri(task.output)
      } finally {
        channels.foreach(c => if (c.isOpen) c.close())
        try {
          if (container != null) try docker.removeContainerCmd(container).withForce(true).exec()
          catch { case scala.util.control.NonFatal(e) => System.err.println("container cleanup: " + e.getMessage) }
        } finally {
          try sources.foreach(_.client.close())
          finally { Data.deleteTree(temp); if (!published) Data.deleteTree(output) }
        }
      }
    } finally { docker.close(); transport.close(); Await.result(system.terminate(),30.seconds) }
  }
}
