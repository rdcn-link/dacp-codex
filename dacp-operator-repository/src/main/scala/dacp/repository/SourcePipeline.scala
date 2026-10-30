package dacp.repository


import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path}
import java.util.ServiceLoader
import java.util.concurrent.{ThreadPoolExecutor,TimeUnit,ArrayBlockingQueue}
import java.util.jar.{JarEntry,JarOutputStream}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import dacp.operator.{SourceAnalyzer,SourceBuildRecipe,SourceArchive}

private[repository] trait SourceBuildEngine {
  def build(root:Path,manifest:JsonNode,runtime:JsonNode):JsonNode
}

/** One isolated compiler at a time; source is mounted read-only and no repository secrets are mounted. */
private[repository] final class DockerSourceBuild extends SourceBuildEngine {
  private val recipes=ServiceLoader.load(classOf[SourceBuildRecipe]).iterator.asScala.toVector
  private val archives=ServiceLoader.load(classOf[SourceArchive]).iterator.asScala.toVector
  require(recipes.map(_.language).distinct.size==recipes.size,"duplicate source build language")
  private def delete(path:Path):Unit={val walk=Files.walk(path);try walk.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally walk.close()}
  def build(root:Path,manifest:JsonNode,runtime:JsonNode):JsonNode={
    val language=manifest.path("build").path("language").asText
    val recipe=recipes.find(_.language==language).getOrElse(throw new IllegalArgumentException("no build plugin for "+language))
    require(recipe.executor==manifest.path("execution").path("executor").asText,"executor does not match build language")
    val source=manifest.path("provenance").path("source_archive")
    val work=Files.createTempDirectory("dacp-source-build-")
    val name="dacp-build-"+java.util.UUID.randomUUID().toString
    var running:Process=null
    try {
      val sources=Files.createDirectory(work.resolve("source"))
      archives.find(_.supports(source.path("filename").asText)).getOrElse(throw new IllegalArgumentException("source archive plugin unavailable")).unpack(Repository.blob(root,source.path("sha256").asText),source.path("filename").asText,sources)
      val helper=work.resolve("helper.jar");val jar=new JarOutputStream(Files.newOutputStream(helper))
      try {val resource="dacp/repository/build/JvmCompiler.class";jar.putNextEntry(new JarEntry(resource));val in=getClass.getClassLoader.getResourceAsStream(resource);try in.transferTo(jar)finally in.close();jar.closeEntry()}finally jar.close()
      val build=runtime.path("build");val image=build.path("image").asText
      def docker(arguments:Seq[String]):Unit={
        val p=new ProcessBuilder((Seq("docker")++arguments).asJava).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
        if(!p.waitFor(120,TimeUnit.SECONDS)){p.destroyForcibly();throw new IllegalStateException("build environment preparation timed out")}
        require(p.exitValue()==0,"build environment unavailable; administrator must load the registered pinned image")
      }
      val inspect=new ProcessBuilder("docker","image","inspect",image).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
      if(!inspect.waitFor(15,TimeUnit.SECONDS)){inspect.destroyForcibly();throw new IllegalStateException("Docker inspection timed out")}
      if(inspect.exitValue()!=0)docker(Seq("load","-i",Repository.blob(root,build.path("archive").path("sha256").asText).toString))
      val uid="65534";val gid="65534"
      val args=Seq("docker","run","--rm","--name",name,"--network","none","--read-only","--cap-drop","ALL","--security-opt","no-new-privileges","--pids-limit","64","--memory","512m","--cpus","1","--user",s"$uid:$gid","--tmpfs","/tmp:rw,nosuid,nodev,size=64m,mode=1777","--tmpfs","/output:rw,nosuid,nodev,size=64m,mode=1777","--mount",s"type=bind,src=$sources,dst=/source,readonly","--mount",s"type=bind,src=$helper,dst=/builder/helper.jar,readonly","--entrypoint",recipe.command.head,image)++recipe.command.tail
      running=new ProcessBuilder(args.asJava).start()
      val artifact=work.resolve(recipe.artifactFilename);val diagnostic=new java.io.ByteArrayOutputStream()
      val failure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val output=new Thread(()=>try {val in=running.getInputStream;val out=Files.newOutputStream(artifact);try {val buffer=new Array[Byte](65536);var size=0L;var n=in.read(buffer);while(n>=0){size+=n;require(size<=64L*1024*1024,"build artifact exceeds 64 MiB");out.write(buffer,0,n);n=in.read(buffer)}}finally out.close()}catch{case NonFatal(e)=>failure.set(e);running.destroyForcibly()})
      val errors=new Thread(()=>{val in=running.getErrorStream;val buffer=new Array[Byte](8192);try {var n=in.read(buffer);while(n>=0){if(diagnostic.size()<65536)diagnostic.write(buffer,0,math.min(n,65536-diagnostic.size()));n=in.read(buffer)}}catch{case NonFatal(_)=>()}})
      output.start();errors.start()
      if(!running.waitFor(120,TimeUnit.SECONDS)){running.destroyForcibly();throw new IllegalStateException("source build exceeded 120 seconds")}
      output.join(5000);errors.join(5000)
      if(failure.get()!=null)throw failure.get()
      require(running.exitValue()==0,"source build failed: "+diagnostic.toString("UTF-8").take(4000))
      require(Files.size(artifact)>0,"build produced no artifact")
      val entry=manifest.path("execution").path("entrypoint").asText
      recipe.verifyArtifact(artifact,entry)
      Repository.store(root,artifact,recipe.artifactFilename)
    }finally {
      if(running!=null && running.isAlive)running.destroyForcibly()
      val cleanup=new ProcessBuilder("docker","rm","-f",name).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start()
      if(!cleanup.waitFor(15,TimeUnit.SECONDS))cleanup.destroyForcibly()
      delete(work)
    }
  }
}

private[repository] final class SourcePipeline(root:Path,admin:Administration,engine:SourceBuildEngine) extends AutoCloseable {
  private val analyzers=ServiceLoader.load(classOf[SourceAnalyzer]).iterator.asScala.toVector
  require(analyzers.nonEmpty,"no source analysis plugin installed")
  private val executor=new ThreadPoolExecutor(1,1,0L,TimeUnit.SECONDS,new ArrayBlockingQueue[Runnable](32))
  def enqueue(id:String):Unit={
    try executor.execute(()=>{
      val submission=admin.submission(id);val manifest=submission.path("manifest");val source=manifest.path("provenance").path("source_archive")
      try {
        admin.processing(id,"analysis",Repository.mapper.createObjectNode().put("status","running"))
        val applicable=analyzers.filter(_.supports(source.path("filename").asText))
        require(applicable.nonEmpty,"no source analyzer supports this archive")
        val reports=applicable.map(_.analyze(Repository.blob(root,source.path("sha256").asText),source.path("filename").asText))
        val summary=Repository.mapper.createObjectNode().put("status",if(reports.forall(_.path("status").asText=="complete"))"complete"else "failed")
        if(summary.path("status").asText=="complete")summary.put("score",reports.map(_.path("score").asInt).min)
        val list=summary.putArray("reports");reports.foreach(list.add)
        admin.processing(id,"analysis",summary)
        require(summary.path("status").asText=="complete","source analysis failed; inspect the private report")
        admin.processing(id,"build_result",Repository.mapper.createObjectNode().put("status","running"))
        val artifact=engine.build(root,manifest,admin.runtimeFor(manifest))
        admin.processing(id,"build_result",Repository.mapper.createObjectNode().put("status","succeeded").put("completed_at_ms",System.currentTimeMillis()),Some(artifact))
      }catch{case NonFatal(e)=>admin.processing(id,"build_result",Repository.mapper.createObjectNode().put("status","failed").put("error",Option(e.getMessage).getOrElse("build failed").take(4000)))}
    }) catch {case _:java.util.concurrent.RejectedExecutionException=>admin.processing(id,"build_result",Repository.mapper.createObjectNode().put("status","failed").put("error","build queue is full; reject and resubmit later"))}
  }
  admin.submissions().elements.asScala.filter(s=>s.path("status").asText=="pending" && Set("queued","running")(s.path("build_result").path("status").asText)).foreach(s=>enqueue(s.path("id").asText))
  def close():Unit={executor.shutdown();if(!executor.awaitTermination(150,TimeUnit.SECONDS)){executor.shutdownNow();executor.awaitTermination(20,TimeUnit.SECONDS)}}
}
