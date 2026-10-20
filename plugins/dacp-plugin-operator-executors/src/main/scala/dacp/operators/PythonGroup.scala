package dacp.operators
import dacp._
import java.nio.file.Files
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipFile

/** Python fusion is explicit: a thread-safe process(ctx) callable, one shared code bundle. */
private[operators] object PythonGroup {
  private def supported(s:OperatorGroupStep):Boolean={
    val path=s.program.artifact.get
    val source=if(s.artifactPath.get.endsWith(".py")){if(Files.size(path)>4*1024*1024)return false;new String(Files.readAllBytes(path),UTF_8)}
    else{val zip=new ZipFile(path.toFile);try Option(zip.getEntry(s.program.entrypoint.replace('.','/')+".py")).filter(_.getSize<=4*1024*1024).map { e=>val in=zip.getInputStream(e);try new String(in.readNBytes(4*1024*1024+1),UTF_8)finally in.close() }.getOrElse("")finally zip.close()}
    "(?m)^def process\\s*\\(".r.findFirstIn(source).nonEmpty
  }
  private def resource(name:String):Array[Byte]={val in=getClass.getResourceAsStream("/"+name);require(in!=null,"missing group runner resource");try in.readAllBytes()finally in.close()}
  def command(group:OperatorGroup):Option[OperatorCommand]={
    if(group.steps.flatMap(_.program.artifact).distinct.size!=1 || !group.steps.forall(s=>s.program.configuration.isEmpty && s.program.artifact.nonEmpty && s.artifactPath.exists(p=>p.endsWith(".py")||p.endsWith(".whl")) && supported(s)))return None
    Some(OperatorCommand(Some(Vector("/usr/local/bin/python","-u","/input/python-group.py")),arguments=Vector("/input"),
      environment=Map("PYTHONDONTWRITEBYTECODE"->"1","PIP_DISABLE_PIP_VERSION_CHECK"->"1"),
      files=Map("python-group.py"->resource("python-group.py"),"group-sdk.py"->resource("group-sdk.py"),"group.json"->JvmGroup.definition(group)),
      mounts=group.steps.flatMap(s=>s.program.artifact.zip(s.artifactPath).map{case(file,path)=>path->file}).toMap))
  }
}
