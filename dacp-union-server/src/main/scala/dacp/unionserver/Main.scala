package dacp.unionserver


import java.nio.file.Path
import dacp.union.Json
object Main {
  def main(args:Array[String]):Unit={
    if(args.headOption.contains("history")){dacp.ExecutionHistoryAdmin.main(args.drop(1));return}
    if(args.headOption.contains("credentials")){dacp.union.CredentialAdmin.main(args.drop(1));return}
    require(args.length==2,"serve CONFIG | identity CONFIG | members CONFIG")
    val settings=Settings.load(Path.of(args(1)))
    args(0) match {
      case "serve"=>val server=new Coordinator(settings);Runtime.getRuntime.addShutdownHook(new Thread(()=>server.close()));server.start()
      case "identity"=>println(Json.obj().put("union_id",UnionIdentity.load(settings)).put("scheduler_identity",settings.tls.identity).toPrettyString)
      case "members"=>val members=new Members(settings);try members.all.foreach(m=>println(members.inspect(m).toPrettyString))finally members.close()
      case _=>throw new IllegalArgumentException("unknown Union command")
    }
  }
}
