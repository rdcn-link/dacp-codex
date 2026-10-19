package dacp.unionserver

import java.nio.file.Path
import dacp.union.Json
object Main {
  def main(args:Array[String]):Unit={
    if(args.headOption.contains("history")){dacp.ExecutionHistoryAdmin.main(args.drop(1));return}
    require(args.length>=2,"serve CONFIG | identity CONFIG | invite CONFIG DACP_ORIGIN | members CONFIG | remove CONFIG NODE_ID")
    val settings=Settings.load(Path.of(args(1)))
    args(0) match {
      case "serve"=>
        require(args.length==2);val server=new Coordinator(settings)
        Runtime.getRuntime.addShutdownHook(new Thread(()=>server.close()));server.start()
      case command=>
        val members=new Members(settings)
        try command match {
          case "identity"=>require(args.length==2);println(Json.obj().put("union_id",members.unionId).put("scheduler_identity",members.tls.identity).toPrettyString)
          case "invite"=>require(args.length==3);println(members.invite(args(2)).toPrettyString)
          case "members"=>require(args.length==2);members.all.foreach(m=>println(members.inspect(m).toPrettyString))
          case "remove"=>require(args.length==3);members.remove(args(2))
          case _=>throw new IllegalArgumentException("unknown union command")
        } finally members.close()
    }
  }
}
