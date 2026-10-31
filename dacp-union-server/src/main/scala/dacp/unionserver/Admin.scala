package dacp.unionserver

import java.nio.file.Path
import dacp.union.Json

/** Union administration; member diagnostics use configured service credentials. */
object Admin {
  private val usage="union-admin identity CONFIG\nunion-admin members CONFIG"
  def main(args:Array[String]):Unit=args.toList match {
    case Nil | List("--help")=>dacp.Admin.run("union-admin",Array("--help"));println(usage)
    case List("identity","--help") | List("members","--help")=>println(usage)
    case command :: _ if dacp.Admin.commands(command)=>dacp.Admin.run("union-admin",args)
    case List(operation,config) if Set("identity","members")(operation)=>
      val settings=Settings.load(Path.of(config))
      try {
        if(operation=="identity")println(Json.obj().put("union_id",UnionIdentity.load(settings)).put("scheduler_identity",settings.tls.identity).toPrettyString)
        else {
          val members=new Members(settings)
          try members.all.foreach(m=>println(members.inspect(m).toPrettyString))finally members.close()
        }
      }finally settings.credentials.close()
    case _=>throw new IllegalArgumentException(usage)
  }
}
