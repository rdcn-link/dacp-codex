package dacp.unionserver

import java.nio.file.Path

object Main {
  def main(args:Array[String]):Unit={
    require(args.length==2 && args(0)=="serve","dacp-union-server serve CONFIG; use union-admin for administration")
    val server=new Coordinator(Settings.load(Path.of(args(1))))
    Runtime.getRuntime.addShutdownHook(new Thread(()=>server.close()))
    server.start()
  }
}
