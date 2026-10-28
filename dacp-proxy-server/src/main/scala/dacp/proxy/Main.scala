package dacp.proxy

import java.nio.file.Path

object Main {
  def main(args:Array[String]):Unit={
    require(args.length==1,"Usage: dacp-proxy-server CONFIG.yaml")
    val proxy=new TcpProxy(ProxyConfig.load(Path.of(args(0))))
    sys.addShutdownHook(proxy.close())
    proxy.start().awaitTermination()
  }
}
