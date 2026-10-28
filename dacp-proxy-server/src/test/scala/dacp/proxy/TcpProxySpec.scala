package dacp.proxy

import java.net.{InetAddress,ServerSocket,Socket}
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch,TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.AnyFunSuite

class TcpProxySpec extends AnyFunSuite {
  private def eventually(check: => Boolean):Unit={val end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!check && System.nanoTime()<end)Thread.sleep(10);assert(check)}
  private def upstream(body:Socket=>Unit)(test:(ServerSocket,AtomicReference[Throwable])=>Unit):Unit={
    val server=new ServerSocket(0,10,InetAddress.getLoopbackAddress)
    val error=new AtomicReference[Throwable]()
    val worker=new Thread(()=>try {val socket=server.accept();socket.setSoTimeout(10000);try body(socket)finally socket.close()}catch{case e:Throwable=>error.set(e)})
    worker.setDaemon(true);worker.start()
    try test(server,error)finally {server.close();worker.join(12000)}
    if(error.get()!=null)throw error.get()
  }
  test("large duplex streams preserve bytes, backpressure and half-close without whole-stream buffers"){
    val bytes=Array.tabulate[Byte](4*1024*1024)(i=>(i%251).toByte)
    upstream{s=>
      val in=s.getInputStream;val out=s.getOutputStream;val chunk=new Array[Byte](8192);var n=in.read(chunk)
      while(n>=0){out.write(chunk,0,n);n=in.read(chunk)}
      out.write("finished".getBytes("UTF-8"));s.shutdownOutput()
    }{(server,_)=>
      val proxy=new TcpProxy(ProxyConfig("127.0.0.1",0,"127.0.0.1",server.getLocalPort,bufferBytes=4096)).start()
      val client=new Socket("127.0.0.1",proxy.boundPort);client.setSoTimeout(15000)
      val sender=new Thread(()=>{client.getOutputStream.write(bytes);client.shutdownOutput()});sender.start()
      try {
        Thread.sleep(100) // Delay reads to exercise downstream backpressure.
        val result=client.getInputStream.readAllBytes();sender.join(10000);assert(!sender.isAlive)
        assert(result.sameElements(bytes++"finished".getBytes("UTF-8")))
        eventually(proxy.stats.active==0)
        assert(proxy.stats.upstreamBytes==bytes.length)
        assert(proxy.stats.downstreamBytes==bytes.length+8)
        assert(proxy.stats.maxBufferedBytes<=8192)
      }finally{client.close();proxy.close()}
      assert(proxy.stats.active==0)
    }
  }
  test("connection limit rejects excess connections and shutdown disconnects active clients"){
    val accepted=new CountDownLatch(1);val release=new CountDownLatch(1)
    upstream{s=>accepted.countDown();release.await(10,TimeUnit.SECONDS)}{(server,_)=>
      val proxy=new TcpProxy(ProxyConfig("127.0.0.1",0,"127.0.0.1",server.getLocalPort,maxConnections=1)).start()
      val first=new Socket("127.0.0.1",proxy.boundPort);first.setSoTimeout(3000)
      val second=new Socket()
      try {
        assert(accepted.await(5,TimeUnit.SECONDS));eventually(proxy.stats.active==1)
        second.connect(new java.net.InetSocketAddress("127.0.0.1",proxy.boundPort));second.setSoTimeout(3000)
        assert(second.getInputStream.read()== -1);eventually(proxy.stats.rejected==1)
        proxy.close();assert(first.getInputStream.read()== -1);assert(proxy.stats.active==0)
      }finally{release.countDown();first.close();second.close();proxy.close()}
    }
  }
  test("failed upstream connections close clients and leave the proxy usable"){
    val unused=new ServerSocket(0);val port=unused.getLocalPort;unused.close()
    val proxy=new TcpProxy(ProxyConfig("127.0.0.1",0,"127.0.0.1",port)).start()
    try {
      for(_<-1 to 2){val client=new Socket("127.0.0.1",proxy.boundPort);client.setSoTimeout(3000);try assert(client.getInputStream.read()== -1)finally client.close()}
      eventually(proxy.stats.active==0);assert(proxy.stats.accepted==2)
    }finally proxy.close()
  }
  test("idle timeout reclaims quiet connections only when explicitly configured"){
    val release=new CountDownLatch(1)
    upstream{s=>release.await(5,TimeUnit.SECONDS)}{(server,_)=>
      val proxy=new TcpProxy(ProxyConfig("127.0.0.1",0,"127.0.0.1",server.getLocalPort,idleTimeoutSeconds=1)).start()
      val client=new Socket("127.0.0.1",proxy.boundPort);client.setSoTimeout(4000)
      try {assert(client.getInputStream.read()== -1);eventually(proxy.stats.active==0)}finally{release.countDown();client.close();proxy.close()}
    }
  }
  test("configuration rejects duplicate keys, executable YAML, unknown options and unsafe resource budgets"){
    val file=Files.createTempFile("proxy-config-",".yaml")
    try {
      Files.writeString(file,"host: 127.0.0.1\nport: 0\nupstream: {host: localhost, port: 8815}\n")
      assert(ProxyConfig.load(file).upstreamPort==8815)
      for(text<-Vector("port: 1\nport: 2\nupstream: {host: localhost, port: 1}","!!java.lang.ProcessBuilder {}","upstream: {host: localhost, port: 1}\ntoken: secret","upstream: {host: localhost, port: 1}\nport: '8815'")){
        Files.writeString(file,text);intercept[Exception](ProxyConfig.load(file))
      }
      intercept[IllegalArgumentException](ProxyConfig("localhost",1,"localhost",2,maxConnections=65536,bufferBytes=1048576))
    }finally Files.deleteIfExists(file)
  }
}
