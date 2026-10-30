package dacp.proxy


import java.net.{InetSocketAddress,StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey,Selector,ServerSocketChannel,SocketChannel}
import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger,LongAdder}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

final case class ProxyStats(active:Int,accepted:Long,rejected:Long,upstreamBytes:Long,downstreamBytes:Long,maxBufferedBytes:Long)

/** Opaque, bounded, full-duplex relay. TLS, credentials and Flight stay end to end. */
final class TcpProxy(val config:ProxyConfig) extends AutoCloseable {
  private val log=org.slf4j.LoggerFactory.getLogger(classOf[TcpProxy])
  private val stopping=new AtomicBoolean(false)
  private val active=new AtomicInteger();private val accepted=new LongAdder();private val rejected=new LongAdder()
  private val upBytes=new LongAdder();private val downBytes=new LongAdder()
  @volatile private var maxBuffered=0L
  @volatile private var failure:Throwable=null
  private var selector:Selector=null
  private var listener:ServerSocketChannel=null
  private var thread:Thread=null
  private var state="built"
  private val upstream=new InetSocketAddress(config.upstreamHost,config.upstreamPort)
  require(!upstream.isUnresolved,"upstream host cannot be resolved")
  private val pairs=mutable.Set.empty[Pair]
  private final class Side(val channel:SocketChannel,val pair:Pair,val client:Boolean) {
    val pending=ByteBuffer.allocate(config.bufferBytes) // write-mode bytes waiting for this socket
    var key:SelectionKey=null;var connected=client;var eof=false;var outputClosed=false
    def peer:Side=if(client)pair.back else pair.front
  }
  private final class Pair(frontChannel:SocketChannel,backChannel:SocketChannel) {
    val front=new Side(frontChannel,this,true);val back=new Side(backChannel,this,false)
    val created=System.nanoTime();var touched=created;var uploaded=0L;var downloaded=0L;var closed=false
    val address=frontChannel.getRemoteAddress.toString
  }
  def stats:ProxyStats=ProxyStats(active.get,accepted.sum,rejected.sum,upBytes.sum,downBytes.sum,maxBuffered)
  def boundPort:Int=synchronized {require(state=="started","proxy is not running");listener.getLocalAddress.asInstanceOf[InetSocketAddress].getPort}
  def start():TcpProxy=synchronized {
    require(state=="built","proxy can only start once")
    try {
      selector=Selector.open();listener=ServerSocketChannel.open();listener.configureBlocking(false)
      listener.bind(new InetSocketAddress(config.host,config.port));listener.register(selector,SelectionKey.OP_ACCEPT)
      val local=listener.getLocalAddress.asInstanceOf[InetSocketAddress]
      require(!(upstream.getPort==local.getPort && (upstream.getAddress==local.getAddress || (local.getAddress.isAnyLocalAddress && (upstream.getAddress.isLoopbackAddress || java.net.NetworkInterface.getByInetAddress(upstream.getAddress)!=null)))),"proxy cannot target its own listener")
      state="started";thread=new Thread(()=>loop(),"dacp-proxy-relay");thread.start()
      log.info(s"DACP proxy listening on ${config.host}:${local.getPort}, upstream ${config.upstreamHost}:${config.upstreamPort}")
      this
    }catch{case NonFatal(e)=>close();throw e}
  }
  private def socket(channel:SocketChannel):Unit={channel.configureBlocking(false);channel.setOption(StandardSocketOptions.TCP_NODELAY,java.lang.Boolean.TRUE);channel.setOption(StandardSocketOptions.SO_KEEPALIVE,java.lang.Boolean.TRUE)}
  private def accept():Unit={
    var channel=listener.accept()
    while(channel!=null){
      if(active.get>=config.maxConnections){rejected.increment();channel.close()}
      else {
        var back:SocketChannel=null;var pair:Pair=null
        try {
          socket(channel);back=SocketChannel.open();socket(back);pair=new Pair(channel,back)
          pairs+=pair;active.incrementAndGet();accepted.increment()
          pair.front.key=channel.register(selector,SelectionKey.OP_READ,pair.front)
          pair.back.connected=back.connect(upstream)
          pair.back.key=back.register(selector,if(pair.back.connected)SelectionKey.OP_READ else SelectionKey.OP_CONNECT,pair.back)
        }catch{case NonFatal(e)=>if(pair!=null)closePair(pair,"connect failed")else {channel.close();if(back!=null)back.close()};log.debug("Upstream connection failed",e)}
      }
      channel=listener.accept()
    }
  }
  private def update(pair:Pair):Unit={
    for(side<-Vector(pair.front,pair.back)){
      if(side.connected && side.peer.eof && side.pending.position()==0 && !side.outputClosed){side.channel.shutdownOutput();side.outputClosed=true}
      if(side.key!=null && side.key.isValid){
        val ops=if(!side.connected)SelectionKey.OP_CONNECT else
          (if(!side.eof && side.peer.pending.hasRemaining)SelectionKey.OP_READ else 0) |
          (if(side.pending.position()>0)SelectionKey.OP_WRITE else 0)
        side.key.interestOps(ops)
      }
    }
    if(pair.front.eof && pair.back.eof && pair.front.pending.position()==0 && pair.back.pending.position()==0)closePair(pair,"completed")
  }
  private def transfer(side:Side):Unit={
    if(side.key.isConnectable){if(side.channel.finishConnect())side.connected=true}
    if(side.connected && side.key.isValid && side.key.isReadable){
      val count=side.channel.read(side.peer.pending)
      if(count<0)side.eof=true
      else if(count>0){side.pair.touched=System.nanoTime();if(side.client){upBytes.add(count);side.pair.uploaded+=count}else{downBytes.add(count);side.pair.downloaded+=count}}
    }
    if(side.connected && side.key.isValid && side.key.isWritable){side.pending.flip();val count=try side.channel.write(side.pending)finally side.pending.compact();if(count>0)side.pair.touched=System.nanoTime()}
    maxBuffered=math.max(maxBuffered,side.pair.front.pending.position.toLong+side.pair.back.pending.position)
    update(side.pair)
  }
  private def closePair(pair:Pair,reason:String):Unit=if(!pair.closed){
    pair.closed=true;pairs-=pair;active.decrementAndGet()
    for(side<-Vector(pair.front,pair.back)){if(side.key!=null)side.key.cancel();try side.channel.close()catch{case NonFatal(_)=>()}}
    log.info(s"Proxy connection closed peer=${pair.address} reason=$reason upstream_bytes=${pair.uploaded} downstream_bytes=${pair.downloaded}")
  }
  private def loop():Unit={
    try while(!stopping.get){
      selector.select(500)
      val selected=selector.selectedKeys().iterator()
      while(selected.hasNext){val key=selected.next();selected.remove()
        if(key.isValid)if(key.isAcceptable)accept()else {
          val side=key.attachment().asInstanceOf[Side]
          try if(!side.pair.closed)transfer(side)catch{case NonFatal(e)=>closePair(side.pair,"connection error");log.debug("Proxy connection error",e)}
        }
      }
      val now=System.nanoTime()
      pairs.toVector.foreach{p=>
        if(!p.back.connected && now-p.created>=config.connectTimeoutSeconds*1000000000L)closePair(p,"connect timeout")
        else if(config.idleTimeoutSeconds>0 && now-p.touched>=config.idleTimeoutSeconds*1000000000L)closePair(p,"idle timeout")
      }
    }catch{case NonFatal(e)=>if(!stopping.get){failure=e;log.error("Proxy relay failed",e)}}
    finally{pairs.toVector.foreach(closePair(_,"proxy shutdown"));try listener.close()finally selector.close()}
  }
  def awaitTermination():Unit={val running=synchronized{require(thread!=null,"proxy not started");thread};running.join();if(failure!=null)throw new IllegalStateException("proxy relay failed",failure)}
  def close():Unit=synchronized {
    if(state!="closed"){
      state="closed";stopping.set(true);if(selector!=null)selector.wakeup()
      if(thread!=null && thread!=Thread.currentThread()){thread.join(10000);require(!thread.isAlive,"proxy failed to stop")}
      else if(thread==null){if(listener!=null)listener.close();if(selector!=null)selector.close()}
    }
  }
}
