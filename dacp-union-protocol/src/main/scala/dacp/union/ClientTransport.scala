package dacp.union


import java.nio.file.Path
import java.net.URI
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import io.grpc.{Metadata,ClientCall,MethodDescriptor,CallOptions,Channel,ClientInterceptor,ForwardingClientCall}
import io.grpc.netty.{NettyChannelBuilder,GrpcSslContexts}
import io.netty.handler.ssl.{SslProvider,SslContextBuilder}

/** Shared connection, TLS and per-RPC credential implementation for both Scala SDK artifacts. */
object ClientTransport {
  def open(allocator:BufferAllocator,origin:String,ca:Option[Path],cert:Option[Path],key:Option[Path],token:()=>Option[String],
    pin:Option[String]=None,observed:String=>Unit=_=>(),traffic:Option[FlightTraffic]=None,allowAnonymousWithoutCa:Boolean=false):FlightClient={
    val canonical=Json.origin(origin);val u=new URI(canonical)
    require(cert.isDefined==key.isDefined,"client certificate and key must be provided together")
    val anonymousWithoutCa=allowAnonymousWithoutCa && ca.isEmpty && cert.isEmpty && pin.isEmpty && token().isEmpty
    val ssl=GrpcSslContexts.configure(SslContextBuilder.forClient(),SslProvider.JDK)
    if(pin.nonEmpty){
      require(ca.nonEmpty && cert.nonEmpty,"pinned service connections require CA and identity")
      ssl.trustManager(new Tls(cert.get,key.get,ca.get).trustManager(pin,observed))
    }else if(anonymousWithoutCa)ssl.trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
    else ca.foreach(p=>ssl.trustManager(p.toFile))
    cert.foreach(p=>ssl.keyManager(p.toFile,key.get.toFile))
    val interceptor=new ClientInterceptor {
      def interceptCall[A,B](method:MethodDescriptor[A,B],options:CallOptions,next:Channel):ClientCall[A,B]=
        new ForwardingClientCall.SimpleForwardingClientCall[A,B](next.newCall(method,
          traffic.map(t=>options.withStreamTracerFactory(t.factory(canonical,method.getFullMethodName))).getOrElse(options))) {
          override def start(listener:ClientCall.Listener[B],headers:Metadata):Unit={
            val credential=token()
            require(!anonymousWithoutCa || credential.isEmpty,"reconnect with a trusted CA before sending credentials")
            credential.foreach(v=>headers.put(Metadata.Key.of("authorization",Metadata.ASCII_STRING_MARSHALLER),"Bearer "+v))
            super.start(listener,headers)
          }
        }
    }
    // For enrollment without a saved pin, still observe a CA-verified server certificate.
    if(pin.isEmpty && cert.nonEmpty && ca.nonEmpty)ssl.trustManager(new Tls(cert.get,key.get,ca.get).trustManager(None,observed))
    val channel=NettyChannelBuilder.forAddress(u.getHost,u.getPort).sslContext(ssl.build()).maxInboundMessageSize(Rpc.MaxMessage).intercept(interceptor).build()
    FlightGrpcUtils.createFlightClient(allocator,channel)
  }
}
