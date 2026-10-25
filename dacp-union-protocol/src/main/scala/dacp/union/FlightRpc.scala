package dacp.union

import com.fasterxml.jackson.databind.JsonNode
import dacp.{OperatorFrame,OperatorFrames}
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.{CallContext,ServerStreamListener,StreamListener}
import org.apache.arrow.memory.{BufferAllocator,RootAllocator}
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.types.pojo.{Schema,Field,FieldType,ArrowType}
import io.grpc.{Context,Contexts,Grpc,Metadata,ServerCall,ServerCallHandler,ServerInterceptor}
import io.grpc.netty.{NettyChannelBuilder,NettyServerBuilder,GrpcSslContexts}
import io.netty.handler.ssl.{SslProvider,SslContextBuilder}
import java.util.concurrent.{ConcurrentHashMap,Executors,TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Public Flight wire contract; no identity is accepted from action bodies or tickets. */
object Rpc {
  val Prefix="dacp.union."
  val MaxJson=1024*1024
  val MaxMessage=9*1024*1024
  val Frames=new Schema(java.util.List.of(new Field("data",FieldType.nullable(new ArrowType.Binary()),java.util.List.of[Field]())))
  val Events=new Schema(java.util.List.of(new Field("event",FieldType.nullable(new ArrowType.Binary()),java.util.List.of[Field]())))
  private val hopsKey:Context.Key[String]=Context.key("dacp-proxy-hops")
  def proxyHops:Vector[String]=Option(hopsKey.get()).filter(_.nonEmpty).map(_.split(",").toVector).getOrElse(Vector.empty)
  def hopOptions(hops:Vector[String]):HeaderCallOption={
    require(hops.size<=8 && hops.forall(_.matches("[a-zA-Z0-9_-]+")),"invalid proxy path")
    val headers=new FlightCallHeaders();headers.insert("x-dacp-proxy-hops",hops.mkString(","));new HeaderCallOption(headers)
  }
  final case class Principal(subject:String,kind:String,certificate:Option[String],credentialId:Option[String]=None,unionId:Option[String]=None,active:()=>Boolean=()=>true)
  private val principalKey:Context.Key[Principal]=Context.key("dacp-principal")
  def principal:Principal=Option(principalKey.get()).getOrElse(throw UnionFailure(FlightStatusCode.UNAUTHENTICATED,"authenticated principal required"))
  def identity:String={val p=principal;if(p.kind=="token")p.subject else p.certificate.getOrElse(p.subject)}
  def isAuthenticatedCall:Boolean=principalKey.get()!=null
  def clientOperation():Unit=if(isAuthenticatedCall && !Set("token","union")(principal.kind))throw CallStatus.UNAUTHORIZED.withDescription("client credential required").toRuntimeException
  def requireClient():Unit=if(!Set("token","union")(principal.kind))throw UnionFailure(FlightStatusCode.UNAUTHORIZED,"client credential required")
  def requireUnion(id:String):Unit={val p=principal;if(p.kind!="union" || !p.unionId.contains(id))throw UnionFailure(FlightStatusCode.UNAUTHORIZED,"Union service credential required")}
  def checkService():Unit={val p=principal;if(p.kind=="union" && !p.active())throw UnionFailure(FlightStatusCode.UNAUTHORIZED,"Union credential revoked or expired")}
  def interceptor(credentials:Option[Credentials]):ServerInterceptor=new ServerInterceptor {
    override def interceptCall[A,B](call:ServerCall[A,B],headers:Metadata,next:ServerCallHandler[A,B]):ServerCall.Listener[A]={
      val started=System.nanoTime()
      val operation=call.getMethodDescriptor.getBareMethodName
      def audit(subject:String,status:String):Unit=credentials.foreach(_.auditAccess(operation,subject,status,(System.nanoTime()-started)/1000000))
      def reject(message:String):ServerCall.Listener[A]={audit("unknown","UNAUTHENTICATED");call.close(io.grpc.Status.UNAUTHENTICATED.withDescription(message),new Metadata());new ServerCall.Listener[A]() {}}
      val session=call.getAttributes.get(Grpc.TRANSPORT_ATTR_SSL_SESSION)
      if(session==null)return reject("TLS required")
      val peer=try Some(Tls.fingerprint(session.getPeerCertificates.head))catch{case NonFatal(_)=>None}
      val auth=Option(headers.get(Metadata.Key.of("authorization",Metadata.ASCII_STRING_MARSHALLER)))
      val actor=auth match {
        case Some(header) if header.startsWith("Bearer ") && header.length<=1024=>
          credentials.flatMap(_.authenticate(header.substring(7),peer)).map { c=>
            Principal(c.subject,c.kind,peer,Some(c.id),Option(c.unionId).filter(_.nonEmpty),()=>credentials.exists(_.valid(c.id)))
          }
        case Some(_)=>None
        case None=>peer.map(id=>Principal(id,"peer",peer))
      }
      if(actor.isEmpty)return reject("valid credential required")
      if(actor.get.kind=="peer" && !Set("DoGet","DoAction")(call.getMethodDescriptor.getBareMethodName))return reject("client credential required")
      val hops=Option(headers.get(Metadata.Key.of("x-dacp-proxy-hops",Metadata.ASCII_STRING_MARSHALLER))).getOrElse("")
      val audited=new io.grpc.ForwardingServerCall.SimpleForwardingServerCall[A,B](call) {
        override def close(status:io.grpc.Status,trailers:Metadata):Unit={
          try audit(actor.get.subject,status.getCode.toString)finally super.close(status,trailers)
        }
      }
      Contexts.interceptCall(Context.current().withValue(principalKey,actor.get).withValue(hopsKey,hops),audited,headers,next)
    }
  }
  def secure(builder:FlightServer.Builder,tls:Tls,credentials:Option[Credentials]=None):FlightServer.Builder=
    builder.useTls(tls.cert.toFile,tls.key.toFile)
      .transportHint("grpc.builderConsumer",new java.util.function.Consumer[NettyServerBuilder]{def accept(b:NettyServerBuilder):Unit={
        val ssl=GrpcSslContexts.configure(SslContextBuilder.forServer(tls.cert.toFile,tls.key.toFile),SslProvider.JDK)
          .trustManager(tls.ca.toFile).clientAuth(io.netty.handler.ssl.ClientAuth.OPTIONAL).build()
        b.sslContext(ssl);b.intercept(interceptor(credentials));()
      }})
  def parse(bytes:Array[Byte]):JsonNode={require(bytes.length<=MaxJson,"control message exceeds 1 MiB");val n=Json.mapper.readTree(bytes);require(n!=null && n.isObject,"Union request must be an object");n}
  def bytes(n:JsonNode):Array[Byte]={val b=Json.mapper.writeValueAsBytes(n);require(b.length<=MaxJson,"control message exceeds 1 MiB");b}
  def ticket(kind:String,n:JsonNode):Ticket={val v=n.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();v.put("type",Prefix+kind);new Ticket(bytes(v))}
  def isTicket(bytes:Array[Byte]):Boolean=bytes.dropWhile(b=>b==32 || b==9 || b==10 || b==13).headOption.contains('{'.toByte)
  def failure(t:Throwable):FlightRuntimeException=t match {
    case e:FlightRuntimeException=>e
    case e:UnionFailure=>new CallStatus(e.code).withDescription(e.detail).toRuntimeException
    case _:IllegalArgumentException|_:com.fasterxml.jackson.core.JsonProcessingException=>CallStatus.INVALID_ARGUMENT.withDescription("invalid Union request").toRuntimeException
    case _:java.util.concurrent.TimeoutException=>CallStatus.TIMED_OUT.withDescription("Union execution expired").toRuntimeException
    case _:java.util.concurrent.CancellationException|_:InterruptedException=>CallStatus.CANCELLED.withDescription("Union stream cancelled").toRuntimeException
    case _=>CallStatus.INTERNAL.withDescription("Union execution failed").toRuntimeException
  }
  def action(ctx:CallContext,a:Action,out:StreamListener[Result])(run:(String,JsonNode,String)=>JsonNode):Unit=
    try {val who=identity;require(a.getType.startsWith(Prefix),"unknown Union action");val n=run(a.getType.stripPrefix(Prefix),parse(a.getBody),who);out.onNext(new Result(bytes(n)));out.onCompleted()}
    catch{case NonFatal(t)=>out.onError(failure(t))}
  def stream(ctx:CallContext,t:Ticket,out:ServerStreamListener)(run:(JsonNode,String)=>Unit):Unit=
    try run(parse(t.getBytes),identity)catch{case NonFatal(e)=>out.error(failure(e))}

  /** One owned binary value per Arrow batch. Backpressure never buffers an unbounded result. */
  final class Sink(allocator:BufferAllocator,val listener:ServerStreamListener,val schema:Schema) extends AutoCloseable {
    private val root=VectorSchemaRoot.create(schema,allocator)
    private var started=false
    def active():Unit=if(listener.isCancelled)throw new java.util.concurrent.CancellationException("Flight consumer disconnected")
    def start():Unit=if(!started){active();listener.start(root);started=true}
    def put(value:Array[Byte]):Unit={
      start();while(!listener.isReady){active();Thread.sleep(10)};active()
      root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,value);root.setRowCount(1);listener.putNext()
    }
    def frame(f:OperatorFrame):Unit=put(OperatorFrames.encode(f))
    def event(n:JsonNode):Unit=put(bytes(n))
    def completed():Unit={start();active();listener.completed()}
    def close():Unit=root.close()
  }
}
trait BinarySource extends AutoCloseable { def next():Option[Array[Byte]] }
final class Remote(tls:Option[Tls],traffic:Option[FlightTraffic]=None,credential:String=>Option[String]=_=>None,ca:Option[java.nio.file.Path]=None) extends AutoCloseable {
  private val allocator=new RootAllocator()
  private final class Connection(val client:FlightClient,val peer:AtomicReference[String])
  private val connections=new ConcurrentHashMap[String,Connection]()
  private val sources=ConcurrentHashMap.newKeySet[BinarySource]()
  private val lifecycle=new Object()
  private var closed=false
  private def connection(origin:String,pin:Option[String]):Connection=lifecycle.synchronized {
    require(!closed,"Union connection closed")
    val canonical=Json.origin(origin);pin.foreach(Json.identity)
    connections.computeIfAbsent(canonical+"/"+pin.getOrElse(""),_=>{
      val u=java.net.URI.create(canonical);val peer=new AtomicReference[String]()
      val client=ClientTransport.open(allocator,canonical,tls.map(_.ca).orElse(ca),tls.map(_.cert),tls.map(_.key),()=>credential(canonical),pin,id=>peer.set(id),traffic)
      new Connection(client,peer)
    })
  }
  /** Public catalog discovery uses transport trust only, without Union execution grants. */
  def dataClient(origin:String,pin:Option[String]):FlightClient=connection(origin,pin).client
  def catalog(origin:String,pin:Option[String]=None,timeout:Long=5,hops:Vector[String]=Vector.empty):Vector[FlightInfo]=
    connection(origin,pin).client.listFlights(Criteria.ALL,CallOptions.timeout(timeout,TimeUnit.SECONDS),Rpc.hopOptions(hops)).asScala.toVector
  def peer(origin:String,pin:Option[String]=None):String=Option(connection(origin,pin).peer.get()).getOrElse(throw new IllegalStateException("no verified server identity"))
  def action(origin:String,name:String,body:JsonNode=Json.obj(),pin:Option[String]=None,timeout:Long=30,hops:Vector[String]=Vector.empty):JsonNode={
    require(timeout>0)
    try {
      val responses=connection(origin,pin).client.doAction(new Action(Rpc.Prefix+name,Rpc.bytes(body)),CallOptions.timeout(timeout,TimeUnit.SECONDS),Rpc.hopOptions(hops))
      require(responses.hasNext,"missing action response");val result=Rpc.parse(responses.next().getBody);require(!responses.hasNext,"multiple action responses");result
    }catch{case e:FlightRuntimeException if Set(FlightStatusCode.INVALID_ARGUMENT,FlightStatusCode.UNAUTHORIZED,FlightStatusCode.UNAUTHENTICATED,FlightStatusCode.NOT_FOUND,FlightStatusCode.ALREADY_EXISTS,FlightStatusCode.RESOURCE_EXHAUSTED,FlightStatusCode.UNIMPLEMENTED)(e.status().code())=>throw UnionFailure(e.status().code(),e.status().description())}
  }
  def stream(origin:String,kind:String,body:JsonNode,pin:Option[String]=None,timeout:Long=1800,hops:Vector[String]=Vector.empty):BinarySource=lifecycle.synchronized {
    require(!closed,"Union connection closed")
    require(timeout>0)
    val reader=connection(origin,pin).client.getStream(Rpc.ticket(kind,body),CallOptions.timeout(timeout,TimeUnit.SECONDS),Rpc.hopOptions(hops))
    val expected=if(kind=="events")Rpc.Events else Rpc.Frames
    val source=new BinarySource {
      private val lock=new Object()
      private var closed=false
      def next():Option[Array[Byte]]=lock.synchronized {try {
        if(closed)None
        else if(!reader.next()){close();None}
        else {val root=reader.getRoot;require(root.getSchema==expected && root.getRowCount==1,"invalid Union Flight envelope");val value=root.getVector(0).asInstanceOf[VarBinaryVector].get(0);require(value!=null && value.length<=Rpc.MaxMessage,"invalid Union payload");Some(value)}
      }catch{case NonFatal(t)=>close();throw t}}
      def close():Unit={
        // Cancel before taking the lock to unblock a concurrent next(). Cleanup must
        // finish even when cancellation interrupted the execution thread.
        reader.cancel("Union stream closed",null)
        lock.synchronized {if(!closed){
          closed=true;val interrupted=Thread.interrupted()
          try reader.close()finally {sources.remove(this);if(interrupted)Thread.currentThread().interrupt()}
        }}
      }
    }
    sources.add(source);source
  }
  def close():Unit=lifecycle.synchronized {if(!closed){
    closed=true
    sources.asScala.foreach(_.close());connections.values.asScala.foreach(_.client.close());connections.clear();allocator.close()
  }}
}
/** Standalone U listener. Workers install these operations on their existing Flight listener. */
final class Endpoint(host:String,port:Int,tls:Tls,allocator:BufferAllocator,producer:FlightProducer,credentials:Option[Credentials]=None) extends AutoCloseable {
  private val threads=Executors.newFixedThreadPool(128)
  private val server=Rpc.secure(FlightServer.builder(allocator,Location.forGrpcTls(host,port),producer),tls,credentials)
    .maxInboundMessageSize(Rpc.MaxMessage).executor(threads).build()
  def start():Unit={server.start();()}
  def boundPort:Int=server.getPort
  def close():Unit={server.close();threads.shutdownNow();require(threads.awaitTermination(10,TimeUnit.SECONDS),"Flight handlers failed to stop")}
}
