package dacp

import _root_.xstream.data._

import com.sun.net.httpserver.{HttpExchange,HttpsServer,HttpsConfigurator}
import java.net.{InetSocketAddress,URLDecoder,URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ArrayBlockingQueue,ThreadPoolExecutor,TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.{ArrowBuf,BufferAllocator}
import org.apache.arrow.vector.{VarBinaryVector,VectorSchemaRoot}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.IpcOption
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Authenticated read-only S3 transport. Dataset plugins remain responsible for content. */
case class S3Settings(enabled:Boolean=true,host:String="0.0.0.0",port:Option[Int]=None) {
  require(host.nonEmpty && port.forall(p=>p>=0 && p<=65535),"invalid s3 address")
  def bindPort(flightPort:Int):Int={val p=port.getOrElse(flightPort+2000);require(p>=0 && p<=65535,"set s3.port when Flight port exceeds 63535");p}
}
object S3Settings {
  def read(raw:Option[Object]):S3Settings=raw.map { v=>
    require(v.isInstanceOf[java.util.Map[_,_]],"s3 must be a mapping")
    val m=v.asInstanceOf[java.util.Map[String,Object]].asScala.toMap
    require(m.keySet.subsetOf(Set("enabled","host","port")),"unknown s3 setting")
    require(m.get("enabled").forall(_.isInstanceOf[java.lang.Boolean]),"s3.enabled must be a boolean")
    require(m.get("host").forall(_.isInstanceOf[String]),"s3.host must be a string")
    require(m.get("port").forall(v=>v.isInstanceOf[java.lang.Integer] || v.isInstanceOf[java.lang.Long]),"s3.port must be an integer")
    val port=m.get("port").map(_.toString.toLong);require(port.forall(p=>p>=0 && p<=65535),"invalid s3.port")
    S3Settings(m.get("enabled").forall(_.toString.toBoolean),m.get("host").map(_.toString).getOrElse("0.0.0.0"),port.map(_.toInt))
  }.getOrElse(S3Settings())
}
private[dacp] final class S3Service(config:Config,flightPort:Int,allocator:BufferAllocator) extends AutoCloseable {
  private case class Failure(status:Int,code:String) extends RuntimeException(code)
  private case object RangeComplete extends RuntimeException with scala.util.control.NoStackTrace
  private val closed=new AtomicBoolean()
  @volatile private var startedAt=0L
  @volatile private var startedNanos=0L
  private val requests=new java.util.concurrent.atomic.LongAdder()
  private val succeeded=new java.util.concurrent.atomic.LongAdder()
  private val failed=new java.util.concurrent.atomic.LongAdder()
  private val downloadedBytes=new java.util.concurrent.atomic.LongAdder()
  private val elapsedNanos=new java.util.concurrent.atomic.LongAdder()
  private val methodCounts=new java.util.concurrent.ConcurrentHashMap[String,java.util.concurrent.atomic.LongAdder]()
  private val statusCounts=new java.util.concurrent.ConcurrentHashMap[String,java.util.concurrent.atomic.LongAdder]()
  def snapshot:com.fasterxml.jackson.databind.JsonNode={
    val n=dacp.union.Json.obj().put("enabled",true).put("state",if(closed.get())"stopped"else if(startedAt>0)"running"else "starting")
      .put("listen_host",config.s3.host).put("port",port).put("endpoint",new java.net.URI("https",null,config.publicHost,port,"/",null,null).toASCIIString)
      .put("started_at_ms",startedAt).put("uptime_ms",if(startedNanos==0)0L else TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-startedNanos))
      .put("observed_at_ms",System.currentTimeMillis()).put("read_only",true)
      .put("active_requests",active.size()).put("requests_total",requests.sum()).put("requests_succeeded",succeeded.sum()).put("requests_failed",failed.sum())
      .put("object_bytes_sent",downloadedBytes.sum())
    val done=succeeded.sum()+failed.sum()
    n.put("mean_request_ms",if(done==0)0.0 else elapsedNanos.sum().toDouble/done/1000000)
    n.putArray("authentication").add("SigV4 request headers").add("Presigned GET URLs")
    val methods=n.putObject("methods");Seq("GET","HEAD","OTHER").foreach(k=>methods.put(k,Option(methodCounts.get(k)).map(_.sum()).getOrElse(0L)))
    val statuses=n.putObject("http_statuses");statusCounts.asScala.toVector.sortBy(_._1).foreach{case(k,v)=>statuses.put(k,v.sum())}
    n
  }
  private val server=HttpsServer.create(new InetSocketAddress(config.s3.host,config.s3.bindPort(flightPort)),32)
  server.setHttpsConfigurator(new HttpsConfigurator(new dacp.union.Tls(config.cert,config.key,config.ca).sslContext))
  private val threads=new ThreadPoolExecutor(8,8,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue[Runnable](64),new ThreadPoolExecutor.AbortPolicy())
  private val active=java.util.concurrent.ConcurrentHashMap.newKeySet[HttpExchange]()
  server.setExecutor(threads)
  server.createContext("/",(e:HttpExchange)=>handle(e))
  def port:Int=server.getAddress.getPort
  def start():Unit={server.start();startedAt=System.currentTimeMillis();startedNanos=System.nanoTime();org.slf4j.LoggerFactory.getLogger("dacp.Main").info(s"dacp authenticated read-only S3 listening on https://${config.s3.host}:$port")}
  def close():Unit=if(closed.compareAndSet(false,true)){active.asScala.foreach(_.close());server.stop(0);threads.shutdownNow();threads.awaitTermination(5,TimeUnit.SECONDS)}
  private def check():Unit=if(closed.get() || Thread.currentThread().isInterrupted)throw new java.util.concurrent.CancellationException()
  private def xml(s:String):String=s.flatMap {case '&'=>"&amp;";case '<'=>"&lt;";case '>'=>"&gt;";case '"'=>"&quot;";case '\''=>"&apos;";case c=>c.toString}
  private def tag(k:String,v:String):String=s"<$k>${xml(v)}</$k>"
  private def encode(s:String):String=URLEncoder.encode(s,UTF_8).replace("+","%20").replace("%7E","~")
  private def reply(e:HttpExchange,status:Int,body:String):Unit={
    val b=("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+body).getBytes(UTF_8)
    e.getResponseHeaders.set("Content-Type","application/xml")
    if(e.getRequestMethod=="HEAD")e.sendResponseHeaders(status,-1)else{e.sendResponseHeaders(status,b.length);e.getResponseBody.write(b)}
  }
  private def handle(e:HttpExchange):Unit={
    val began=System.nanoTime();requests.increment()
    val method=if(Set("GET","HEAD")(e.getRequestMethod))e.getRequestMethod else "OTHER"
    methodCounts.computeIfAbsent(method,_=>new java.util.concurrent.atomic.LongAdder()).increment()
    active.add(e);e.getResponseHeaders.set("x-amz-request-id",java.util.UUID.randomUUID().toString);val audit=config.accessLog.begin("s3_"+e.getRequestMethod.toLowerCase(java.util.Locale.ROOT))
    var success=false
    try {
      check()
      if(!Set("GET","HEAD")(e.getRequestMethod))throw Failure(405,"MethodNotAllowed")
      val credential=try dacp.union.SigV4.authenticate(e.getRequestMethod,e.getRequestURI,
        e.getRequestHeaders.asScala.map{case(k,v)=>k->v.asScala.toVector}.toMap,config.credentials)
        catch{case NonFatal(_)=>throw Failure(403,"AccessDenied")}
      audit.actor(credential.subject)
      if(!config.credentials.permissions(credential)("data:read"))throw Failure(403,"AccessDenied")
      val pairs=dacp.union.SigV4.query(e.getRequestURI).filterNot(_._1.startsWith("X-Amz-"))
      require(pairs.map(_._1).distinct.length==pairs.length,"duplicate parameter")
      val q=pairs.toMap
      val path=URLDecoder.decode(e.getRequestURI.getRawPath.replace("+","%2B"),UTF_8)
      val parts=path.stripPrefix("/").split("/",2)
      val bucket=parts(0);val key=if(parts.length==2)parts(1)else ""
      audit.resource(bucket,if(key.isEmpty)""else "/"+key,"bytes")
      if(bucket.isEmpty) {
        require(path=="/" && q.isEmpty,"invalid root request")
        if(e.getRequestMethod=="HEAD")e.sendResponseHeaders(200,-1)
        else reply(e,200,"<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>dacp</ID><DisplayName>DACP</DisplayName></Owner><Buckets>"+config.datasetInfos(()=>check()).map(i=>"<Bucket>"+tag("Name",i.name)+tag("CreationDate","1970-01-01T00:00:00Z")+"</Bucket>").mkString+"</Buckets></ListAllMyBucketsResult>")
      } else {
        DatasetNames.validate(bucket)
        val ds=config.openDataset(bucket,()=>check())
        try {
          if(key.isEmpty) {
            if(e.getRequestMethod=="HEAD")e.sendResponseHeaders(200,-1)
            else if(q.keySet==Set("location"))reply(e,200,"<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>")
            else list(e,ds,bucket,q)
          }else {
            if(q.nonEmpty || Seq("If-Match","If-None-Match","If-Modified-Since","If-Unmodified-Since").exists(e.getRequestHeaders.containsKey))throw Failure(501,"NotImplemented")
            val logical=LogicalPath.validate("/"+key)
            val item=ds.describe(logical)
            if(!config.plugins.views(item).contains("bytes"))throw Failure(404,"NoSuchKey")
            download(e,ds,item,audit)
          }
        }finally ds.close()
      }
      success=true
    }catch {
      case NonFatal(t)=>
        if(e.getResponseCode == -1) {
          val (status,code)=t match {
            case f:Failure=>(f.status,f.code)
            case _:DatasetNotFound=>(404,"NoSuchBucket")
            case e:org.apache.arrow.flight.FlightRuntimeException if e.status().code()==org.apache.arrow.flight.FlightStatusCode.UNAVAILABLE=>(503,"ServiceUnavailable")
            case _:java.io.FileNotFoundException=>(404,"NoSuchKey")
            case _:SecurityException | _:java.nio.file.AccessDeniedException=>(403,"AccessDenied")
            case _:IllegalArgumentException=>(400,"InvalidArgument")
            case _=>(500,"InternalError")
          }
          try reply(e,status,"<Error>"+tag("Code",code)+tag("Message",code)+"</Error>")catch{case NonFatal(_)=>()}
        }
    }finally {
      if(success)succeeded.increment()else failed.increment()
      elapsedNanos.add(System.nanoTime()-began)
      statusCounts.computeIfAbsent(e.getResponseCode.toString,_=>new java.util.concurrent.atomic.LongAdder()).increment()
      e.close();active.remove(e);audit.finish(if(success)"completed"else "failed")
    }
  }
  private def list(e:HttpExchange,ds:DatasetAccess,bucket:String,q:Map[String,String]):Unit={
    if(!q.keySet.subsetOf(Set("list-type","prefix","delimiter","max-keys","continuation-token","start-after","marker","encoding-type")))throw Failure(501,"NotImplemented")
    require(q.get("list-type").forall(_=="2"),"invalid list-type")
    require(q.get("encoding-type").forall(_=="url"),"invalid encoding-type")
    val v2=q.contains("list-type");require(v2 || !q.contains("continuation-token"),"invalid token")
    val prefix=q.getOrElse("prefix","");val delimiter=q.getOrElse("delimiter","")
    val max=q.get("max-keys").map(_.toInt).getOrElse(1000);require(max>=0 && max<=1000,"invalid max-keys")
    val context=bucket+"\n"+prefix+"\n"+delimiter+"\n"
    val after=q.get("continuation-token").map { t=>
      val decoded=new String(java.util.Base64.getUrlDecoder.decode(t),UTF_8)
      require(decoded.startsWith(context),"invalid continuation-token");decoded.substring(context.length)
    }.getOrElse(q.getOrElse(if(v2)"start-after"else "marker",""))
    def text(s:String)=if(q.contains("encoding-type"))encode(s)else s
    // DatasetAccess.list() is a recursive logical namespace snapshot, supplied by the plugin.
    val objects=ds.list().filter(i=>config.plugins.views(i).contains("bytes")).map { i=>LogicalPath.validate(i.path);i.path.stripPrefix("/")->i }.toMap
    val entries=objects.keysIterator.filter(_.startsWith(prefix)).map { key=>
      val at=if(delimiter.isEmpty)-1 else key.indexOf(delimiter,prefix.length)
      if(at>=0)key.take(at+delimiter.length)->true else key->false
    }.toVector.distinct.sortBy(_._1).filter(_._1>after)
    val page=entries.take(max);val truncated=max>0 && entries.size>max
    val body=new StringBuilder("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
    body.append(tag("Name",bucket)+tag("Prefix",text(prefix))+tag("MaxKeys",max.toString)+tag("IsTruncated",truncated.toString))
    if(delimiter.nonEmpty)body.append(tag("Delimiter",text(delimiter)))
    if(q.contains("encoding-type"))body.append(tag("EncodingType","url"))
    if(v2){body.append(tag("KeyCount",page.size.toString));q.get("continuation-token").foreach(t=>body.append(tag("ContinuationToken",t)));q.get("start-after").foreach(t=>body.append(tag("StartAfter",text(t))))}
    else body.append(tag("Marker",text(after)))
    page.foreach {case(key,folder)=>
      check()
      if(folder)body.append("<CommonPrefixes>"+tag("Prefix",text(key))+"</CommonPrefixes>")
      else {
        val item=objects(key)
        body.append("<Contents>"+tag("Key",text(key))+tag("LastModified",java.time.Instant.ofEpochMilli(item.lastModifiedTimeMs.getOrElse(0L)).toString))
        item.totalSizeBytes.foreach(n=>body.append(tag("Size",n.toString)))
        body.append(tag("StorageClass","STANDARD")+"</Contents>")
      }
    }
    if(truncated){val last=page.last._1;if(v2)body.append(tag("NextContinuationToken",java.util.Base64.getUrlEncoder.withoutPadding.encodeToString((context+last).getBytes(UTF_8))))else body.append(tag("NextMarker",text(last)))}
    reply(e,200,body.append("</ListBucketResult>").toString)
  }
  private def download(e:HttpExchange,ds:DatasetAccess,item:Item,audit:AccessEvent):Unit={
    val size=item.totalSizeBytes
    val range=Option(e.getRequestHeaders.getFirst("Range"))
    var offset=0L;var length=size
    range.foreach { value=>
      if(size.isEmpty)throw Failure(501,"NotImplemented")
      val pattern="bytes=([0-9]*)-([0-9]*)".r
      val n=size.get
      value match {
        case pattern(a,b) if a.nonEmpty || b.nonEmpty=>
          try {
            if(a.isEmpty){val suffix=b.toLong;if(suffix<=0)throw Failure(416,"InvalidRange");offset=math.max(0L,n-suffix);length=Some(n-offset)}
            else {offset=a.toLong;val end=if(b.isEmpty)n-1 else math.min(b.toLong,n-1);if(end<offset)throw Failure(416,"InvalidRange");length=Some(end-offset+1)}
          }catch{case _:NumberFormatException=>throw Failure(416,"InvalidRange")}
          if(offset>=n) {e.getResponseHeaders.set("Content-Range",s"bytes */$n");throw Failure(416,"InvalidRange")}
        case _=>throw Failure(416,"InvalidRange")
      }
    }
    val headers=e.getResponseHeaders
    headers.set("Content-Type","application/octet-stream")
    size.foreach(_=>headers.set("Accept-Ranges","bytes"))
    item.lastModifiedTimeMs.foreach(t=>headers.set("Last-Modified",java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.Instant.ofEpochMilli(t).atZone(java.time.ZoneOffset.UTC))))
    if(range.nonEmpty)headers.set("Content-Range",s"bytes $offset-${offset+length.get-1}/${size.get}")
    length.foreach(n=>headers.set("Content-Length",n.toString))
    val status=if(range.nonEmpty)206 else 200
    if(e.getRequestMethod=="HEAD" || length.contains(0L)){e.sendResponseHeaders(status,-1);return}
    var position=0L;var written=0L;var complete=false
    val stream=ds.requireView(item.path,DataViews.Bytes).open(AllBytes,StreamContext(allocator,()=>check()))
    try {
      try {
        stream.consume(new DataConsumer[ByteChunk] {
          def start(d:DataDescriptor[ByteChunk]):Unit=e.sendResponseHeaders(status,length.getOrElse(0L))
          def accept(b:ByteChunk):Unit={
            check()
            val skip=math.min(b.length.toLong,math.max(0L,offset-position)).toInt
            val take=math.min((b.length-skip).toLong,length.map(_-written).getOrElse(Long.MaxValue)).toInt
            if(take>0){e.getResponseBody.write(b.bytes,b.offset+skip,take);written+=take;downloadedBytes.add(take);audit.add(0,take)}
            position+=b.length
            if(range.nonEmpty && length.contains(written)){complete=true;throw RangeComplete}
          }
        })
        require(length.forall(_==written) && (range.nonEmpty || size.forall(_==position)),"incomplete object stream")
        complete=true
      } catch {case RangeComplete=>()}
    } finally stream.close()
    require(complete,"incomplete object stream")
  }
}
