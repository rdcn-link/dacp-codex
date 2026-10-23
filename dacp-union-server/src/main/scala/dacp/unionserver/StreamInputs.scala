package dacp.unionserver

import dacp._
import dacp.union.{Rpc,Json}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import scala.jdk.CollectionConverters._

/** Short-lived, pinned, single-consumer uploads used when this service is itself a worker. */
private[unionserver] final class StreamInputs {
  private case class Input(owner:String,sources:Map[String,FrameSource],headers:Map[String,com.fasterxml.jackson.databind.JsonNode],claimed:java.util.Set[String])
  private val inputs=new ConcurrentHashMap[String,Input]()
  def register(owner:String,sources:Map[String,FrameSource]):String={
    val headers=sources.map {case(p,s)=>val h=StreamProtocol.header(s.next().getOrElse(throw new IllegalArgumentException("missing input start")));require(h.path("type").asText=="start","missing input start");p->h}
    val id="input-"+Json.uuid();inputs.put(id,Input(owner,sources,headers,ConcurrentHashMap.newKeySet[String]()));id
  }
  def view(id:String,port:String):String=inputs.get(id).headers(port).path("representation").asText
  def remove(id:String):Unit={inputs.remove(id);()}
  def entries(owner:String):Seq[DatasetInfo]=inputs.entrySet.asScala.filter(_.getValue.owner==owner).map(e=>DatasetInfo(e.getKey)).toVector
  def contains(id:String):Boolean=inputs.containsKey(id)
  def open(id:String):PublishedDataset={
    val input=Option(inputs.get(id)).getOrElse(throw new java.io.FileNotFoundException())
    if(Rpc.identity!=input.owner)throw org.apache.arrow.flight.CallStatus.UNAUTHORIZED.withDescription("upload consumer denied").toRuntimeException
    new PublishedDataset {
      def open(check:()=>Unit):DatasetAccess=new DatasetAccess {
        def describe(path:String):Item={
          LogicalPath.validate(path);val h=input.headers.getOrElse(path.stripPrefix("/"),throw new java.io.FileNotFoundException())
          val view=h.path("representation").asText
          val schema=if(view=="bytes")PluginSchemas.bytes else StreamProtocol.schema(h)
          Item(path,h.path("format").asText,Some(schema),views=Some(Seq(view)))
        }
        def list():Seq[Item]=input.headers.keys.toSeq.sorted.map(p=>describe("/"+p))
        def list(path:String):Seq[Item]={require(path=="/");list()}
        def count(path:String):Option[Long]=None
        def stream(path:String,view:String,a:BufferAllocator,out:ServerStreamListener,ready:()=>Unit):Unit={
          val item=describe(path);require(item.views.exists(_.contains(view)),"unsupported upload view")
          val port=path.stripPrefix("/");require(input.claimed.add(port),"upload already consumed")
          val root=VectorSchemaRoot.create(item.schema.get,a)
          try {
            root.allocateNew();out.start(root)
            var ended=false
            while(!ended){check();ready();val f=input.sources(port).next().getOrElse(throw new IllegalArgumentException("upload truncated"));val h=StreamProtocol.header(f)
              h.path("type").asText match {
                case "data"=>
                  if(view=="bytes"){root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,f.data);root.setRowCount(1)}
                  else {
                    val reader=new ArrowStreamReader(new java.io.ByteArrayInputStream(f.data),a)
                    try {require(reader.loadNextBatch() && reader.getVectorSchemaRoot.getSchema==root.getSchema,"invalid upload records");root.clear();root.allocateNew();val source=reader.getVectorSchemaRoot;root.getFieldVectors.asScala.zip(source.getFieldVectors.asScala).foreach{case(to,from)=>(0 until source.getRowCount).foreach(i=>to.copyFromSafe(i,i,from))};root.setRowCount(source.getRowCount)}finally reader.close()
                  }
                  check();ready();out.putNext()
                case "end"=>ended=true
                case _=>throw new IllegalArgumentException("invalid upload frame")
              }
            }
            // The owning UnionWorker validates upstream completion after this runner returns.
            out.completed()
          }finally root.close()
        }
      }
    }
  }
}
