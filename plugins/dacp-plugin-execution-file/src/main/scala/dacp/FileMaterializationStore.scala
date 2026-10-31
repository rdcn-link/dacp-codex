package dacp

import _root_.xstream.data._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.{ArrowFileReader,ArrowFileWriter,ArrowStreamReader,ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.Schema
import java.io.{ByteArrayInputStream,ByteArrayOutputStream}
import java.nio.channels.WritableByteChannel
import java.nio.ByteBuffer
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.{Files,Path,StandardCopyOption,StandardOpenOption}
import java.nio.channels.FileChannel
import java.security.MessageDigest
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class FileMaterializationStoreProvider extends MaterializationStoreProvider {
  val id="file"
  def open(settings:Map[String,Object]):MaterializationStore={
    require(settings.keySet.subsetOf(Set("directory","max_bytes","max_task_bytes","retention_seconds")) && settings.contains("directory"),"invalid file materialization settings")
    def positive(k:String,default:Long):Long={val v=settings.get(k).map(_.toString.toLong).getOrElse(default);require(v>0,s"$k must be positive");v}
    new FileMaterializationStore(Path.of(settings("directory").toString),positive("max_bytes",1073741824L),positive("max_task_bytes",268435456L),positive("retention_seconds",86400))
  }
}
final class FileMaterializationStore(directory:Path,maxBytes:Long,maxTaskBytes:Long,retentionSeconds:Long) extends MaterializationStore {
  require(maxBytes>0 && maxTaskBytes>0 && retentionSeconds>0 && retentionSeconds<=Long.MaxValue/1000)
  Files.createDirectories(directory)
  private val channel=FileChannel.open(directory.resolve(".writer.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)
  private val lock=try Option(channel.tryLock()).getOrElse(throw new IllegalStateException("materialization directory already in use"))catch{case t:Throwable=>channel.close();throw t}
  private var closed=false
  private val leases=mutable.Map.empty[Path,Int]
  private val writers=mutable.Set.empty[MaterializationWriter]
  private def listing:Vector[Path]={val s=Files.list(directory);try s.iterator.asScala.toVector finally s.close()}
  private def size(p:Path):Long={val s=Files.walk(p);try s.iterator.asScala.filter(Files.isRegularFile(_)).map(Files.size(_)).sum finally s.close()}
  private def delete(p:Path):Unit={val s=Files.walk(p);try s.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally s.close()}
  listing.filter(_.getFileName.toString.startsWith(".pending-")).foreach(delete)
  private var used=listing.filter(Files.isDirectory(_)).map(size).sum
  private def path(key:String):Path={require(key.matches("[A-Za-z0-9_-]{1,256}"),"invalid materialization key");directory.resolve(key)}
  private def digest(bytes:Array[Byte]):String=bytes.map(b=>f"${b & 255}%02x").mkString
  private def fileDigest(path:Path):String={
    val hash=MessageDigest.getInstance("SHA-256");val in=Files.newInputStream(path);val buffer=new Array[Byte](65536)
    try {var n=in.read(buffer);while(n>=0){if(n>0)hash.update(buffer,0,n);n=in.read(buffer)}}finally in.close()
    digest(hash.digest())
  }
  def cleanup():Unit=synchronized {
    require(!closed,"materialization store closed")
    listing.filter(p=>Files.isDirectory(p) && !p.getFileName.toString.startsWith(".") && !leases.contains(p)).foreach { p=>
      val m=Data.mapper.readTree(p.resolve("manifest.json").toFile)
      if(m.path("expires_at_ms").asLong<=System.currentTimeMillis()){val bytes=size(p);delete(p);used-=bytes}
    }
  }
  def begin(key:String,metadata:JsonNode,ports:Set[String]):MaterializationWriter=synchronized {
    cleanup();val target=path(key);require(!Files.exists(target),"materialized task already exists")
    require(metadata.isObject && ports.nonEmpty && ports.forall(_.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}")),"invalid materialization ports")
    val staging=Files.createTempDirectory(directory,".pending-")
    val outputs=mutable.Map.empty[String,ObjectNode]
    var bytes=0L;var done=false;var failed=false
    def reserve(count:Long):Unit={
      require(count>=0 && count<=maxTaskBytes-bytes && count<=maxBytes-used,"materialization quota exceeded")
    }
    def segment(port:String,root:VectorSchemaRoot,dictionaries:DictionaryProvider,batch:Boolean):Unit={
      val ps=outputs(port);val segments=ps.withArray("segments")
      val name=port+"-"+f"${segments.size()}%08d"+".arrow"
      val file=staging.resolve(name)
      val channel=FileChannel.open(file,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)
      val limited=new WritableByteChannel {
        def isOpen:Boolean=channel.isOpen
        def close():Unit=()
        def write(b:ByteBuffer):Int={reserve(b.remaining().toLong);val n=channel.write(b);bytes+=n;used+=n;n}
      }
      val writer=new ArrowFileWriter(root,dictionaries,limited)
      try {writer.start();if(batch)writer.writeBatch();writer.end();writer.close();channel.force(true)}
      finally {try writer.close()finally channel.close()}
      segments.addObject().put("file",name).put("bytes",Files.size(file)).put("sha256",fileDigest(file)).put("batches",if(batch)1 else 0)
    }
    val writer=new MaterializationWriter {
      def append(port:String,frame:OperatorFrame):Unit=FileMaterializationStore.this.synchronized {
        require(!closed && !done && !failed && ports(port) && frame.metadata.length<=OperatorFrames.maxHeader && frame.data.length<=OperatorFrames.maxData,"invalid materialization frame")
        try {
        val h=Data.mapper.readTree(frame.metadata)
        require(h.isObject && h.path("port").asText==port,"invalid materialization port")
        val allocator=new RootAllocator(64L*1024*1024)
        try h.path("type").asText match {
          case "start"=>
            require(!outputs.contains(port),"duplicate materialization start")
            val view=h.path("representation").asText;require(view.nonEmpty,"missing output view")
            val original=if(view=="bytes")PluginSchemas.bytes else Schema.deserializeMessage(ByteBuffer.wrap(java.util.Base64.getDecoder.decode(h.path("schema").asText)))
            val descriptor=if(view=="bytes")Data.mapper.createObjectNode().put("media_type","application/octet-stream")
              else if(view=="records")Data.mapper.createObjectNode() else h.path("structure")
            val schema=ArrowStreamMetadata.schema(original,view,descriptor)
            val state=Data.mapper.createObjectNode();state.set[JsonNode]("start",h);state.put("arrow_schema",java.util.Base64.getEncoder.encodeToString(schema.serializeAsMessage()));state.putArray("segments")
            outputs(port)=state
            if(frame.data.nonEmpty){
              val reader=new ArrowStreamReader(new ByteArrayInputStream(frame.data),allocator)
              try {
                require(reader.getVectorSchemaRoot.getSchema==original,"start IPC schema mismatch")
                require(!reader.loadNextBatch(),"start event contains business data")
                val root=new VectorSchemaRoot(schema,reader.getVectorSchemaRoot.getFieldVectors,0)
                segment(port,root,reader,batch=true)
              }finally reader.close()
            }else {
              val root=VectorSchemaRoot.create(schema,allocator)
              try segment(port,root,null,batch=true)finally root.close()
            }
          case "data"=>
            require(outputs.contains(port) && !outputs(port).has("end"),"invalid materialization data")
            val state=outputs(port);val schema=Schema.deserializeMessage(ByteBuffer.wrap(java.util.Base64.getDecoder.decode(state.path("arrow_schema").asText)))
            if(state.path("start").path("representation").asText=="bytes"){
              val root=VectorSchemaRoot.create(schema,allocator)
              try {root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,frame.data);root.setRowCount(1);segment(port,root,null,batch=true)}finally root.close()
            }else {
              val reader=new ArrowStreamReader(new ByteArrayInputStream(frame.data),allocator)
              try {
                val original=Schema.deserializeMessage(ByteBuffer.wrap(java.util.Base64.getDecoder.decode(state.path("start").path("schema").asText)))
                require(reader.getVectorSchemaRoot.getSchema==original && reader.loadNextBatch(),"invalid materialization batch")
                val source=reader.getVectorSchemaRoot
                segment(port,new VectorSchemaRoot(schema,source.getFieldVectors,source.getRowCount),reader,batch=true)
                require(!reader.loadNextBatch(),"expected one Arrow batch per data event")
              }finally reader.close()
            }
          case "end"=>
            require(outputs.contains(port) && !outputs(port).has("end") && frame.data.isEmpty,"invalid materialization end")
            outputs(port).set[JsonNode]("end",h)
          case _=>throw new IllegalArgumentException("control events cannot be materialized as data")
        } finally allocator.close()
        } catch {case t:Throwable=>failed=true;throw t}
      }
      def commit():Unit=FileMaterializationStore.this.synchronized {
        require(!closed && !done && !failed && outputs.keySet==ports && outputs.values.forall(_.has("end")),"incomplete materialization")
        val m=metadata.deepCopy[ObjectNode]();val ps=m.putObject("stored_ports")
        ports.toVector.sorted.foreach(p=>ps.set[JsonNode](p,outputs(p)))
        m.put("storage_format","arrow-ipc-segments-v1").put("complete",true)
        m.put("expires_at_ms",Math.addExact(System.currentTimeMillis(),Math.multiplyExact(retentionSeconds,1000L)))
        val json=Data.mapper.writeValueAsBytes(m);reserve(json.length.toLong)
        Files.write(staging.resolve("manifest.json"),json);used+=json.length;bytes+=json.length
        val c=FileChannel.open(staging.resolve("manifest.json"),StandardOpenOption.WRITE);try c.force(true)finally c.close()
        val sc=FileChannel.open(staging,StandardOpenOption.READ);try sc.force(true)finally sc.close()
        Files.move(staging,target,StandardCopyOption.ATOMIC_MOVE)
        done=true;writers-=this
        val parent=FileChannel.open(directory,StandardOpenOption.READ);try parent.force(true)finally parent.close()
      }
      def close():Unit=FileMaterializationStore.this.synchronized {if(!done){done=true;try {if(Files.exists(staging))delete(staging)}finally {used-=bytes;writers-=this}}}
    }
    writers+=writer;writer
  }
  def get(key:String):Option[MaterializedTask]=synchronized {
    cleanup();val p=path(key)
    if(!Files.isDirectory(p))return None
    val m=Data.mapper.readTree(p.resolve("manifest.json").toFile)
    if(m.path("expires_at_ms").asLong<=System.currentTimeMillis())return None
    leases(p)=leases.getOrElse(p,0)+1
    Some(new MaterializedTask {
      private var released=false
      private val readers=mutable.Set.empty[MaterializedFrames]
      def metadata:JsonNode=m.deepCopy[JsonNode]()
      def open(port:String):MaterializedFrames=FileMaterializationStore.this.synchronized {
        require(!closed && !released && m.path("stored_ports").has(port),"materialized port unavailable")
        val expected=m.path("stored_ports").path(port)
        val segments=expected.path("segments").elements.asScala.toVector
        require(m.path("storage_format").asText=="arrow-ipc-segments-v1" && m.path("complete").asBoolean && segments.nonEmpty,"incomplete materialization manifest")
        require(segments.zipWithIndex.forall {case(n,i)=>n.path("file").asText==port+"-"+f"$i%08d"+".arrow" && n.path("batches").asInt()==1},"invalid Arrow segment order")
        val schema=Schema.deserializeMessage(ByteBuffer.wrap(java.util.Base64.getDecoder.decode(expected.path("arrow_schema").asText)))
        val start=expected.path("start");val bytesView=start.path("representation").asText=="bytes"
        val original=if(bytesView)PluginSchemas.bytes else Schema.deserializeMessage(ByteBuffer.wrap(java.util.Base64.getDecoder.decode(start.path("schema").asText)))
        var index=0;var phase=0;var ended=false
        def readSegment[A](n:JsonNode)(read:ArrowFileReader=>A):A={
          val name=n.path("file").asText
          require(name.matches(java.util.regex.Pattern.quote(port)+"-[0-9]{8}\\.arrow"),"invalid segment name")
          val file=p.resolve(name)
          require(Files.isRegularFile(file) && Files.size(file)==n.path("bytes").asLong && fileDigest(file)==n.path("sha256").asText,"materialized output integrity failure")
          val allocator=new RootAllocator(64L*1024*1024)
          try {
            val in=new ArrowFileReader(FileChannel.open(file,StandardOpenOption.READ),allocator)
            try {require(in.getVectorSchemaRoot.getSchema==schema,"materialized schema changed");read(in)}finally in.close()
          }finally allocator.close()
        }
        def ipc(in:ArrowFileReader,batch:Boolean):Array[Byte]={
          if(!batch)return ArrowIpc.header(original,in,in.lookup(in.getDictionaryIds.iterator().next()).getVector.getAllocator)
          val source=in.getVectorSchemaRoot
          val root=new VectorSchemaRoot(original,source.getFieldVectors,source.getRowCount)
          val out=new ByteArrayOutputStream();val writer=new ArrowStreamWriter(root,in,out)
          try {writer.start();if(batch)writer.writeBatch();writer.end();out.toByteArray}finally writer.close()
        }
        val reader=new MaterializedFrames {
          def next():Option[OperatorFrame]=FileMaterializationStore.this.synchronized {
            require(!closed && !ended,"materialized reader closed")
            if(phase==0){
              phase=1;index=1
              val payload=readSegment(segments.head){in=>
                require(in.loadNextBatch() && in.getVectorSchemaRoot.getRowCount==0,"invalid initial Arrow segment")
                val result=if(!in.getDictionaryIds.isEmpty)ipc(in,batch=false)else Array.emptyByteArray
                require(!in.loadNextBatch(),"unexpected initial Arrow batch");result
              }
              Some(OperatorFrame(Data.mapper.writeValueAsBytes(start),payload))
            }else if(index<segments.size){
              val n=segments(index);index+=1
              val data=readSegment(n){in=>
                require(in.loadNextBatch(),"missing materialized batch")
                val root=in.getVectorSchemaRoot
                val result=if(bytesView){require(root.getRowCount==1,"invalid byte segment");root.getVector(0).asInstanceOf[VarBinaryVector].get(0)}else ipc(in,batch=true)
                require(!in.loadNextBatch(),"unexpected materialized batch");result
              }
              val header=Data.mapper.createObjectNode().put("type","data").put("port",port)
              Some(OperatorFrame(Data.mapper.writeValueAsBytes(header),data))
            }else if(phase==1){phase=2;Some(OperatorFrame(Data.mapper.writeValueAsBytes(expected.path("end"))))}
            else {close();None}
          }
          def close():Unit=FileMaterializationStore.this.synchronized {ended=true}
        }
        readers+=reader;reader
      }
      def close():Unit=FileMaterializationStore.this.synchronized {if(!released){released=true;readers.foreach(_.close());val n=leases(p)-1;if(n==0)leases-=p else leases(p)=n}}
    })
  }
  def close():Unit=synchronized {if(!closed){writers.toVector.foreach(_.close());closed=true;try lock.release()finally channel.close()}}
}
