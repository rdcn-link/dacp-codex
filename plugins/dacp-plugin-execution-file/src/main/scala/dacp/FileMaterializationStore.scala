package dacp

import _root_.xstream.data._

import com.fasterxml.jackson.databind.JsonNode
import java.io.{DataInputStream,DataOutputStream,BufferedInputStream,BufferedOutputStream,EOFException}
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
    val outputs=mutable.Map.empty[String,DataOutputStream];val hashes=ports.map(p=>p->MessageDigest.getInstance("SHA-256")).toMap
    var bytes=0L;var done=false
    val writer=new MaterializationWriter {
      def append(port:String,chunk:Array[Byte]):Unit=FileMaterializationStore.this.synchronized {
        require(!closed && !done && ports(port) && chunk.length<=9*1024*1024,"invalid materialization chunk")
        val count=chunk.length.toLong+4;require(bytes+count<=maxTaskBytes && used+count<=maxBytes,"materialization quota exceeded")
        val out=outputs.getOrElseUpdate(port,new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(staging.resolve(port+".bin")))))
        used+=count;bytes+=count;out.writeInt(chunk.length);out.write(chunk);hashes(port).update(chunk)
      }
      def commit():Unit=FileMaterializationStore.this.synchronized {
        require(!closed && !done && outputs.keySet==ports,"incomplete materialization")
        outputs.values.foreach(_.close())
        val m=metadata.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]();val ps=m.putObject("stored_ports")
        ports.toVector.sorted.foreach { p=>
          val f=staging.resolve(p+".bin");val c=FileChannel.open(f,StandardOpenOption.WRITE);try c.force(true)finally c.close()
          ps.putObject(p).put("sha256",digest(hashes(p).digest())).put("bytes",Files.size(f))
        }
        m.put("expires_at_ms",Math.addExact(System.currentTimeMillis(),Math.multiplyExact(retentionSeconds,1000L)))
        val json=Data.mapper.writeValueAsBytes(m);require(bytes+json.length<=maxTaskBytes && used+json.length<=maxBytes,"materialization quota exceeded")
        Files.write(staging.resolve("manifest.json"),json);used+=json.length;bytes+=json.length
        val c=FileChannel.open(staging.resolve("manifest.json"),StandardOpenOption.WRITE);try c.force(true)finally c.close()
        val sc=FileChannel.open(staging,StandardOpenOption.READ);try sc.force(true)finally sc.close()
        Files.move(staging,target,StandardCopyOption.ATOMIC_MOVE)
        done=true;writers-=this
        val parent=FileChannel.open(directory,StandardOpenOption.READ);try parent.force(true)finally parent.close()
      }
      def close():Unit=FileMaterializationStore.this.synchronized {if(!done){done=true;try outputs.values.foreach(_.close())finally {if(Files.exists(staging))delete(staging);used-=bytes;writers-=this}}}
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
      private val readers=mutable.Set.empty[MaterializedChunks]
      def metadata:JsonNode=m.deepCopy[JsonNode]()
      def open(port:String):MaterializedChunks=FileMaterializationStore.this.synchronized {
        require(!closed && !released && m.path("stored_ports").has(port),"materialized port unavailable")
        val in=new DataInputStream(new BufferedInputStream(Files.newInputStream(p.resolve(port+".bin"))))
        val hash=MessageDigest.getInstance("SHA-256");var read=0L;var ended=false
        val reader=new MaterializedChunks {
          def next():Option[Array[Byte]]=synchronized {
            require(!ended,"materialized reader closed")
            val expected=m.path("stored_ports").path(port)
            if(read==expected.path("bytes").asLong){require(in.read()== -1 && digest(hash.digest())==expected.path("sha256").asText,"materialized output integrity failure");close();None}
            else {val count=in.readInt();require(count>=0 && count<=9*1024*1024 && read+4+count<=expected.path("bytes").asLong,"corrupt materialized chunk");val b=new Array[Byte](count);in.readFully(b);read+=count+4;hash.update(b);Some(b)}
          }
          def close():Unit=synchronized {if(!ended){ended=true;in.close()}}
        }
        readers+=reader;reader
      }
      def close():Unit=FileMaterializationStore.this.synchronized {if(!released){released=true;readers.foreach(_.close());val n=leases(p)-1;if(n==0)leases-=p else leases(p)=n}}
    })
  }
  def close():Unit=synchronized {if(!closed){writers.toVector.foreach(_.close());closed=true;try lock.release()finally channel.close()}}
}
