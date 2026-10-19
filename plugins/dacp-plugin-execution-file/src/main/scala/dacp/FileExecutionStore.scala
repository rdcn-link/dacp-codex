package dacp
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files,Path,StandardOpenOption,StandardCopyOption}
import java.nio.channels.FileChannel
import scala.jdk.CollectionConverters._

class FileExecutionStoreProvider extends ExecutionStoreProvider {
  val id="file"
  def open(settings:Map[String,Object],readOnly:Boolean):ExecutionStore={
    require(settings.keySet==Set("directory"),"file execution store requires directory")
    new FileExecutionStore(Path.of(settings("directory").toString),readOnly)
  }
}
final class FileExecutionStore(directory:Path,readOnly:Boolean) extends ExecutionStore {
  Files.createDirectories(directory)
  private var closed=false
  private val channel=if(!readOnly)Some(FileChannel.open(directory.resolve(".writer.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE))else None
  private val lock=channel.map(c=>try Option(c.tryLock()).getOrElse(throw new IllegalStateException("execution history directory is already in use"))catch{case t:Throwable=>c.close();throw t})
  private def file(id:String):Path={require(id.matches("[A-Za-z0-9_-]+"),"invalid record id");directory.resolve(id+".json")}
  def get(id:String):Option[JsonNode]=synchronized {require(!closed,"store closed");val p=file(id);if(Files.exists(p))Some(Data.mapper.readTree(p.toFile))else None}
  def records:Vector[JsonNode]=synchronized {
    require(!closed,"store closed");val stream=Files.list(directory)
    try stream.iterator.asScala.filter(_.getFileName.toString.matches("[A-Za-z0-9_-]+\\.json")).map(p=>Data.mapper.readTree(p.toFile)).toVector finally stream.close()
  }
  def create(record:JsonNode):Unit=synchronized {require(get(record.path("record_id").asText).isEmpty,"execution record already exists");put(record)}
  def put(record:JsonNode):Unit=synchronized {
    require(!closed && !readOnly,"store closed or read-only")
    val interrupted=Thread.interrupted()
    try {
      val target=file(record.path("record_id").asText);val temp=Files.createTempFile(directory,".record-",".tmp")
      try {
        val out=FileChannel.open(temp,StandardOpenOption.WRITE)
        try {val b=java.nio.ByteBuffer.wrap(Data.mapper.writeValueAsBytes(record));while(b.hasRemaining)out.write(b);out.force(true)}finally out.close()
        Files.move(temp,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING)
        val parent=FileChannel.open(directory,StandardOpenOption.READ);try parent.force(true)finally parent.close()
      }finally Files.deleteIfExists(temp)
    }finally if(interrupted)Thread.currentThread().interrupt()
  }
  def close():Unit=synchronized {if(!closed){closed=true;try lock.foreach(l=>if(l.isValid)l.release())finally channel.foreach(_.close())}}
}
