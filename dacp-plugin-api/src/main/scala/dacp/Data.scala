package dacp
import java.nio.file.{Files,Path}
import scala.jdk.CollectionConverters._
import com.fasterxml.jackson.databind.ObjectMapper
object Data {
  val mapper = new ObjectMapper()
  def deleteTree(p: Path): Unit = if (Files.exists(p)) { val s = Files.walk(p); try s.iterator.asScala.toSeq.reverse.foreach(Files.deleteIfExists) finally s.close() }
}
