package example


import java.io.{InputStream,OutputStream}
import java.util.zip.GZIPOutputStream
import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.ByteStreamOperator

/** A standard Java streaming library handles bytes across chunk boundaries. */
object StreamGzip extends ByteStreamOperator {
  override def outputFormat:String="gzip"
  override def transform(input:InputStream,output:OutputStream,params:JsonNode):Unit={
    val gzip=new GZIPOutputStream(output)
    try input.transferTo(gzip)finally gzip.close()
  }
}
