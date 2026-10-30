package bench

import dacp.operator.ByteStreamOperator
import java.io.{InputStream,OutputStream}
import com.fasterxml.jackson.databind.JsonNode
/** ASCII case conversion, bounded memory independent of Item size. */
object Transform extends ByteStreamOperator {
  override def outputFormat:String="text"
  def transform(input:InputStream,output:OutputStream,params:JsonNode):Unit={
    val upper=params.path("upper").asBoolean
    val buffer=new Array[Byte](65536)
    var n=input.read(buffer)
    while(n != -1){
      var i=0
      while(i<n){val b=buffer(i);if(upper && b>=97 && b<=122)buffer(i)=(b-32).toByte else if(!upper && b>=65 && b<=90)buffer(i)=(b+32).toByte;i+=1}
      output.write(buffer,0,n);n=input.read(buffer)
    }
  }
}
