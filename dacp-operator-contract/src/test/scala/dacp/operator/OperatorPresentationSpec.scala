package dacp.operator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.funsuite.AnyFunSuite

class OperatorPresentationSpec extends AnyFunSuite {
  val mapper=new ObjectMapper()
  def valid=mapper.readTree("""{"category":"Scientific data / 气象","icon":{"path":"M3 5h18v14H3zM3 10h18","color":"#14866b"}}""").asInstanceOf[ObjectNode]
  test("categories and portable icon paths are author-defined without a registry"){
    OperatorPresentation.validate(valid)
    val n=valid;n.put("category","My custom category");OperatorPresentation.validate(n)
  }
  test("missing metadata and active or externally loaded icons are rejected"){
    for(field<-Seq("category","icon")){val n=valid;n.remove(field);intercept[IllegalArgumentException](OperatorPresentation.validate(n))}
    for(category<-Seq("", " padded", "bad\nlabel", "x"*81)){val n=valid;n.put("category",category);intercept[IllegalArgumentException](OperatorPresentation.validate(n))}
    for(path<-Seq("https://example.com/a.svg","<svg onload='alert(1)'/>","M","MNaN 1", "M"+"1"*2048)){
      val n=valid;n.path("icon").asInstanceOf[ObjectNode].put("path",path);intercept[IllegalArgumentException](OperatorPresentation.validate(n))
    }
    val n=valid;n.path("icon").asInstanceOf[ObjectNode].put("color","red;display:none");intercept[IllegalArgumentException](OperatorPresentation.validate(n))
  }
}
