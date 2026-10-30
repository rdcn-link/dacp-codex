package xstream.designer


import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Optional presentation metadata; execution engines deliberately ignore it. */
object DesignerLayout {
  val name = "XStream Designer"
  val softwareVersion: String = {
    val stream = getClass.getResourceAsStream("/designer-version.txt")
    require(stream != null, "missing designer build version")
    try scala.io.Source.fromInputStream(stream, "UTF-8").mkString.trim finally stream.close()
  }
  def stamp(job:JsonNode):Unit={
    val root=job.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    val layout=Option(root.get("designer")).map(_.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]).getOrElse(root.putObject("designer").put("version",1))
    layout.putObject("generator").put("name",name).put("version",softwareVersion)
    layout.put("exported_at",java.time.Instant.now().toString)
  }

  def validate(n:JsonNode):Unit={
    def fields(value:JsonNode,allowed:Set[String]):Unit=require(value.isObject && value.fieldNames.asScala.toSet.subsetOf(allowed),"invalid designer layout fields")
    def number(value:JsonNode,min:Double,max:Double):Unit=require(value.isNumber && java.lang.Double.isFinite(value.asDouble) && value.asDouble>=min && value.asDouble<=max,"invalid designer layout coordinate")
    def point(value:JsonNode):Unit={fields(value,Set("x","y"));number(value.path("x"),0,100000);number(value.path("y"),0,100000)}
    fields(n,Set("version","positions","outlet","viewport","generator","exported_at"))
    require(n.path("version").isIntegralNumber && n.path("version").asInt==1,"unsupported designer layout version")
    Option(n.get("generator")).foreach { g=>
      fields(g,Set("name","version"))
      Seq("name","version").foreach(k=>require(g.path(k).isTextual && g.path(k).asText.nonEmpty && g.path(k).asText.length<=128,"invalid designer generator"))
    }
    Option(n.get("exported_at")).foreach { t=>
      require(t.isTextual,"invalid designer export time")
      try java.time.Instant.parse(t.asText) catch {case _:java.time.format.DateTimeParseException=>throw new IllegalArgumentException("invalid designer export time")}
    }
    def points(key:String,pattern:String):Unit=Option(n.get(key)).foreach { values=>
      require(values.isObject && values.size<=128,"invalid designer layout mapping")
      values.fields.asScala.foreach{e=>require(e.getKey.matches(pattern),"invalid designer layout element");point(e.getValue)}
    }
    points("positions","[A-Za-z_][A-Za-z0-9_-]{0,127}")
    Option(n.get("outlet")).foreach(point)
    Option(n.get("viewport")).foreach { v=>
      fields(v,Set("zoom","scroll_left","scroll_top"))
      number(v.path("zoom"),0.35,1.6);number(v.path("scroll_left"),0,200000);number(v.path("scroll_top"),0,200000)
    }
  }
}
