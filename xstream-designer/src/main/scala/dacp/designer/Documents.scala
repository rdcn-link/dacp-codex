package xstream.designer


import com.fasterxml.jackson.databind.JsonNode
import dacp.union.{Json,YamlDocument}
import scala.jdk.CollectionConverters._

/** Authoring checks only: never executes a job or invents output schemas. */
object Documents {
  def mapping(text:String):JsonNode={val n=YamlDocument.parse(text);require(n.isObject,"Expected a YAML mapping");n}
  def job(text:String,allowEmpty:Boolean=false):JsonNode={
    val n=mapping(text)
    fields(n,Set("name","tasks","outputs","designer"))
    Option(n.get("designer")).foreach(DesignerLayout.validate)
    require(n.path("name").isTextual && n.path("name").asText.trim.nonEmpty && n.path("name").asText.length<=256 && !n.path("name").asText.exists(_.isControl),"Job name must contain 1–256 readable characters")
    require(n.path("tasks").isArray && (allowEmpty || n.path("tasks").size()>0) && n.path("tasks").size()<=64,"Expected 1–64 steps")
    require(n.path("outputs").isObject,"Expected outputs mapping")
    require(n.path("outputs").size()<=1,"Outlet accepts only one step output port")
    val tasks=n.path("tasks").elements.asScala.toVector
    val ids=tasks.map(t=>t.path("id").asText)
    require(ids.distinct.size==ids.size && ids.forall(_.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}")),"Step IDs must be unique identifiers")
    val dependencies=scala.collection.mutable.Map.empty[String,Vector[String]]
    val consumed=scala.collection.mutable.Set.empty[(String,String)]
    def binding(value:JsonNode):String={
      require(value.isTextual,"Connections must use task[:port]")
      val parts=value.asText.split(":",-1)
      require(parts.length<=2 && parts.forall(_.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}")) && ids.contains(parts(0)),"Unknown step or invalid connection: "+value.asText)
      val key=parts(0)->(if(parts.length==2)parts(1)else "result")
      require(consumed.add(key),"Output already has a consumer: "+value.asText);parts(0)
    }
    tasks.foreach{t=>
      fields(t,Set("id","title","operator","inputs","params"))
      require(t.path("operator").isTextual && t.path("operator").asText.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*@[A-Za-z0-9_-][A-Za-z0-9_.-]*"),"Expected a versioned operator")
      require(t.path("inputs").isObject,"Step inputs must be a mapping")
      require(!t.has("params") || t.path("params").isObject,"Step params must be a mapping")
      require(!t.has("title") || t.path("title").isTextual,"Step title must be text")
      t.path("inputs").fieldNames.asScala.foreach(p=>require(p.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"Invalid input port"))
      dependencies(t.path("id").asText)=t.path("inputs").elements.asScala.map(binding).toVector
      if(t.path("operator").asText=="read_item@1.0")require(t.path("inputs").size()==0,"read_item has no inputs")
    }
    n.path("outputs").fieldNames.asScala.foreach(p=>require(p.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}"),"Invalid output port"))
    n.path("outputs").elements.asScala.foreach(binding)
    val visiting=scala.collection.mutable.Set.empty[String];val done=scala.collection.mutable.Set.empty[String]
    def visit(id:String):Unit=if(!done(id)){require(!visiting(id),"Workflow contains a cycle");visiting+=id;dependencies(id).foreach(visit);visiting-=id;done+=id}
    ids.foreach(visit);n
  }
  private def fields(n:JsonNode,allowed:Set[String]):Unit={require(n.isObject,"Expected a mapping");require(n.fieldNames.asScala.toSet.subsetOf(allowed),"Unsupported fields: "+n.fieldNames.asScala.filterNot(allowed).mkString(", "))}
  def variables(text:String):JsonNode=if(text.trim.isEmpty)Json.obj()else mapping(text)
}
