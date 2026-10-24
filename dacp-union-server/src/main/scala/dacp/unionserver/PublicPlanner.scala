package dacp.unionserver

import dacp._
import dacp.union.{Json,Rpc}
import scala.jdk.CollectionConverters._

private[unionserver] final class PublicPlanner(members:Members,settings:Settings,catalog:ProxyCatalog,routes:DatasetRoutes) {
  private def origin=settings.publicUrl.stripSuffix("/")
  def logicalNode(member:String):String="nd-"+routes.assign(member,"").stripPrefix("ds-")
  def nodes:com.fasterxml.jackson.databind.JsonNode={
    val n=Json.obj();val nodes=n.putArray("nodes")
    members.all.foreach { m=>val status=members.inspect(m);if(status.path("state").asText=="approved")nodes.addObject().put("id",logicalNode(m.path("identity").asText)).put("capacity",status.path("status").path("capacity").asInt) };n
  }
  def plan(input:JobPlan,check:()=>Unit):JobPlan={
    val candidates=members.all.map(m=>members.inspect(m)).filter(_.path("state").asText=="approved")
    val placements=scala.collection.mutable.Map.empty[String,String]
    val used=scala.collection.mutable.Map.empty[String,Int].withDefaultValue(0)
    val readSizes=scala.collection.mutable.Map.empty[String,(String,Option[Long])]
    val tasks=input.tasks.map { t=>
      require(t.source.isEmpty,"Union requires versioned operators")
      val sources=Jobs.readBinding(t).toVector.flatMap { b=>b.uri.map { uri=>
        val port="result"
        require(DacpUri.origin(uri)==origin,"input must use the Union public URI")
        val parts=DacpUri.parse(uri).getPath.stripPrefix("/").split("/",2)
        require(parts.length==2,"input must address an Item")
        val(m,local)=catalog.target(parts(0),check)
        val published=catalog.open(parts(0),check);val access=published.open(check)
        val size=try access.describe("/"+parts(1)).totalSizeBytes finally {access.close();published.close()}
        (port,m.path("identity").asText,new java.net.URI("dacp",null,java.net.URI.create(m.path("data_origin").asText).getHost,java.net.URI.create(m.path("data_origin").asText).getPort,"/"+local+"/"+parts(1),null,null).toASCIIString,size)
      }}
      sources.headOption.foreach(s=>readSizes(t.id)=s._2->s._4)
      val rawInputs=t.inputs.values.toVector.flatMap(_.task.flatMap(readSizes.get))
      val preference=(sources.map(_._2)++Jobs.dependencies(t).flatMap(placements.get)).distinct
      val eligible=candidates.filter { m=>
        val id=m.path("identity").asText;val status=m.path("status");val grant=m.path("grant")
        val capacity=math.min(status.path("capacity").asInt-status.path("reserved_tasks").asInt,status.path("union_capacity").asInt-status.path("union_reserved_tasks").asInt)
        (t.operator==Jobs.ReadItem || grant.path("operators").elements.asScala.exists(_.asText==t.operator)) && (sources.isEmpty || sources.forall(_._2==id)) && capacity>used(id)
      }
      val selected=t.node match {
        case Some(id)=>eligible.find(m=>logicalNode(m.path("identity").asText)==id).getOrElse(throw new IllegalArgumentException("requested logical node unavailable"))
        case None=>
          val local=eligible.filter(m=>preference.contains(m.path("identity").asText))
          val pool=if(local.nonEmpty)local else eligible
          require(pool.nonEmpty,"no eligible execution node")
          if(rawInputs.nonEmpty && rawInputs.size==t.inputs.size && rawInputs.forall(_._2.isDefined))
            pool.minBy(m=>(rawInputs.filter(_._1!=m.path("identity").asText).map(s=>BigInt(s._2.get)).sum,m.path("identity").asText))
          else preference.iterator.flatMap(id=>pool.find(_.path("identity").asText==id)).take(1).toVector.headOption.getOrElse(pool.sortBy(_.path("identity").asText).head)
      }
      val id=selected.path("identity").asText;used(id)+=1;placements(t.id)=id
      val params=new java.util.LinkedHashMap[String,Object](t.params)
      sources.headOption.foreach(s=>params.put("uri",s._3))
      t.copy(node=Some(selected.path("data_origin").asText),params=params)
    }
    JobPlan(tasks,input.outputs)
  }
}
