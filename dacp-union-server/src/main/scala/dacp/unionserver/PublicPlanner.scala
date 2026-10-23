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
    require(input.outputs.nonEmpty,"explicit outputs required")
    val candidates=members.all.map(m=>members.inspect(m)).filter(_.path("state").asText=="approved")
    val placements=scala.collection.mutable.Map.empty[String,String]
    val used=scala.collection.mutable.Map.empty[String,Int].withDefaultValue(0)
    val tasks=input.tasks.map { t=>
      require(t.source.isEmpty,"Union requires versioned operators")
      val sources=t.inputs.toVector.flatMap {case(port,b)=>b.uri.map { uri=>
        require(DacpUri.origin(uri)==origin,"input must use the Union public URI")
        val parts=DacpUri.parse(uri).getPath.stripPrefix("/").split("/",2)
        require(parts.length==2,"input must address an Item")
        val(m,local)=catalog.target(parts(0),check)
        val published=catalog.open(parts(0),check);val access=published.open(check)
        val size=try access.describe("/"+parts(1)).totalSizeBytes finally {access.close();published.close()}
        (port,m.path("identity").asText,new java.net.URI("dacp",null,java.net.URI.create(m.path("data_origin").asText).getHost,java.net.URI.create(m.path("data_origin").asText).getPort,"/"+local+"/"+parts(1),null,null).toASCIIString,size)
      }}
      val preference=t.inputs.toVector.flatMap {case(p,b)=>sources.find(_._1==p).map(_._2).orElse(b.task.flatMap(placements.get))}.distinct
      val eligible=candidates.filter { m=>
        val id=m.path("identity").asText;val status=m.path("status");val grant=m.path("grant")
        val capacity=math.min(status.path("capacity").asInt-status.path("reserved_tasks").asInt,status.path("union_capacity").asInt-status.path("union_reserved_tasks").asInt)
        grant.path("operators").elements.asScala.exists(_.asText==t.operator) && capacity>used(id)
      }
      val selected=t.node match {
        case Some(id)=>eligible.find(m=>logicalNode(m.path("identity").asText)==id).getOrElse(throw new IllegalArgumentException("requested logical node unavailable"))
        case None=>
          val local=eligible.filter(m=>preference.contains(m.path("identity").asText))
          val pool=if(local.nonEmpty)local else eligible
          require(pool.nonEmpty,"no eligible execution node")
          if(sources.nonEmpty && sources.forall(_._4.isDefined) && t.inputs.values.forall(_.uri.nonEmpty))
            pool.minBy(m=>(sources.filter(_._2!=m.path("identity").asText).map(s=>BigInt(s._4.get)).sum,m.path("identity").asText))
          else preference.iterator.flatMap(id=>pool.find(_.path("identity").asText==id)).take(1).toVector.headOption.getOrElse(pool.sortBy(_.path("identity").asText).head)
      }
      val id=selected.path("identity").asText;used(id)+=1;placements(t.id)=id
      t.copy(node=Some(selected.path("data_origin").asText),inputs=scala.collection.immutable.VectorMap.from(t.inputs.toVector.map {case(p,b)=>p->b.copy(uri=b.uri.map(_=>sources.find(_._1==p).get._3))}))
    }
    JobPlan(tasks,input.outputs)
  }
}
