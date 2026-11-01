package dacp.unionserver

import _root_.xstream.engine.{JobPlan,Jobs}


import dacp._
import dacp.union.{Json,Rpc}

import scala.jdk.CollectionConverters._

private[unionserver] final case class PlacementDecision(step:String,node:String,reason:String,requestedSource:Option[String]=None,resolvedSource:Option[String]=None)
private[unionserver] final case class UnionPlan(job:JobPlan,decisions:Vector[PlacementDecision])

private[unionserver] final class PublicPlanner(members:Members,settings:Settings,catalog:ProxyCatalog,routes:DatasetRoutes) {
  private def origin=settings.publicUrl.stripSuffix("/")
  def logicalNode(member:String):String="nd-"+routes.assign(member,"").stripPrefix("ds-")
  def nodes:com.fasterxml.jackson.databind.JsonNode={
    val n=Json.obj();val nodes=n.putArray("nodes")
    members.all.foreach { m=>
      val member=members.inspect(m);val status=member.path("status")
      val node=nodes.addObject().put("id",logicalNode(member.path("identity").asText)).put("address",member.path("url").asText)
        .put("state",member.path("state").asText).put("capacity",status.path("capacity").asInt).put("observed_at_ms",member.path("observed_at_ms").asLong)
      Seq("reserved_tasks","union_reserved_tasks","union_capacity","started_at_ms","uptime_ms","web_enabled","web_url","executors","resources").foreach { key=>
        if(status.has(key))node.set[com.fasterxml.jackson.databind.JsonNode](key,status.get(key))
      }
    };n
  }
  def plan(input:JobPlan,check:()=>Unit):UnionPlan={
    val candidates=members.all.map(m=>members.inspect(m)).filter(_.path("state").asText=="approved")
    val sourceLocations=scala.collection.mutable.Map.empty[String,(String,String,Option[Long])]
    val decisions=scala.collection.mutable.ArrayBuffer.empty[PlacementDecision]
    val resolved=Jobs.resolveReads(input,origin)
    resolved.steps.foreach { t=>
      require(t.source.isEmpty && t.localOperator.isEmpty,"Union requires versioned operators")
      val sources=Jobs.readBinding(t).toVector.flatMap { b=>b.uri.filter(uri=>DacpUri.origin(uri)==origin).map { uri=>
        val port="result"
        require(DacpUri.origin(uri)==origin,"input must use the Union public URI")
        val parts=DacpUri.parse(uri).getPath.stripPrefix("/").split("/",2)
        require(parts.length==2,"input must address an Item")
        val(m,local)=catalog.target(parts(0),check)
        val published=catalog.open(parts(0),check);val access=published.open(check)
        val size=try access.describe("/"+parts(1)).totalSizeBytes finally {access.close();published.close()}
        (port,m.path("identity").asText,new java.net.URI("dacp",null,java.net.URI.create(m.path("data_origin").asText).getHost,java.net.URI.create(m.path("data_origin").asText).getPort,"/"+local+"/"+parts(1),null,null).toASCIIString,size)
      }} ++ Jobs.readBinding(t).toVector.flatMap(_.uri).filter(uri=>DacpUri.origin(uri)!=origin).flatMap { uri=>
        candidates.find(m=>m.path("data_origin").asText==DacpUri.origin(uri)).map(m=>("result",m.path("identity").asText,uri,None:Option[Long]))
      }
      sources.headOption.foreach(s=>sourceLocations(t.id)=(s._2,s._3,s._4))
    }
    val targets=candidates.map { m=>
      val status=m.path("status")
      PlanningTarget(m.path("identity").asText,math.min(status.path("capacity").asInt,status.path("union_capacity").asInt))
    }.toVector
    val graph=resolved.steps.map { t=>
      val requested=t.node.map { name=>candidates.find(m=>logicalNode(m.path("identity").asText)==name).getOrElse(throw new IllegalArgumentException("requested logical node unavailable")).path("identity").asText }
      PlanningStep(t.id,Jobs.dependencies(t),sourceLocations.get(t.id).map(s=>s._1->s._3),requested)
    }
    val placements=GraphPlacement.plan(graph,targets)
    val steps=resolved.steps.map { t=>
      val placement=placements(t.id)
      val selected=candidates.find(_.path("identity").asText==placement.target).get
      val source=sourceLocations.get(t.id)
      val original=input.steps.find(_.id==t.id).get
      val requested=Jobs.readBinding(original).flatMap(_.uri)
      decisions+=PlacementDecision(t.id,selected.path("url").asText,placement.reason,requested,source.map(_._2).orElse(Jobs.readBinding(t).flatMap(_.uri)))
      val params=new java.util.LinkedHashMap[String,Object](t.params)
      source.foreach(s=>params.put("uri",s._2))
      t.copy(node=Some(selected.path("data_origin").asText),params=params)
    }
    UnionPlan(JobPlan(steps,input.outputs,input.name),decisions.toVector)
  }
}
