package dacp.unionserver

import dacp._
import dacp.union.{Json,Rpc}
import scala.jdk.CollectionConverters._

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
  def plan(input:JobPlan,check:()=>Unit):JobPlan={
    val candidates=members.all.map(m=>members.inspect(m)).filter(_.path("state").asText=="approved")
    val placements=scala.collection.mutable.Map.empty[String,String]
    val used=scala.collection.mutable.Map.empty[String,Int].withDefaultValue(0)
    val readSizes=scala.collection.mutable.Map.empty[String,(String,Option[Long])]
    val decisions=scala.collection.mutable.ArrayBuffer.empty[PlacementDecision]
    val tasks=Jobs.resolveReads(input,origin).tasks.map { t=>
      require(t.source.isEmpty,"Union requires versioned operators")
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
      sources.headOption.foreach(s=>readSizes(t.id)=s._2->s._4)
      val rawInputs=t.inputs.values.toVector.flatMap(_.task.flatMap(readSizes.get))
      val preference=(sources.map(_._2)++Jobs.dependencies(t).flatMap(placements.get)).distinct
      val eligible=candidates.filter { m=>
        val id=m.path("identity").asText;val status=m.path("status");val grant=m.path("grant")
        val capacity=math.min(status.path("capacity").asInt,status.path("union_capacity").asInt)
        (sources.isEmpty || sources.forall(_._2==id)) && capacity>used(id)
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
      val original=input.tasks.find(_.id==t.id).get
      val requested=Jobs.readBinding(original).flatMap(_.uri)
      val reason=if(t.node.nonEmpty)"explicit_placement"else if(sources.nonEmpty)"data_locality"else if(requested.nonEmpty)"remote_read_capacity"else if(rawInputs.nonEmpty && rawInputs.size==t.inputs.size && rawInputs.forall(_._2.isDefined))"minimize_transfer"else if(preference.contains(id))"upstream_locality"else"available_capacity"
      decisions+=PlacementDecision(t.id,selected.path("url").asText,reason,requested,sources.headOption.map(_._3).orElse(Jobs.readBinding(t).flatMap(_.uri)))
      val params=new java.util.LinkedHashMap[String,Object](t.params)
      sources.headOption.foreach(s=>params.put("uri",s._3))
      t.copy(node=Some(selected.path("data_origin").asText),params=params)
    }
    JobPlan(tasks,input.outputs,input.name,decisions.toVector)
  }
}
