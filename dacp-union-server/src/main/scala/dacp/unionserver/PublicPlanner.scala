package dacp.unionserver

import _root_.xstream.engine.{JobPlan,Jobs}


import dacp._
import dacp.union._
import xstream.data.PluginEnvironment
import org.apache.arrow.flight.{FlightDescriptor,CallOptions}
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

import scala.jdk.CollectionConverters._

private[unionserver] final case class PlacementDecision(step:String,node:String,reason:String,requestedSource:Option[String]=None,resolvedSource:Option[String]=None)
private[unionserver] final case class UnionPlan(job:JobPlan,decisions:Vector[PlacementDecision],optimization:com.fasterxml.jackson.databind.JsonNode)

private[unionserver] final class PublicPlanner(members:Members,settings:Settings,catalog:ProxyCatalog,routes:DatasetRoutes,plugins:PluginEnvironment) {
  private val optimizer=plugins.byId(classOf[PhysicalPlanOptimizer],settings.planning.provider)
  optimizer.validate(settings.planning.settings)
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
  def plan(input:JobPlan,check:()=>Unit,disableFusion:Boolean=false):UnionPlan={
    val candidates=members.all.map(m=>members.inspect(m)).filter(_.path("state").asText=="approved")
    val warnings=scala.collection.mutable.ArrayBuffer.empty[String]
    val sourceLocations=scala.collection.mutable.Map.empty[String,(String,String,Option[Long])]
    val decisions=scala.collection.mutable.ArrayBuffer.empty[PlacementDecision]
    val resolved=Jobs.resolveReads(input,origin)
    resolved.steps.foreach { t=>
      require(t.inlineOperator.isEmpty && t.localOperator.isEmpty,"Union requires versioned operators")
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
        candidates.find(m=>m.path("data_origin").asText==DacpUri.origin(uri)).map { m=>
          check();val u=DacpUri.parse(uri);val parts=u.getPath.stripPrefix("/").split("/",2)
          val size=try {
            val client=members.remote.dataClient(m.path("url").asText,Some(m.path("identity").asText))
            val info=client.getInfo(FlightDescriptor.path("datasets",parts(0),"/"+parts(1)),CallOptions.timeout(5,TimeUnit.SECONDS))
            val item=dacp.protocol.Metadata.Item.parseFrom(info.getAppMetadata)
            if(item.hasTotalSizeBytes)Some(item.getTotalSizeBytes)else None
          }catch{case _:java.util.concurrent.CancellationException=>throw new java.util.concurrent.CancellationException();case NonFatal(_)=>warnings+="Item size unavailable; conservative estimate used";None}
          check();("result",m.path("identity").asText,uri,size)
        }
      }
      sources.headOption.foreach(s=>sourceLocations(t.id)=(s._2,s._3,s._4))
    }
    val profiles=candidates.map { m=>
      check();val body=Json.obj().put("union_id",members.unionId);val refs=body.putArray("operators");resolved.steps.map(_.operator).distinct.take(256).foreach(refs.add)
      val values=try members.remote.action(m.path("url").asText,"planning_profile",body,Some(m.path("identity").asText),timeout=5)
      catch{case _:java.util.concurrent.CancellationException=>throw new java.util.concurrent.CancellationException();case NonFatal(_)=>warnings+="Installed runtime metadata unavailable; fusion not assumed";Json.obj()}
      m.path("identity").asText->values
    }.toMap
    val targets=candidates.map { m=>
      val status=m.path("status");val r=status.path("resources")
      val memory=if(r.has("memory_total_bytes")&&r.has("memory_used_bytes"))Some(math.max(0L,r.path("memory_total_bytes").asLong-r.path("memory_used_bytes").asLong))else None
      PlanningTarget(m.path("identity").asText,math.min(status.path("capacity").asInt,status.path("union_capacity").asInt),
        status.path("executors").elements.asScala.map(_.asText).toSet,memory,status.path("reserved_tasks").asInt*500d)
    }.toVector
    val graph=resolved.steps.map { t=>
      val requested=t.node.map { name=>candidates.find(m=>logicalNode(m.path("identity").asText)==name).getOrElse(throw new IllegalArgumentException("requested logical node unavailable")).path("identity").asText }
      val metadata=profiles.flatMap{case(node,operators)=>Option(operators.get(t.operator)).map(p=>node->PlanningProfile(p.path("executor").asText,p.path("runtime").asText,Option(p.get("fusion_key")).map(_.asText)))}
      PlanningStep(t.id,Jobs.dependencies(t),sourceLocations.get(t.id).map(s=>s._1->s._3),requested,t.operator,Json.mapper.writeValueAsString(t.params),metadata,
        disableFusion||(t.operator==Jobs.ItemSource&&input.outputs.values.exists(_._1==t.id)))
    }
    val result=GraphPlacement.optimize(PlanningInput(graph,targets,settings.planning.beamWidth),Some(optimizer),settings.planning.settings,check)
    val placements=result.placements
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
    val report=Json.obj().put("provider",optimizer.id).put("search","bounded_beam").put("beam_width",settings.planning.beamWidth)
      .put("evaluated_candidates",result.candidates).put("estimated_completion_ms",result.estimate.completionMs).put("estimated_transfer_bytes",result.estimate.transferBytes)
      .put("estimated_processes",result.estimate.processes).put("score",result.score).put("estimates_only",true)
    val notices=report.putArray("warnings");warnings.distinct.foreach(notices.add)
    def nodeUrl(id:String)=candidates.find(_.path("identity").asText==id).get.path("data_origin").asText
    val estimates=report.putArray("steps");result.estimate.steps.toVector.sortBy(_._1).foreach{case(id,e)=>estimates.addObject().put("step",id).put("output_bytes",e.outputBytes).put("compute_ms",e.computeMs).put("startup_ms",e.startupMs).put("confidence",e.confidence)}
    val edges=report.putArray("edges");result.estimate.edges.foreach(e=>edges.addObject().put("from",e.from).put("to",e.to).put("source_node",nodeUrl(e.sourceNode)).put("target_node",nodeUrl(e.targetNode)).put("estimated_bytes",e.bytes).put("estimated_transfer_ms",e.transferMs).put("mode",e.mode))
    val units=report.putArray("execution_units");result.estimate.units.foreach { u=>val n=units.addObject().put("id",u.id).put("node",nodeUrl(u.node)).put("runtime",u.runtime).put("mode",u.mode);val ids=n.putArray("steps");u.steps.foreach(ids.add)}
    UnionPlan(JobPlan(steps,input.outputs,input.name),decisions.toVector,report)
  }
}
