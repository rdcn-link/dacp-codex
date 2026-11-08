package dacp

import java.time.Instant
import org.apache.arrow.flight.{CallStatus,FlightDescriptor}
import org.apache.arrow.memory.BufferAllocator
import _root_.xstream.data._
import _root_.xstream.engine.{JobPlan,Jobs,Task}
import dacp.union.Rpc

/** Dataset-wide content policy. Discovery metadata is always public. */
object DatasetPublication {
  final case class Policy(mode:String,publicAt:Option[Instant]=None) {
    def readable(now:Instant=Instant.now()):Boolean=mode=="public" || (mode=="embargoed" && !now.isBefore(publicAt.get))
  }
  def parse(description:String):Policy={
    val root=Data.mapper.readTree(description)
    val p=root.get("publication")
    if(p==null)return Policy("private")
    require(p.isObject,"publication must be an object")
    import scala.jdk.CollectionConverters._
    val fields=p.fieldNames.asScala.toSet
    require(p.path("policy").isTextual,"publication.policy is required")
    p.path("policy").asText match {
      case mode @ ("public"|"private")=>require(fields==Set("policy"),"unexpected publication field");Policy(mode)
      case "embargoed"=>
        require(fields==Set("policy","public_at") && p.path("public_at").isTextual,"embargoed publication requires public_at")
        Policy("embargoed",Some(Instant.parse(p.path("public_at").asText)))
      case _=>throw new IllegalArgumentException("unknown publication policy")
    }
  }
  def trustedUnion:Boolean=Rpc.isAuthenticatedCall && Rpc.principal.kind=="union" && Rpc.principal.active()
  def trustedService(config:Config):Boolean=trustedUnion || (Rpc.isAuthenticatedCall && Rpc.principal.credentialId.exists(config.credentials.isInternal))
  def requireRead(description:String,trusted:Boolean=false):Unit={
    val policy=parse(description)
    if(!trusted && !policy.readable())throw CallStatus.UNAUTHORIZED.withDescription("dataset content is not public").toRuntimeException
  }
  def requireDataset(config:Config,name:String,trusted:Boolean=false):Unit={
    if(trusted)return
    val info=config.datasetDirectory().entries.find(_.name==name).getOrElse(throw new DatasetNotFound)
    requireRead(info.description,trusted)
  }
  def requireUri(config:Config,allocator:BufferAllocator,value:String,trusted:Boolean=false):Unit={
    val uri=if(value.startsWith("context:"))DacpUri.origin(config.uri(""))+java.net.URI.create(value).getRawPath else value
    val u=DacpUri.parse(uri);val dataset=u.getPath.stripPrefix("/").split("/",2).head
    if(DacpUri.origin(uri)==DacpUri.origin(config.uri("")))requireDataset(config,dataset,trusted)
    else if(!trusted){
      val client=NodeConnection.open(config,allocator,uri)
      try {
        val info=client.getInfo(FlightDescriptor.path("datasets",dataset))
        val metadata=dacp.protocol.Metadata.DataSet.parseFrom(info.getAppMetadata)
        requireRead(metadata.getDescription)
      }finally client.close()
    }
  }
  def requireTask(config:Config,allocator:BufferAllocator,task:Task,trusted:Boolean=trustedUnion || !Rpc.isAuthenticatedCall):Unit={
    val resolved=Jobs.resolveReads(JobPlan(Vector(task),Map.empty),config.uri("")).steps.head
    val uris=Jobs.readBinding(resolved).flatMap(_.uri).toSeq++resolved.inputs.values.flatMap(_.uri)
    uris.distinct.foreach(requireUri(config,allocator,_,trusted))
  }
}
