package dacp.unionserver

import dacp._
import dacp.protocol.Metadata
import org.apache.arrow.flight._
import scala.jdk.CollectionConverters._

/** On-demand, single-member metadata for the read-only website. */
private[unionserver] final class MemberDatasets(members:Members) {
  def entries(identity:String,check:()=>Unit=()=>(),hops:Vector[String]=Vector.empty):Option[Vector[Metadata.DataSet]]=members.all.find(_.path("identity").asText==identity).map { member=>
    check()
    val id=member.path("identity").asText;val origin=member.path("data_origin").asText
    val infos=try members.remote.catalog(member.path("url").asText,Some(id),hops=hops)
      catch{case scala.util.control.NonFatal(t)=>throw CallStatus.UNAVAILABLE.withDescription("dataset directory unavailable: "+origin).withCause(t).toRuntimeException}
    val entries=infos.map { f=>
      check();require(f.getAppMetadata.length<=256*1024,"catalog entry too large")
      val m=Metadata.DataSet.parseFrom(f.getAppMetadata)
      DatasetNames.validate(m.getName)
      DatasetDescriptions.validate(m.getDescription)
      val uri=new java.net.URI(m.getUri)
      require(m.getUri==new java.net.URI("dacp",null,uri.getHost,uri.getPort,"/"+m.getName,null,null).toASCIIString && DacpUri.origin(m.getUri)==origin,"catalog URI outside member namespace")
      // Whitelist public metadata; do not aggregate Item listings or unknown fields.
      Metadata.DataSet.newBuilder().setName(m.getName).setUri(m.getUri)
        .setDisplayName(m.getDisplayName).setDescription(m.getDescription).setNodeIdentity(id).setObservedAtMs(System.currentTimeMillis()).build()
    }.sortBy(_.getName)
    require(entries.map(_.getName).distinct.size==entries.size,"duplicate dataset name")
    entries
  }
}
