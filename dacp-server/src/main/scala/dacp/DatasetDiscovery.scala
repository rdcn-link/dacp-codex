package dacp

import dacp.protocol.Metadata
import org.apache.arrow.flight._
import scala.jdk.CollectionConverters._

/** Metadata-only Flight discovery. Does not open datasets, enumerate Items or grant data access. */
object DatasetDiscovery {
  def metadata(config:Config,info:DatasetInfo):Metadata.DataSet=Metadata.DataSet.newBuilder()
    .setUri(config.uri(info.name)).setName(info.name).setTitle(info.metadata.title)
    .setDescription(info.metadata.description).addAllTags(info.metadata.tags.asJava).setObservedAtMs(System.currentTimeMillis()).build()
  def flight(meta:Metadata.DataSet,descriptor:FlightDescriptor):FlightInfo=
    new FlightInfo(PluginSchemas.bytes,descriptor,java.util.Collections.emptyList[FlightEndpoint](),-1,-1,false,
      org.apache.arrow.vector.ipc.message.IpcOption.DEFAULT,meta.toByteArray)
}
