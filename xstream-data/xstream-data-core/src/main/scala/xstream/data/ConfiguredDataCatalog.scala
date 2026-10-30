package xstream.data

import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Shared configuration host for local applications. Dataset parsing stays plugin-owned. */
final class ConfiguredDataCatalog(settings:JsonNode,val plugins:PluginEnvironment) extends CatalogContext with AutoCloseable {
  def batchSize:Int=settings.path("batch_size").asInt(1024)
  def byteChunk:Int=settings.path("byte_chunk").asInt(65536)
  def inferSample:Int=settings.path("infer_sample").asInt(4)
  def countFileRecordsOnStartup:Boolean=false
  private def mapping(n:JsonNode):Map[String,Object]=if(n.isMissingNode)Map.empty else Data.mapper.convertValue(n,classOf[java.util.LinkedHashMap[String,Object]]).asScala.toMap
  val sources:Map[String,SourceDefinition]=settings.path("sources").fields.asScala.map(e=>e.getKey->SourceDefinition(e.getValue.path("provider").asText,mapping(e.getValue.path("settings")))).toMap
  val configuredDatasets:Map[String,DatasetDefinition]=settings.path("datasets").fields.asScala.map(e=>e.getKey->plugins.datasetDefinition(mapping(e.getValue))).toMap
  require(!settings.has("data_catalog") || configuredDatasets.isEmpty,"datasets and data_catalog are mutually exclusive")
  def createDataset(d:DatasetDefinition):PublishedDataset=plugins.byId(classOf[DatasetProvider],d.provider).create(d,this)
  private val definition=settings.path("data_catalog")
  val catalog:DataCatalog=DataCatalogs.create(DataCatalogDefinition(definition.path("provider").asText("static"),mapping(definition.path("source"))),this)
  private val lease=try DataCatalogRuntime.bind(catalog)catch{case scala.util.control.NonFatal(e)=>catalog.close();throw e}
  def open(name:String,check:()=>Unit=()=>()):DatasetAccess={
    val published=catalog.open(name,check)
    val access=try ItemByteViews.wrap(published.open(check),i=>plugins.views(i),byteChunk)catch{case scala.util.control.NonFatal(e)=>published.close();throw e}
    new DatasetAccess {
      def list():Seq[Item]=access.list()
      def list(path:String):Seq[Item]=access.list(path)
      def describe(path:String):Item=access.describe(path)
      def count(path:String):Option[Long]=access.count(path)
      def view[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]=access.view(path,kind)
      override def close():Unit=try access.close()finally published.close()
    }
  }
  def close():Unit=lease.close()
}
