package dacp

/** Public discovery metadata. Never include storage locations, credentials or private settings. */
case class DatasetMetadata(title:String="",description:String="",tags:Seq[String]=Seq.empty) {
  require(title!=null && title.length<=1024 && description!=null && description.length<=32768,"invalid dataset description")
  require(tags!=null && tags.size<=64 && tags.forall(t=>t!=null && t.nonEmpty && t.length<=256),"invalid dataset tags")
}
case class DatasetInfo(name:String,metadata:DatasetMetadata=DatasetMetadata())
object DatasetNames {
  def validate(name:String):Unit=require(name!=null && name.nonEmpty && !name.contains("/") && !name.contains("\\") && name!="." && name!=".." && !name.exists(_.isControl),"invalid dataset name")
}

/** Thread-safe live discovery. A failed backend query must throw, not return an empty list.
  * Each open returns a caller-owned PublishedDataset lease. Its close must not close resources
  * owned by another lease. Existing leases remain usable when discovery metadata changes.
  */
trait DatasetCatalog extends AutoCloseable {
  def list():Seq[DatasetInfo]
  def list(check:()=>Unit):Seq[DatasetInfo]={check();val result=list();check();result}
  def open(name:String):PublishedDataset
  def open(name:String,check:()=>Unit):PublishedDataset={check();open(name)}
  def close():Unit=()
}
case class DatasetCatalogDefinition(provider:String,source:Map[String,Object]=Map.empty) {
  override def toString:String=s"DatasetCatalogDefinition($provider,<private settings>)"
}
trait DatasetCatalogProvider extends Plugin {
  def id:String
  def create(settings:Map[String,Object],context:CatalogContext):DatasetCatalog
}
trait CatalogContext extends DatasetContext {
  /** Static configuration, used by the built-in static catalog. */
  def configuredDatasets:Map[String,DatasetDefinition]
}
