package xstream.data

/** Public, user-defined JSON object text. Never include credentials or storage settings. */
object DatasetDescriptions {
  private val reader=new com.fasterxml.jackson.databind.ObjectMapper().reader()
    .`with`(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  def validate(value:String):Unit={
    require(value!=null && value.length<=32768,"dataset description must be JSON object text of at most 32768 characters")
    val node=try reader.readTree(value)
      catch {case scala.util.control.NonFatal(_)=>throw new IllegalArgumentException("dataset description must be valid JSON object text")}
    require(node!=null && node.isObject,"dataset description must be a JSON object")
  }
}
case class DatasetInfo(name:String,description:String="{}") {
  DatasetDescriptions.validate(description)
}
object DatasetNames {
  def validate(name:String):Unit=require(name!=null && name.nonEmpty && !name.contains("/") && !name.contains("\\") && name!="." && name!=".." && !name.exists(_.isControl),"invalid dataset name")
}

/** Thread-safe live discovery. A failed backend query must throw, not return an empty list.
  * Each open returns a caller-owned PublishedDataset lease. Its close must not close resources
  * owned by another lease. Existing leases remain usable when discovery metadata changes.
  */
case class DatasetDirectory(entries:Seq[DatasetInfo],complete:Boolean=true,unavailableSources:Int=0)

trait DataCatalog extends AutoCloseable {
  def directory(check:()=>Unit):DatasetDirectory=DatasetDirectory(list(check))
  def list():Seq[DatasetInfo]
  def list(check:()=>Unit):Seq[DatasetInfo]={check();val result=list();check();result}
  def open(name:String):PublishedDataset
  def open(name:String,check:()=>Unit):PublishedDataset={check();open(name)}
  def close():Unit=()
}
case class DataCatalogDefinition(provider:String,source:Map[String,Object]=Map.empty) {
  override def toString:String=s"DataCatalogDefinition($provider,<private settings>)"
}
trait DataCatalogProvider extends Plugin {
  def id:String
  def create(settings:Map[String,Object],context:CatalogContext):DataCatalog
}
trait CatalogContext extends DatasetContext {
  /** Static configuration, used by the built-in static catalog. */
  def configuredDatasets:Map[String,DatasetDefinition]
}
