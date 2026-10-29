package dacp
import java.io.FileNotFoundException

/** Static publication uses exactly the same catalog contract as external discovery plugins. */
class StaticDataCatalogProvider extends DataCatalogProvider {
  val id="static"
  def create(settings:Map[String,Object],context:CatalogContext):DataCatalog={
    require(settings.isEmpty,"static catalog does not accept source settings")
    val built=scala.collection.mutable.Map.empty[String,PublishedDataset]
    try context.configuredDatasets.foreach {case(name,definition)=>DatasetNames.validate(name);built(name)=context.createDataset(definition)}
    catch {case scala.util.control.NonFatal(t)=>built.values.foreach(p=>try p.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)});throw t}
    val datasets=built.toMap
    new DataCatalog {
      def list():Seq[DatasetInfo]=datasets.keys.toSeq.sorted.map(name=>DatasetInfo(name,context.configuredDatasets(name).description))
      def open(name:String):PublishedDataset={
        val underlying=datasets.getOrElse(name,throw new FileNotFoundException("dataset unavailable"))
        new PublishedDataset {def open(check:()=>Unit):DatasetAccess=underlying.open(check)}
      }
      override def close():Unit={
        var failure:Throwable=null
        datasets.values.foreach(p=>try p.close() catch{case scala.util.control.NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)})
        if(failure!=null)throw failure
      }
    }
  }
}
