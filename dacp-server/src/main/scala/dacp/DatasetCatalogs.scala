package dacp

import java.io.FileNotFoundException
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

private[dacp] final class DatasetNotFound extends FileNotFoundException("dataset unavailable")

object DatasetCatalogs {
  def create(definition:DatasetCatalogDefinition,context:CatalogContext):DatasetCatalog={
    val providers=Seq(new StaticDatasetCatalogProvider)++ServiceLoader.load(classOf[DatasetCatalogProvider],PluginLoader.classLoader).iterator.asScala.toVector
    require(providers.forall(_.id.matches("[a-z][a-z0-9-]*")),"invalid dataset catalog provider id")
    require(providers.map(_.id).distinct.size==providers.size,"duplicate dataset catalog provider id")
    providers.find(_.id==definition.provider).getOrElse(throw new IllegalArgumentException("unsupported dataset catalog provider")).create(definition.source,context)
  }
}

/** Static publication uses exactly the same catalog contract as external discovery plugins. */
class StaticDatasetCatalogProvider extends DatasetCatalogProvider {
  val id="static"
  def create(settings:Map[String,Object],context:CatalogContext):DatasetCatalog={
    require(settings.isEmpty,"static catalog does not accept source settings")
    val built=scala.collection.mutable.Map.empty[String,PublishedDataset]
    try context.configuredDatasets.foreach {case(name,definition)=>DatasetNames.validate(name);built(name)=context.createDataset(definition)}
    catch {case scala.util.control.NonFatal(t)=>built.values.foreach(p=>try p.close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)});throw t}
    val datasets=built.toMap
    new DatasetCatalog {
      def list():Seq[DatasetInfo]=datasets.keys.toSeq.sorted.map(DatasetInfo(_))
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
