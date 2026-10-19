package dacp

import java.io.FileNotFoundException
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

private[dacp] final class DatasetNotFound extends FileNotFoundException("dataset unavailable")

object DatasetCatalogs {
  def create(definition:DatasetCatalogDefinition,context:CatalogContext):DatasetCatalog={
    context.plugins.byId(classOf[DatasetCatalogProvider],definition.provider).create(definition.source,context)
  }
}
