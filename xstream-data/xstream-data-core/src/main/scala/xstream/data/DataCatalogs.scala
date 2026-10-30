package xstream.data

import java.io.FileNotFoundException
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

final class DatasetNotFound extends FileNotFoundException("dataset unavailable")

object DataCatalogs {
  def create(definition:DataCatalogDefinition,context:CatalogContext):DataCatalog={
    context.plugins.byId(classOf[DataCatalogProvider],definition.provider).create(definition.source,context)
  }
}
