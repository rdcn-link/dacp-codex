package dacp

import _root_.xstream.data._

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class PluginDiscoverySpec extends AnyFunSuite {
  private val plugins=new PluginEnvironment()
  private val Formats=plugins.formats
  private val Containers=plugins.containers

  test("bundled capabilities are discoverable service contributions outside the server artifact") {
    def services[A](contract: Class[A]): Seq[A] =
      ServiceLoader.load(contract,plugins.classLoader).iterator().asScala.toVector
    val datasets = services(classOf[DatasetProvider])
    val formats = services(classOf[FormatHandler])
    val archives = services(classOf[ContainerReader])
    val catalogs = services(classOf[DataCatalogProvider])
    val executors = services(classOf[OperatorExecutor])
    assert(Set("directory","sqldb","mixed").subsetOf(datasets.map(_.id).toSet))
    assert(Set("csv","ndjson","json","bin","parquet","excel").subsetOf(formats.map(_.id).toSet))
    assert(Set("zip","gzip","tar","tar-gzip").subsetOf(archives.map(_.id).toSet))
    assert(catalogs.exists(_.id == "static"))
    assert(Set("container","jvm","python","command").subsetOf(executors.map(_.id).toSet))
    for (provider <- datasets.filter(p => Set("directory","sqldb","mixed")(p.id)))
      assert(plugins.byId(classOf[DatasetProvider],provider.id).getClass == provider.getClass)
    for (handler <- formats) assert(Formats.byId(handler.id).getClass == handler.getClass)
    for (reader <- archives) assert(Containers.byId(reader.id).getClass == reader.getClass)
    val bundled = datasets.filter(p => Set("directory","sqldb","mixed")(p.id)) ++
      formats.filter(p => Set("csv","ndjson","json","bin","parquet","excel")(p.id)) ++
      archives.filter(p => Set("zip","gzip","tar","tar-gzip")(p.id)) ++
      catalogs.filter(_.id == "static") ++ executors
    bundled.foreach { implementation =>
      val origin = implementation.getClass.getProtectionDomain.getCodeSource.getLocation.toString
      assert(!origin.contains("/dacp-server/"),s"bundled plugin leaked into server artifact: $origin")
      assert(origin.contains("/plugins/"),s"plugin is outside the plugin modules: $origin")
    }
  }
}
