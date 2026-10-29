package example

import dacp._
import java.nio.file.Path
import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Application owns registry updates and the JVM lifecycle; DACP owns its catalog instance. */
object EmbeddedServerExample {
  def main(args:Array[String]):Unit={
    require(args.length==2,"arguments: CERTIFICATE_DIRECTORY CSV_FILE")
    val certificates=Path.of(args(0)).toAbsolutePath
    val config=Config("127.0.0.1",8815,"localhost",Map.empty,
      certificates.resolve("server.crt"),certificates.resolve("server.key"),certificates.resolve("ca.crt"))
    val registry=new ConcurrentHashMap[String,DatasetDefinition]()
    val catalog=new DataCatalog {
      def list():Seq[DatasetInfo]=registry.keySet.asScala.toVector.sorted.map(DatasetInfo(_))
      def open(name:String):PublishedDataset={
        val definition=Option(registry.get(name)).getOrElse(throw new FileNotFoundException("dataset unavailable"))
        config.createDataset(definition)
      }
    }
    val server=DacpServer.builder().config(config).catalog(catalog).build()
    val hook=new Thread(()=>server.close())
    Runtime.getRuntime.addShutdownHook(hook) // The embedding application chooses whether to install a hook.
    try {
      server.start() // Returns after the listener starts.
      registry.put("reports",DatasetDefinition("mixed",items=Map("/orders"->Map("file"->Path.of(args(1)).toAbsolutePath.toString))))
      println("Published dynamically: dacp://localhost:8815/reports/orders")
      // The application may registry.put(...) / registry.remove(...) while the server runs.
      server.awaitTermination()
    }finally {
      try Runtime.getRuntime.removeShutdownHook(hook)catch{case _:IllegalStateException=>()}
      server.close()
    }
  }
}
