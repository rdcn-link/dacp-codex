package dacp

import java.nio.file.{Files,Path}
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class DatasetCatalogSpec extends AnyFunSuite {
  private def config(dir:Path,catalog:Option[DatasetCatalogDefinition]=None,instance:Option[DatasetCatalog]=None)=
    Config("localhost",8815,"localhost",Map.empty,Seq("dacp://localhost:8815/public"),dir,dir,dir,datasetCatalog=catalog,catalogOverride=instance)

  test("registered JDBC catalog discovers additions and removals through Flight; leases survive removal and authorization still applies") {
    val dir=Files.createTempDirectory("dynamic-catalog-");val url="jdbc:h2:mem:catalog"+java.util.UUID.randomUUID.toString
    val db=DriverManager.getConnection(url);val sql=db.createStatement()
    sql.execute("CREATE TABLE dacp_datasets(name VARCHAR PRIMARY KEY)")
    sql.execute("CREATE TABLE dacp_items(dataset_name VARCHAR, item_path VARCHAR, file_path VARCHAR)")
    val file=dir.resolve("records.csv");Files.writeString(file,"id\n1\n2\n")
    val insert=db.prepareStatement("INSERT INTO dacp_items VALUES (?, '/records', ?)")
    val c=config(dir,Some(DatasetCatalogDefinition("example-jdbc-catalog",Map("jdbc_url"->url))))
    val allocator=new RootAllocator()
    val server=FlightServer.builder(allocator,Location.forGrpcInsecure("localhost",0),new Producer(c,allocator)).build().start()
    val client=FlightClient.builder(allocator,server.getLocation).build()
    def names()=client.listFlights(Criteria.ALL).asScala.toSeq.map(_.getDescriptor.getPath.get(1))
    try {
      assert(names().isEmpty)
      sql.execute("INSERT INTO dacp_datasets VALUES ('public'), ('private')")
      insert.setString(1,"public");insert.setString(2,file.toString);insert.executeUpdate()
      assert(names()==Seq("public"))
      val info=client.getInfo(FlightDescriptor.path("datasets","public","/records"))
      val stream=client.getStream(info.getEndpoints.get(0).getTicket)
      try assert(stream.next())finally stream.close()
      intercept[FlightRuntimeException](client.getInfo(FlightDescriptor.path("datasets","private")))
      val lease=c.openDataset("public")
      sql.execute("DELETE FROM dacp_datasets WHERE name='public'")
      assert(names().isEmpty)
      assert(lease.count("/records").contains(2L));lease.close();lease.close()
      val missing=intercept[FlightRuntimeException](client.getInfo(FlightDescriptor.path("datasets","public")))
      assert(missing.status.code()==FlightStatusCode.NOT_FOUND)
      sql.execute("DROP TABLE dacp_datasets")
      intercept[FlightRuntimeException](names()) // Backend failure must not masquerade as an empty catalog.
    }finally {client.close();server.close();c.close();allocator.close();insert.close();sql.close();db.close();Data.deleteTree(dir)}
  }
  test("catalog identity checks, cancellation and ownership apply to directly supplied catalogs") {
    val dir=Files.createTempDirectory("catalog-contract-");val closed=new AtomicInteger();val leases=new AtomicInteger()
    var entries=Seq(DatasetInfo("public"))
    val cat=new DatasetCatalog {
      def list():Seq[DatasetInfo]=entries
      def open(name:String):PublishedDataset=new PublishedDataset {
        def open(check:()=>Unit):DatasetAccess=throw new IllegalStateException("failed access initialization")
        override def close():Unit={leases.incrementAndGet();()}
      }
      override def close():Unit={closed.incrementAndGet();()}
    }
    val c=config(dir,instance=Some(cat))
    try {
      assert(c.datasetInfos()==entries)
      entries=Seq(DatasetInfo("../bad"));intercept[IllegalArgumentException](c.datasetInfos())
      entries=Seq(DatasetInfo("public"),DatasetInfo("public"));intercept[IllegalArgumentException](c.datasetInfos())
      intercept[java.util.concurrent.CancellationException](c.datasetInfos(()=>throw new java.util.concurrent.CancellationException()))
      intercept[IllegalStateException](c.openDataset("public"));assert(leases.get()==1)
      c.close();c.close();assert(closed.get()==1)
    }finally {c.close();Data.deleteTree(dir)}
  }
  test("catalog configuration rejects unknown providers, invalid fields and simultaneous static declarations") {
    val dir=Files.createTempDirectory("catalog-settings-")
    val file=dir.resolve("server.yaml")
    val base="host: localhost\npublic_host: localhost\ntls_cert: cert\ntls_key: key\ntls_ca: ca\nallowed_uri_prefixes: []\n"
    try {
      for(text<-Seq("dataset_catalog: {provider: missing}","dataset_catalog: {provider: static, unexpected: true}","dataset_catalog: {provider: static}\ndatasets: {public: {provider: mixed, items: {}}}")) {
        Files.writeString(file,base+text);intercept[IllegalArgumentException](Config.load(file))
      }
    }finally Data.deleteTree(dir)
  }
}
