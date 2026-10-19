package example

import dacp._
import java.nio.file.Path
import java.sql.{Connection,DriverManager}
import java.io.FileNotFoundException
import scala.util.Using

/** Example metadata tables select public Dataset names and file-backed Items at request time. */
class JdbcCatalogProvider extends DatasetCatalogProvider {
  val id="example-jdbc-catalog"
  def create(settings:Map[String,Object],context:CatalogContext):DatasetCatalog={
    require(settings.keySet.subsetOf(Set("jdbc_url","user_env","password_env")) && settings.get("jdbc_url").exists(_.isInstanceOf[String]),"catalog requires source.jdbc_url")
    def credential(key:String):Option[String]=settings.get(key).map { value=>
      require(value.isInstanceOf[String],"credential environment name must be text")
      sys.env.getOrElse(value.toString,throw new IllegalArgumentException("missing catalog credential environment variable"))
    }
    val user=credential("user_env");val password=credential("password_env")
    require(user.isDefined==password.isDefined,"catalog credentials require both user_env and password_env")
    val url=settings("jdbc_url").toString
    def connection():Connection=user match {
      case Some(u)=>DriverManager.getConnection(url,u,password.get)
      case None=>DriverManager.getConnection(url)
    }
    new DatasetCatalog {
      def list():Seq[DatasetInfo]=list(()=>())
      override def list(check:()=>Unit):Seq[DatasetInfo]=Using.Manager { use=>
        check();val db=use(connection());val query=use(db.prepareStatement("SELECT name FROM dacp_datasets ORDER BY name"));query.setQueryTimeout(10)
        val rows=use(query.executeQuery());val result=Vector.newBuilder[DatasetInfo]
        while(rows.next()){check();result+=DatasetInfo(rows.getString(1))}
        result.result()
      }.get
      def open(name:String):PublishedDataset=open(name,()=>())
      override def open(name:String,check:()=>Unit):PublishedDataset={
        DatasetNames.validate(name)
        val bindings=Using.Manager { use=>
          check();val db=use(connection())
          val exists=use(db.prepareStatement("SELECT name FROM dacp_datasets WHERE name = ?"));exists.setQueryTimeout(10);exists.setString(1,name)
          if(!use(exists.executeQuery()).next())throw new FileNotFoundException("dataset unavailable")
          val query=use(db.prepareStatement("SELECT item_path, file_path FROM dacp_items WHERE dataset_name = ? ORDER BY item_path"));query.setQueryTimeout(10);query.setString(1,name)
          val rows=use(query.executeQuery());val result=Map.newBuilder[String,Map[String,Object]]
          while(rows.next()){check();result+=(rows.getString(1)->Map[String,Object]("file"->rows.getString(2)))}
          result.result()
        }.get
        check();context.createDataset(DatasetDefinition("mixed",items=bindings))
      }
    }
  }
}
