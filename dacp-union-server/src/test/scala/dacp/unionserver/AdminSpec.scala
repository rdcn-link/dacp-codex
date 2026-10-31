package dacp.unionserver

import dacp.union.Json
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import scala.sys.process._
import scala.jdk.CollectionConverters._

class AdminSpec extends AnyFunSuite {
  test("Union admin shares credentials and history; identity and member diagnostics never start a listener") {
    val dir=Files.createTempDirectory("union-admin-")
    val socket=new java.net.ServerSocket(0)
    def capture(args:String*):String={val out=new java.io.ByteArrayOutputStream();Console.withOut(out){Admin.main(args.toArray)};out.toString("UTF-8")}
    try {
      val key=dir.resolve("key.pem");val cert=dir.resolve("cert.pem")
      assert(Process(Seq("openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-subj","/CN=localhost","-keyout",key.toString,"-out",cert.toString)).!(ProcessLogger(_=>(),_=>()))==0)
      val config=Json.obj().put("host","localhost").put("port",socket.getLocalPort).put("public_url",s"dacp://localhost:${socket.getLocalPort}")
        .put("tls_cert",cert.toString).put("tls_key",key.toString).put("tls_ca",cert.toString).put("state_directory",dir.resolve("state").toString)
      val path=dir.resolve("union.yaml");Files.writeString(path,config.toString)
      val first=Json.mapper.readTree(capture("identity",path.toString))
      val second=Json.mapper.readTree(capture("identity",path.toString))
      assert(first==second && first.path("union_id").asText.nonEmpty && first.path("scheduler_identity").asText.matches("[a-f0-9]{64}"))
      assert(capture("members",path.toString).trim.isEmpty)
      assert(capture("--help").contains("union-admin identity CONFIG"))
      val account=Json.mapper.readTree(capture("create-account",dir.resolve("credentials").toString,"--subject","alice","--role","runner"))
      assert(account.path("token").asText.nonEmpty && account.path("access_key_id").asText.nonEmpty && account.path("secret_key").asText.nonEmpty)
      val common=new java.io.ByteArrayOutputStream()
      Console.withOut(common){dacp.Admin.main(Array("list-credentials",dir.resolve("credentials").toString))}
      assert(Json.mapper.readTree(common.toByteArray)==Json.mapper.readTree(capture("list-credentials",dir.resolve("credentials").toString)))
      assert(Json.mapper.readTree(capture("list-subjects",dir.resolve("credentials").toString)).path("alice").path("roles").get(0).asText=="runner")
      assert(Json.mapper.readTree(capture("list-history",dir.resolve("history").toString)).isArray)
      intercept[IllegalArgumentException](Admin.main(Array("serve",path.toString)))
      intercept[IllegalArgumentException](Main.main(Array("identity",path.toString)))
    }finally {
      socket.close()
      val paths=Files.walk(dir);try paths.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally paths.close()
    }
  }
}
