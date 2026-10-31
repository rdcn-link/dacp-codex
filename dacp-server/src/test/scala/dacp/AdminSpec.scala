package dacp

import _root_.xstream.data._

import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite

class AdminSpec extends AnyFunSuite {
  test("shared admin manages roles and credentials locally and rejects unknown commands") {
    val dir=Files.createTempDirectory("dacp-admin-")
    val output=new java.io.ByteArrayOutputStream()
    try {
      Console.withOut(output){Admin.main(Array("--help"));Admin.main(Array("create-account",dir.toString,"--subject","alice","--role","runner"))}
      assert(output.toString("UTF-8").contains("dacp-admin create-account"))
      val store=new dacp.union.Credentials(dir)
      try assert(store.listSubjects.path("alice").path("roles").get(0).asText=="runner")finally store.close()
      intercept[IllegalArgumentException](Admin.main(Array("serve")))
      intercept[IllegalArgumentException](Admin.main(Array("credentials",dir.toString,"list")))
      intercept[IllegalArgumentException](Admin.main(Array("set-roles",dir.toString,"alice","admin")))
    }finally Data.deleteTree(dir)
  }
  test("history administration queries running stores without changing records") {
    val dir=Files.createTempDirectory("dacp-admin-history-")
    val history=new ExecutionHistory(new FileExecutionStore(dir,false))
    try {
      history.begin("job","example")
      val list=new java.io.ByteArrayOutputStream();val get=new java.io.ByteArrayOutputStream()
      Console.withOut(list){Admin.main(Array("list-history",dir.toString))}
      Console.withOut(get){Admin.main(Array("get-history",dir.toString,"example"))}
      assert(dacp.union.Json.mapper.readTree(list.toByteArray).isArray)
      assert(dacp.union.Json.mapper.readTree(get.toByteArray).path("state").asText=="accepted")
      assert(history.get("example").get.path("state").asText=="accepted")
      intercept[IllegalArgumentException](Admin.main(Array("identity","unused.yaml")))
      intercept[IllegalArgumentException](Main.main(Array("list-history",dir.toString)))
    }finally{history.close();Data.deleteTree(dir)}
  }

  test("account provisioning pairs credentials, validates before writes and keeps secrets private") {
    val dir=Files.createTempDirectory("paired-account-")
    def invoke(args:String*):com.fasterxml.jackson.databind.JsonNode={
      val out=new java.io.ByteArrayOutputStream()
      Console.withOut(out){Admin.main(args.toArray)}
      dacp.union.Json.mapper.readTree(out.toByteArray)
    }
    try {
      val issued=invoke("create-account",dir.toString,"--subject","alice")
      val store=new dacp.union.Credentials(dir)
      try {
        val token=store.authenticate(issued.path("token").asText,None).get
        val key=store.s3(issued.path("access_key_id").asText).get
        assert(token.subject==key.subject && key.subject=="alice")
        assert(store.permissions(token)==Set("data:read") && store.permissions(key)==Set("data:read"))
        assert(key.secret.contains(issued.path("secret_key").asText))
        assert(store.authenticate(key.secret.get,None).isEmpty)
        val before=Files.readString(dir.resolve("credentials.json"))
        for(args<-Seq(
          Seq("create-account",dir.toString,"--subject","alice","--role","runner"),
          Seq("create-account",dir.toString,"--subject","bob","--role","admin"),
          Seq("create-account",dir.toString,"--subject","bob","--expires-in","-1"),
          Seq("create-account",dir.toString,"--subject","bob","--kind","s3"),
          Seq("create-token",dir.toString,"--subject","bob"))) {
          intercept[IllegalArgumentException](invoke(args:_*))
          assert(Files.readString(dir.resolve("credentials.json"))==before)
        }
        val extra=invoke("create-token",dir.toString,"--subject","alice")
        assert(extra.path("subject").asText=="alice" && extra.path("token").asText!=issued.path("token").asText)
        val listing=invoke("list-credentials",dir.toString).toString
        assert(!listing.contains(issued.path("token").asText) && !listing.contains(key.secret.get))
        val audit=Files.readString(dir.resolve("credential-audit.jsonl"))
        assert(!audit.contains(issued.path("token").asText) && !audit.contains(key.secret.get))
      }finally store.close()
    }finally Data.deleteTree(dir)
  }

}
