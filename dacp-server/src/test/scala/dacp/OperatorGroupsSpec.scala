package dacp

import java.nio.file.Path
import org.scalatest.funsuite.AnyFunSuite

class OperatorGroupsSpec extends AnyFunSuite {
  private def task(id:String,input:InputBinding)=Task(id,Map("source"->input),"copy@1",new java.util.LinkedHashMap[String,Object]())
  private val plan=JobPlan(Vector(task("a",InputBinding(uri=Some("dacp://localhost:8815/data/item"))),task("b",InputBinding(task=Some("a")))),Map("result"->("b","result")))
  private def config=Config("localhost",8815,"localhost",Map.empty,Path.of("unused"),Path.of("unused"),Path.of("unused"))
  private val port=PortContract(Set("bytes"),Set.empty,None)
  private def op(image:String="sha256:"+"a"*64,executor:String="container",environment:Map[String,String]=Map.empty)=ResolvedOperator(image,None,None,OperatorCommand(environment=environment),Map("source"->port),Map("result"->port),executor,Some(OperatorProgram("copy@1","",None)))
  test("configuration reads a strict boolean fusion switch") {
    val file=java.nio.file.Files.createTempFile("fusion-config-", ".yaml")
    val base="host: localhost\npublic_host: localhost\ntls_cert: cert\ntls_key: key\ntls_ca: ca\n"
    try {
      for(value<-Seq("true","false")){
        java.nio.file.Files.writeString(file,base+s"operator_fusion_enabled: $value\n")
        val c=Config.load(file)
        try assert(c.operatorFusionEnabled==value.toBoolean) finally c.close()
      }
      java.nio.file.Files.writeString(file,base+"operator_fusion_enabled: 'false'\n")
      intercept[IllegalArgumentException](Config.load(file))
    }finally java.nio.file.Files.delete(file)
  }
  test("disabled fusion skips operator resolution and Docker") {
    val c=config.copy(operatorFusionEnabled=false)
    try assert(OperatorGroups.prepare(c,plan)==Left("disabled_by_configuration")) finally c.close()
  }
  test("fusion keeps unconsumed transport sources in the normal execution path") {
    val c=config
    val params=new java.util.LinkedHashMap[String,Object]();params.put("uri","dacp://localhost:8815/data/item");params.put("view","bytes")
    val read=Task("read",Map.empty,Jobs.ReadItem,params)
    try assert(OperatorGroups.prepare(c,plan.copy(tasks=plan.tasks:+read))==Left("unconsumed_transport_source")) finally c.close()
  }
  test("fixed image IDs, executor IDs and process environments must all agree") {
    val c=config
    try {
      assert(OperatorGroups.prepareResolved(c,plan,Map("a"->op(),"b"->op(image="sha256:"+"b"*64)))==Left("different_runtime_images"))
      assert(OperatorGroups.prepareResolved(c,plan,Map("a"->op(),"b"->op(executor="python")))==Left("different_executors"))
      assert(OperatorGroups.prepareResolved(c,plan,Map("a"->op(),"b"->op(environment=Map("LANG"->"C"))))==Left("different_process_environments"))
      assert(OperatorGroups.prepareResolved(c,plan,Map("a"->op(),"b"->op()))==Left("executor_or_operator_not_fusible"))
    }finally c.close()
  }
  test("node contraction rejects A to B to A, while preserving ordinary cross-node DAGs") {
    val expanded=plan.copy(tasks=plan.tasks:+task("c",InputBinding(task=Some("b"))))
    assert(!Jobs.canGroupByNode(expanded,Map("a"->"A","b"->"B","c"->"A")))
    assert(Jobs.canGroupByNode(expanded,Map("a"->"A","b"->"B","c"->"B")))
    assert(Jobs.canGroupByNode(expanded,Map("a"->"A","b"->"A","c"->"A")))
  }
  test("wheel mounting preserves the distribution filename required by pip") {
    val c=config;val file=java.nio.file.Files.createTempFile("sha256-wheel-", "")
    val zip=new java.util.zip.ZipOutputStream(java.nio.file.Files.newOutputStream(file))
    try {zip.putNextEntry(new java.util.zip.ZipEntry("operators.py"));zip.write("def process(ctx):\n    pass\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));zip.closeEntry()}finally zip.close()
    val resolved=op(executor="python").copy(artifact=Some(file),artifactPath=Some("/operator/example_ops-1.0-py3-none-any.whl"),program=Some(OperatorProgram("copy@1","operators",Some(file))))
    try {
      val grouped=OperatorGroups.prepareResolved(c,plan,Map("a"->resolved,"b"->resolved)).toOption.get
      assert(grouped.operator.command.mounts.keySet==Set("/operator/example_ops-1.0-py3-none-any.whl"))
    }finally {c.close();java.nio.file.Files.delete(file)}
  }
  test("content-addressed code keeps its declared Python filename when mounted") {
    val c=config;val file=java.nio.file.Files.createTempFile("sha256-", "")
    java.nio.file.Files.writeString(file,"def process(ctx):\n    pass\n")
    val program=OperatorProgram("copy@1","operators",Some(file))
    val resolved=op(executor="python").copy(artifact=Some(file),artifactPath=Some("/operator/operators.py"),program=Some(program))
    try {
      val grouped=OperatorGroups.prepareResolved(c,plan,Map("a"->resolved,"b"->resolved)).toOption.get
      assert(grouped.operator.command.mounts.keySet==Set("/operator/operators.py"))
      assert(grouped.inputs.keySet==Set("i0"))
      assert(grouped.outputs==Map("o0"->("b","result")))
    }finally{c.close();java.nio.file.Files.delete(file)}
  }

}
