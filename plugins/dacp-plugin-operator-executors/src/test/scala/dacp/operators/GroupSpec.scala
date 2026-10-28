package dacp.operators

import dacp._
import dacp.operator.{BytesOperator,RecordsOperator}
import com.fasterxml.jackson.databind.JsonNode
import java.io.{ByteArrayInputStream,ByteArrayOutputStream}
import java.nio.file.{Files,Path}
import java.util.zip.{ZipOutputStream,ZipEntry}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.scalatest.funsuite.AnyFunSuite

object GroupUpper extends BytesOperator {
  override def outputFormat="text"
  def transform(b:Array[Byte],p:JsonNode):Array[Byte]=new String(b,java.nio.charset.StandardCharsets.UTF_8).toUpperCase(java.util.Locale.ROOT).getBytes(java.nio.charset.StandardCharsets.UTF_8)
}
object GroupCopy extends BytesOperator {override def outputFormat="text";def transform(b:Array[Byte],p:JsonNode)=b}
object GroupRecords extends RecordsOperator {
  override def previewOutputs(inputs:Map[String,dacp.StreamDescriptor],params:JsonNode,limits:dacp.PreviewLimits)=inputs.get("source").flatMap(_.sample).map(s=>Map("result"->s.bounded(limits))).getOrElse(Map.empty)
  def transform(batch:VectorSchemaRoot,p:JsonNode):Option[VectorSchemaRoot]=Some(batch)
}
class GroupSpec extends AnyFunSuite {
  private def artifact(dir:Path):Path={
    val path=dir.resolve("operators.jar");val zip=new ZipOutputStream(Files.newOutputStream(path))
    try Vector("GroupUpper","GroupUpper$","GroupCopy","GroupCopy$","GroupRecords","GroupRecords$").foreach { n=>
      val name="dacp/operators/"+n+".class";zip.putNextEntry(new ZipEntry(name));val in=getClass.getClassLoader.getResourceAsStream(name);try zip.write(in.readAllBytes())finally in.close();zip.closeEntry()
    }finally zip.close();path
  }
  private def group(jar:Path):OperatorGroup={
    val contract=OperatorGroupContract(Set("bytes"),Set("text"));val params=new java.util.LinkedHashMap[String,Object]()
    def step(id:String,entry:String,b:OperatorGroupBinding)=OperatorGroupStep(id,OperatorProgram(entry+"@1","dacp.operators."+entry,Some(jar)),Some(jar.toString),params,Map("source"->b),Map("source"->contract),Map("result"->contract))
    OperatorGroup(Vector(step("a","GroupUpper",OperatorGroupBinding(None,"i0")),step("b","GroupCopy",OperatorGroupBinding(Some("a"),"result"))),Map("o0"->("b","result")))
  }
  test("same-JVM SDK steps stream more than the bounded link capacity without losing chunks") {
    val dir=Files.createTempDirectory("group-test-");val jar=artifact(dir);val model=group(jar)
    assert(JvmGroup.eligible(model.steps.head.program))
    val command=new JvmExecutor().groupCommand(model).get
    command.files.foreach{case(n,b)=>Files.write(dir.resolve(n),b)}
    assert(command.mounts.size==1)
    val data=Array.fill[Byte](700000)('a'.toByte)
    val allocator=new RootAllocator();val root=VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
    val writer=new ArrowStreamWriter(root,null,Files.newOutputStream(dir.resolve("0.arrow")))
    try{writer.start();data.grouped(65536).foreach { chunk=>root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,chunk);root.setRowCount(1);writer.writeBatch() };writer.end()}finally{writer.close();root.close();allocator.close()}
    Files.writeString(dir.resolve("manifest.json"),"""[{"port":"i0","file":"0.arrow","representation":"bytes","format":"text"}]""")
    val out=new ByteArrayOutputStream()
    val pool=java.util.concurrent.Executors.newSingleThreadExecutor()
    try pool.submit(new Runnable{def run():Unit=new JvmGroupExecution(dir,out).run()}).get(15,java.util.concurrent.TimeUnit.SECONDS)finally pool.shutdownNow()
    val raw=out.toByteArray
    val eventsIn=new ByteArrayInputStream(raw);var event=OperatorFrames.read(eventsIn);var pids=Set.empty[Long]
    while(event.nonEmpty){val h=new com.fasterxml.jackson.databind.ObjectMapper().readTree(event.get.metadata);if(h.path("type").asText=="task_state")pids+=h.path("runtime_pid").asLong;event=OperatorFrames.read(eventsIn)}
    assert(pids.size==1)
    val in=new ByteArrayInputStream(raw);val payload=new ByteArrayOutputStream();var events=Vector.empty[String];var next=OperatorFrames.read(in)
    while(next.nonEmpty){val f=next.get;val h=new com.fasterxml.jackson.databind.ObjectMapper().readTree(f.metadata);if(!h.path("type").asText.startsWith("task_")){events=events:+h.path("type").asText;assert(h.path("port").asText=="o0");if(h.path("type").asText=="data")payload.write(f.data)};next=OperatorFrames.read(in)}
    assert(events.head=="start" && events.last=="end")
    assert(payload.toByteArray.sameElements(Array.fill[Byte](700000)('A'.toByte)))
  }
  test("unrecognized main programs and unsupported executors decline fusion") {
    val dir=Files.createTempDirectory("group-ineligible-");val jar=artifact(dir)
    val model=group(jar);assert(new JvmExecutor().groupCommand(model.copy(steps=model.steps.map(s=>s.copy(program=s.program.copy(entrypoint="unknown.Main"))))).isEmpty)
    assert(new ContainerExecutor().groupCommand(model).isEmpty)
    assert(new dacp.operators.CommandExecutor().groupCommand(model).isEmpty)
  }
  test("Python fusion requires explicit process(ctx) and one code bundle") {
    val dir=Files.createTempDirectory("python-group-eligibility-");val code=dir.resolve("operators.py")
    Files.writeString(code,"def process(ctx):\n    pass\n")
    val base=group(code)
    val model=base.copy(steps=base.steps.map(s=>s.copy(program=s.program.copy(entrypoint="operators",artifact=Some(code)),artifactPath=Some("/operator/operators.py"))))
    val launch=new PythonExecutor().groupCommand(model).get
    assert(launch.files.keySet==Set("python-group.py","group-sdk.py","inference.py","group.json"))
    Files.writeString(code,"def main():\n    pass\n")
    assert(new PythonExecutor().groupCommand(model).isEmpty)
    val other=dir.resolve("other.py");Files.writeString(other,"def process(ctx):\n    pass\n")
    assert(new PythonExecutor().groupCommand(model.copy(steps=model.steps.updated(1,model.steps(1).copy(program=model.steps(1).program.copy(artifact=Some(other)))))).isEmpty)
  }

  test("record links preserve schemas, nulls, batch contents and row metrics") {
    val dir=Files.createTempDirectory("group-records-");val jar=artifact(dir)
    val field=new org.apache.arrow.vector.types.pojo.Field("value",org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32,true)),java.util.Collections.emptyList())
    val schema=new org.apache.arrow.vector.types.pojo.Schema(java.util.Collections.singletonList(field))
    val contract=OperatorGroupContract(Set("records"),Set("arrow"),Some(java.util.Base64.getEncoder.encodeToString(schema.serializeAsMessage())))
    val base=group(jar);val model=base.copy(steps=base.steps.map(s=>s.copy(program=s.program.copy(entrypoint="dacp.operators.GroupRecords"),inputs=Map("source"->contract),outputs=Map("result"->contract))))
    val command=new JvmExecutor().groupCommand(model).get;command.files.foreach{case(n,b)=>Files.write(dir.resolve(n),b)}
    val allocator=new RootAllocator();val root=VectorSchemaRoot.create(schema,allocator);root.allocateNew()
    val vector=root.getVector(0).asInstanceOf[org.apache.arrow.vector.IntVector];vector.setSafe(0,1);vector.setNull(1);vector.setSafe(2,3);root.setRowCount(3)
    val writer=new ArrowStreamWriter(root,null,Files.newOutputStream(dir.resolve("0.arrow")))
    try{writer.start();writer.writeBatch();writer.end()}finally{writer.close();root.close()}
    val meta=new com.fasterxml.jackson.databind.ObjectMapper().createArrayNode();meta.addObject().put("port","i0").put("file","0.arrow").put("representation","records").put("format","arrow").put("schema",java.util.Base64.getEncoder.encodeToString(schema.serializeAsMessage()))
    Files.writeString(dir.resolve("manifest.json"),meta.toString)
    val output=new ByteArrayOutputStream();val pool=java.util.concurrent.Executors.newSingleThreadExecutor()
    try pool.submit(new Runnable{def run():Unit=new JvmGroupExecution(dir,output).run()}).get(15,java.util.concurrent.TimeUnit.SECONDS)finally pool.shutdownNow()
    val in=new ByteArrayInputStream(output.toByteArray);var next=OperatorFrames.read(in);var rows=Vector.empty[Any];var reported=false
    while(next.nonEmpty){val f=next.get;val h=new com.fasterxml.jackson.databind.ObjectMapper().readTree(f.metadata)
      if(h.path("type").asText=="task_metric" && h.path("records").asInt>0)reported=true
      if(h.path("type").asText=="data"){
        val reader=new org.apache.arrow.vector.ipc.ArrowStreamReader(new ByteArrayInputStream(f.data),allocator)
        try{assert(reader.getVectorSchemaRoot.getSchema==schema);assert(reader.loadNextBatch());val v=reader.getVectorSchemaRoot.getVector(0);rows++=(0 until 3).map(i=>v.getObject(i))}finally reader.close()
      };next=OperatorFrames.read(in)
    }
    assert(rows==Vector(1,null,3));assert(reported);allocator.close()
  }

}
