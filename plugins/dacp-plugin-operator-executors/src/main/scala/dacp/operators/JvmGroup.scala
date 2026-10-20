package dacp.operators

import dacp._
import dacp.operator.{Operator,OperatorRunner}
import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import java.io.{ByteArrayOutputStream,DataInputStream}
import java.nio.file.{Files,Path}
import java.util.jar.JarFile
import java.util.zip.{ZipEntry,ZipOutputStream}
import scala.jdk.CollectionConverters._

/** Eligibility inspects class metadata only. User classes never load in the server JVM. */
private[operators] object JvmGroup {
  private def parents(bytes:Array[Byte]):Vector[String]={
    val in=new DataInputStream(new java.io.ByteArrayInputStream(bytes));require(in.readInt()==0xcafebabe)
    in.readUnsignedShort();in.readUnsignedShort();val cp=new Array[Any](in.readUnsignedShort());var i=1
    while(i<cp.length){in.readUnsignedByte() match {
      case 1=>cp(i)=in.readUTF()
      case 7=>cp(i)=in.readUnsignedShort()
      case 3|4=>in.skipBytes(4)
      case 5|6=>in.skipBytes(8);i+=1
      case 8|16|19|20=>in.skipBytes(2)
      case 9|10|11|12|17|18=>in.skipBytes(4)
      case 15=>in.skipBytes(3)
      case _=>throw new IllegalArgumentException("unsupported class constant")
    };i+=1}
    def name(index:Int):String=if(index==0)""else cp(cp(index).asInstanceOf[Int]).asInstanceOf[String]
    in.readUnsignedShort();in.readUnsignedShort();val parent=name(in.readUnsignedShort())
    Vector(parent)++Vector.fill(in.readUnsignedShort())(name(in.readUnsignedShort()))
  }
  def eligible(p:OperatorProgram):Boolean=p.artifact.exists { path=>
    val jar=new JarFile(path.toFile)
    try {
      def sdk(name:String,seen:Set[String]):Boolean={
        if(Set("dacp/operator/Operator","dacp/operator/BytesOperator","dacp/operator/RecordsOperator","dacp/operator/ByteStreamOperator")(name))true
        else if(seen(name))false
        else Option(jar.getJarEntry(name+".class")).filter(_.getSize<=16*1024*1024).exists { e=>val in=jar.getInputStream(e);val b=try in.readNBytes(16*1024*1024+1)finally in.close();require(b.length<=16*1024*1024,"class metadata too large");parents(b).exists(sdk(_,seen+name)) }
      }
      val name=p.entrypoint.replace('.','/');sdk(name+"$",Set.empty)||sdk(name,Set.empty)
    }finally jar.close()
  }
  lazy val worker:Array[Byte]={
    val bytes=new ByteArrayOutputStream();val zip=new ZipOutputStream(bytes);val seen=scala.collection.mutable.Set.empty[String]
    def add(name:String,data: =>Array[Byte]):Unit=if(name.endsWith(".class") && !seen(name)){seen+=name;zip.putNextEntry(new ZipEntry(name));zip.write(data);zip.closeEntry()}
    Vector(classOf[JvmGroupWorker],classOf[Operator],classOf[OperatorFrame]).map(c=>Path.of(c.getProtectionDomain.getCodeSource.getLocation.toURI)).distinct.foreach { path=>
      if(Files.isDirectory(path)){val walk=Files.walk(path);try walk.iterator.asScala.filter(Files.isRegularFile(_)).foreach(f=>add(path.relativize(f).toString,Files.readAllBytes(f)))finally walk.close()}
      else {val jar=new JarFile(path.toFile);try jar.entries.asScala.filterNot(_.isDirectory).foreach(e=>add(e.getName,{val in=jar.getInputStream(e);try in.readAllBytes()finally in.close()}))finally jar.close()}
    };zip.close();bytes.toByteArray
  }
  def definition(group:OperatorGroup):Array[Byte]={
    val mapper=new ObjectMapper();val doc=mapper.createObjectNode();val steps=doc.putArray("steps")
    group.steps.foreach { s=>
      val n=steps.addObject().put("id",s.id).put("entrypoint",s.program.entrypoint).put("artifact",s.artifactPath.get)
      n.set[JsonNode]("params",mapper.valueToTree(s.params))
      val bindings=n.putObject("bindings");s.bindings.foreach{case(port,b)=>val v=bindings.putObject(port).put("port",b.port);b.task.foreach(v.put("task",_))}
      def contracts(key:String,cs:Map[String,OperatorGroupContract]):Unit={val all=n.putObject(key);cs.foreach{case(port,c)=>val v=all.putObject(port);val r=v.putArray("representations");c.representations.toVector.sorted.foreach(r.add);val f=v.putArray("formats");c.formats.toVector.sorted.foreach(f.add);c.schema.foreach(v.put("schema",_))}}
      contracts("inputs",s.inputs);contracts("outputs",s.outputs)
    }
    val outputs=doc.putObject("outputs");group.outputs.foreach{case(port,(task,source))=>outputs.putObject(port).put("task",task).put("port",source)}
    mapper.writeValueAsBytes(doc)
  }
  def command(group:OperatorGroup):Option[OperatorCommand]={
    if(!group.steps.forall(s=>s.program.configuration.isEmpty && eligible(s.program)))return None
    Some(OperatorCommand(Some(Vector("/opt/java/bin/java","--add-opens=java.base/java.nio=ALL-UNNAMED","-Dio.netty.tryReflectionSetAccessible=true","-cp","/input/group-worker.jar:/opt/lib/*","dacp.operators.JvmGroupWorker")),
      arguments=Vector("/input"),files=Map("group-worker.jar"->worker,"group.json"->definition(group)),
      mounts=group.steps.flatMap(s=>s.program.artifact.zip(s.artifactPath).map{case(file,path)=>path->file}).toMap))
  }
}

/** Marker class also locates the packaged worker without loading any operator classes. */
final class JvmGroupWorker
object JvmGroupWorker {
  def main(args:Array[String]):Unit={
    try {new JvmGroupExecution(Path.of(args(0))).run();System.exit(0)}
    catch {case t:Throwable=>t.printStackTrace(System.err);System.exit(1)}
  }
}
