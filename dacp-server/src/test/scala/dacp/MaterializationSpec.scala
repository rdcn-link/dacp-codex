package dacp

import _root_.xstream.data._
import java.nio.file.{Files,Path}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class MaterializationSpec extends AnyFunSuite {
  private def temp(f:Path=>Unit):Unit={val p=Files.createTempDirectory("materialization-contract-");try f(p)finally {val s=Files.walk(p);try s.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally s.close()}}
  private def meta=Data.mapper.createObjectNode().put("owner","owner")
  private def append(w:MaterializationWriter,port:String,bytes:Array[Byte]):Unit={
    w.append(port,StreamProtocol.start(port,"bytes","binary"))
    w.append(port,StreamProtocol.frame("data",port,bytes))
    w.append(port,StreamProtocol.frame("end",port))
  }
  test("plugin discovery, atomic multiport Arrow File commit, abort and restart") {temp { p=>
    val env=new PluginEnvironment();val store=env.byId(classOf[MaterializationStoreProvider],"file").open(Map("directory"->p.toString))
    val w=store.begin("task",meta,Set("left","right"))
    append(w,"left",Array[Byte](1,2));assert(store.get("task").isEmpty)
    intercept[IllegalArgumentException](w.commit())
    append(w,"right",Array[Byte](3,4));w.commit();w.close()
    val aborted=store.begin("abort",meta,Set("result"));aborted.append("result",StreamProtocol.start("result","bytes","binary"));aborted.close();assert(store.get("abort").isEmpty)
    val segments=Files.list(p.resolve("task"));try {
      val files=segments.iterator.asScala.filter(_.toString.endsWith(".arrow")).toVector
      assert(files.size==4)
      files.foreach(f=>assert(new String(Files.readAllBytes(f).take(6),java.nio.charset.StandardCharsets.US_ASCII)=="ARROW1"))
    }finally segments.close()
    store.close();env.close()
    val reopened=new FileMaterializationStore(p,100000,100000,60)
    try {val a=reopened.get("task").get;try {
      val r=a.open("left");try {
        assert(StreamProtocol.header(r.next().get).path("type").asText=="start")
        assert(r.next().get.data.sameElements(Array[Byte](1,2)))
        assert(StreamProtocol.header(r.next().get).path("type").asText=="end")
        assert(r.next().isEmpty)
      }finally r.close()
      assert(a.metadata.path("stored_ports").size()==2 && a.metadata.path("complete").asBoolean)
    }finally a.close()}finally reopened.close()
  }}
  test("quota rejects incomplete output, expired artifacts disappear, leased artifacts are retained") {temp { p=>
    val store=new FileMaterializationStore(p,30000,10000,1)
    try {
      val tooBig=store.begin("large",meta,Set("result"));tooBig.append("result",StreamProtocol.start("result","bytes","binary"))
      intercept[IllegalArgumentException](tooBig.append("result",StreamProtocol.frame("data","result",Array.fill[Byte](11000)(1))));intercept[IllegalArgumentException](tooBig.append("result",StreamProtocol.frame("end","result")));intercept[IllegalArgumentException](tooBig.commit());tooBig.close();assert(store.get("large").isEmpty)
      val w=store.begin("small",meta,Set("result"));append(w,"result",Array[Byte](1));w.commit();w.close();val lease=store.get("small").get
      Thread.sleep(1100);store.cleanup();assert(Files.exists(p.resolve("small")));assert(store.get("small").isEmpty)
      lease.close();store.cleanup();assert(!Files.exists(p.resolve("small")))
    }finally store.close()
  }}
  test("corrupted or truncated Arrow segments fail before exposing their data") {temp { p=>
    val store=new FileMaterializationStore(p,100000,100000,60)
    try {
      val w=store.begin("bad",meta,Set("result"));append(w,"result",Array[Byte](1,2));w.commit();w.close()
      val file=p.resolve("bad/result-00000001.arrow");val bytes=Files.readAllBytes(file);bytes(bytes.length-1)=42;Files.write(file,bytes)
      val a=store.get("bad").get;try {val r=a.open("result");try {assert(r.next().nonEmpty);intercept[IllegalArgumentException](r.next())}finally r.close()}finally a.close()
      Files.write(file,Array[Byte](0,0,0,8,1));val b=store.get("bad").get;try {val r=b.open("result");try {assert(r.next().nonEmpty);intercept[IllegalArgumentException](r.next())}finally r.close()}finally b.close()
    }finally store.close()
  }}
  test("typed records and dictionaries survive materialization and replay") {temp { p=>
    val a=new org.apache.arrow.memory.RootAllocator()
    val encoding=new org.apache.arrow.vector.types.pojo.DictionaryEncoding(9L,false,new org.apache.arrow.vector.types.pojo.ArrowType.Int(32,true))
    val field=new org.apache.arrow.vector.types.pojo.Field("station",new org.apache.arrow.vector.types.pojo.FieldType(true,new org.apache.arrow.vector.types.pojo.ArrowType.Int(32,true),encoding),null)
    val schema=new org.apache.arrow.vector.types.pojo.Schema(Seq(field).asJava)
    val root=org.apache.arrow.vector.VectorSchemaRoot.create(schema,a)
    val values=new org.apache.arrow.vector.VarCharVector("values",a)
    val dictionaries=new org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider(new org.apache.arrow.vector.dictionary.Dictionary(values,encoding))
    val store=new FileMaterializationStore(p,100000,100000,60)
    try {
      values.allocateNew();values.setSafe(0,"station-A".getBytes("UTF-8"));values.setValueCount(1)
      root.allocateNew();root.getVector(0).asInstanceOf[org.apache.arrow.vector.IntVector].setSafe(0,0);root.setRowCount(1)
      val w=store.begin("records",meta,Set("result"))
      try {
        w.append("result",StreamProtocol.start("result","records","arrow",Some(schema),dictionaries=dictionaries))
        w.append("result",StreamProtocol.frame("data","result",StreamProtocol.batch(root,dictionaries)))
        w.append("result",StreamProtocol.frame("end","result"));w.commit()
      } finally w.close()
      val task=store.get("records").get
      try {val frames=task.open("result");try {
        val start=frames.next().get
        val initial=new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(start.data),a)
        try {assert(!initial.loadNextBatch());assert(initial.lookup(9L).getVector.getObject(0).toString=="station-A")}finally initial.close()
        val data=frames.next().get
        val reader=new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(data.data),a)
        try {assert(reader.loadNextBatch());assert(reader.getVectorSchemaRoot.getVector(0).getObject(0)==Integer.valueOf(0));assert(reader.lookup(9L).getVector.getObject(0).toString=="station-A");assert(!reader.loadNextBatch())}finally reader.close()
        assert(StreamProtocol.header(frames.next().get).path("type").asText=="end")
      }finally frames.close()}finally task.close()
    } finally {store.close();root.close();dictionaries.close();assert(a.getAllocatedMemory==0);a.close()}
  }}
  test("scientific array segments retain dimensions and typed values on replay") {temp { p=>
    val a=new org.apache.arrow.memory.RootAllocator()
    val descriptor=ArrayDescriptor(Vector(Dimension("x",2)),NumericEncoding("float64",java.nio.ByteOrder.LITTLE_ENDIAN))
    val view=DataViews.blocks[ArrayChunk,ArraySelection]((_,_,out)=>{
      out.start(descriptor)
      val values=java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);values.putDouble(1.5).putDouble(2.5).flip()
      out.accept(ArrayChunk(ArraySection(Vector(0),Vector(2),Vector(1)),descriptor.encoding,values))
    })
    val store=new FileMaterializationStore(p,100000,100000,60)
    try {
      val w=store.begin("array",meta,Set("result"))
      try {
        view.open(ArraySelection(),StreamContext(a)).consume(ItemFrameStreams.encoded("netcdf",f=>w.append("result",StreamProtocol.rename(f,"result"))))
        w.append("result",StreamProtocol.frame("end","result"));w.commit()
      }finally w.close()
      val task=store.get("array").get
      try {val frames=task.open("result");try {
        assert(StreamProtocol.header(frames.next().get).path("structure").path("view").asText=="array")
        val frame=frames.next().get;var count=0
        ArrowBlocks.consume(ArrowIpc.readStream(new java.io.ByteArrayInputStream(frame.data),StreamContext(a)),new DataConsumer[ArrayChunk]{
          def start(d:DataDescriptor[ArrayChunk]):Unit=assert(d==descriptor)
          def accept(b:ArrayChunk):Unit={count+=1;assert(b.section.shape==Vector(2));assert(b.values.duplicate().order(b.encoding.byteOrder).getDouble()==1.5)}
        });assert(count==1)
      }finally frames.close()}finally task.close()
    }finally {store.close();assert(a.getAllocatedMemory==0);a.close()}
  }}
  test("empty outputs preserve schema and require a successful end before publication") {temp { p=>
    val store=new FileMaterializationStore(p,100000,100000,60)
    try {
      val w=store.begin("empty",meta,Set("result"));w.append("result",StreamProtocol.start("result","bytes","binary"))
      intercept[IllegalArgumentException](w.commit());w.append("result",StreamProtocol.frame("end","result"));w.commit();w.close()
      val task=store.get("empty").get
      try {val r=task.open("result");try {assert(StreamProtocol.header(r.next().get).path("type").asText=="start");assert(StreamProtocol.header(r.next().get).path("type").asText=="end");assert(r.next().isEmpty)}finally r.close()}finally task.close()
    }finally store.close()
  }}
}
