package dacp

import java.nio.file.{Files,Path}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class MaterializationSpec extends AnyFunSuite {
  private def temp(f:Path=>Unit):Unit={val p=Files.createTempDirectory("materialization-contract-");try f(p)finally {val s=Files.walk(p);try s.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally s.close()}}
  private def meta=Data.mapper.createObjectNode().put("owner","owner")
  test("plugin discovery, atomic multiport commit, abort and restart with opaque bytes") {temp { p=>
    val env=new PluginEnvironment();val store=env.byId(classOf[MaterializationStoreProvider],"file").open(Map("directory"->p.toString))
    val w=store.begin("task",meta,Set("left","right"))
    w.append("left",Array[Byte](1,2));assert(store.get("task").isEmpty)
    w.append("right",Array[Byte](3,4));w.commit();w.close()
    val aborted=store.begin("abort",meta,Set("result"));aborted.append("result",Array[Byte](9));aborted.close();assert(store.get("abort").isEmpty)
    store.close();env.close()
    val reopened=new FileMaterializationStore(p,100000,100000,60)
    try {val a=reopened.get("task").get;try {val r=a.open("left");try {assert(r.next().get.sameElements(Array[Byte](1,2)));assert(r.next().isEmpty)}finally r.close();assert(a.metadata.path("stored_ports").size()==2)}finally a.close()}finally reopened.close()
  }}
  test("quota rejects incomplete output, expired artifacts disappear, leased artifacts are retained") {temp { p=>
    val store=new FileMaterializationStore(p,10000,1000,1)
    try {
      val tooBig=store.begin("large",meta,Set("result"));intercept[IllegalArgumentException](tooBig.append("result",Array.fill[Byte](1100)(1)));tooBig.close();assert(store.get("large").isEmpty)
      val w=store.begin("small",meta,Set("result"));w.append("result",Array[Byte](1));w.commit();w.close();val lease=store.get("small").get
      Thread.sleep(1100);store.cleanup();assert(Files.exists(p.resolve("small")));assert(store.get("small").isEmpty)
      lease.close();store.cleanup();assert(!Files.exists(p.resolve("small")))
    }finally store.close()
  }}
  test("truncated and corrupted chunks cannot reach a successful replay EOF") {temp { p=>
    val store=new FileMaterializationStore(p,10000,10000,60)
    try {
      val w=store.begin("bad",meta,Set("result"));w.append("result",Array[Byte](1,2));w.commit();w.close()
      val file=p.resolve("bad/result.bin");val bytes=Files.readAllBytes(file);bytes(bytes.length-1)=42;Files.write(file,bytes)
      val a=store.get("bad").get;try {val r=a.open("result");try {assert(r.next().nonEmpty);intercept[IllegalArgumentException](r.next())}finally r.close()}finally a.close()
      Files.write(file,Array[Byte](0,0,0,8,1));val b=store.get("bad").get;try {val r=b.open("result");try intercept[IllegalArgumentException](r.next())finally r.close()}finally b.close()
    }finally store.close()
  }}
}
