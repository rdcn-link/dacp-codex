package dacp

import _root_.xstream.data._


import dacp.union.Json
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class ExecutionHistorySpec extends AnyFunSuite {
  private def temporary(run:java.nio.file.Path=>Unit):Unit={val dir=Files.createTempDirectory("dacp-execution-history-");try run(dir)finally{val paths=Files.walk(dir);try paths.iterator.asScala.toVector.reverse.foreach(Files.deleteIfExists)finally paths.close()}}
  test("history persists lifecycle, timings, failures and ownership across restarts") { temporary { dir=>
    val history=new ExecutionHistory(new FileExecutionStore(dir,false))
    val record=history.begin("task","first",Json.obj().put("owner","client-a").put("operator","identity@1.3"))
    record.update("running");record.update("failed",Json.obj().put("payload_bytes",12),Some("operator failed"))
    record.update("running") // A late worker cannot overwrite the terminal outcome.
    history.close()
    val restored=new ExecutionHistory(new FileExecutionStore(dir,false))
    try {
      val value=restored.get("first").get
      assert(value.path("state").asText=="failed")
      assert(value.path("events").elements.asScala.map(_.path("state").asText).toVector==Vector("accepted","running","failed"))
      assert(value.path("started_at_ms").asLong>0 && value.path("finished_at_ms").asLong>=value.path("started_at_ms").asLong)
      assert(value.path("message").asText=="operator failed" && value.path("payload_bytes").asInt==12)
      assert(restored.list(owner=Some("client-a")).size==1 && restored.list(owner=Some("client-b")).isEmpty)
      assert(restored.list(offset=1).isEmpty)
    }finally restored.close()
  }}
  test("unfinished work is marked interrupted without being resumed or inventing execution time") { temporary { dir=>
    val history=new ExecutionHistory(new FileExecutionStore(dir,false));val record=history.begin("job","unfinished")
    record.update("running");val last=history.get("unfinished").get.path("updated_at_ms").asLong;history.close()
    val restored=new ExecutionHistory(new FileExecutionStore(dir,false))
    try {val value=restored.get("unfinished").get;assert(value.path("state").asText=="interrupted");assert(value.path("finished_at_ms").asLong==last);assert(value.path("interruption_detected_at_ms").asLong>=last)}finally restored.close()
  }}
  test("cancelled threads persist their final record and retain interruption") { temporary { dir=>
    val history=new ExecutionHistory(new FileExecutionStore(dir,false));val record=history.begin("task","cancelled")
    try {
      Thread.currentThread().interrupt()
      record.update("cancelled",message=Some("client closed results"))
      assert(Thread.currentThread().isInterrupted)
    }finally Thread.interrupted()
    try assert(history.get("cancelled").get.path("state").asText=="cancelled")finally history.close()
  }}
  test("one writer owns the directory; local read-only access remains available") { temporary { dir=>
    val history=new ExecutionHistory(new FileExecutionStore(dir,false))
    try {
      history.begin("task","record");intercept[Exception](new ExecutionHistory(new FileExecutionStore(dir,false)))
      val reader=new ExecutionHistory(new FileExecutionStore(dir,true),recoverInterrupted=false)
      try {assert(reader.get("record").get.path("state").asText=="accepted");intercept[IllegalArgumentException](reader.begin("task"))}finally reader.close()
      intercept[IllegalArgumentException](history.get("../escape"));intercept[IllegalArgumentException](history.list(limit=1001))
    }finally history.close()
  }}
}
