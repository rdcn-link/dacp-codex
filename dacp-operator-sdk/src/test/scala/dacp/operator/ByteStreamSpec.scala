package dacp.operator

import java.io.{ByteArrayInputStream,ByteArrayOutputStream,InputStream,OutputStream,IOException}
import java.nio.file.{Files,Path}
import java.util.zip.{GZIPInputStream,GZIPOutputStream}
import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector,VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.scalatest.funsuite.AnyFunSuite
import dacp.{OperatorFrame,OperatorFrames,PluginSchemas}

class ByteStreamSpec extends AnyFunSuite {
  private def fixture(chunks:Seq[Array[Byte]])(run:(Array[String],ByteArrayOutputStream)=>Unit):Unit={
    val dir=Files.createTempDirectory("dacp-byte-stream-")
    val allocator=new RootAllocator()
    val root=VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
    try {
      val raw=Files.newOutputStream(dir.resolve("0.arrow"))
      val writer=new ArrowStreamWriter(root,null,raw)
      try {
        writer.start()
        for(chunk<-chunks){
          root.clear();val vector=root.getVector(0).asInstanceOf[VarBinaryVector]
          vector.allocateNew();vector.setSafe(0,chunk);root.setRowCount(1);writer.writeBatch()
        }
        writer.end()
      }finally {writer.close();raw.close()}
      Files.writeString(dir.resolve("manifest.json"),"""[{"port":"source","file":"0.arrow","representation":"bytes","format":"binary"}]""")
      Files.writeString(dir.resolve("params.json"),"""{"suffix":"!"}""")
      run(Array("operator",dir.toString,dir.resolve("params.json").toString),new ByteArrayOutputStream())
    }finally {
      root.close();allocator.close()
      Seq("0.arrow","manifest.json","params.json").foreach(n=>Files.deleteIfExists(dir.resolve(n)));Files.delete(dir)
    }
  }
  private def frames(out:ByteArrayOutputStream):Vector[OperatorFrame]={
    val in=new ByteArrayInputStream(out.toByteArray)
    Iterator.continually(OperatorFrames.read(in)).takeWhile(_.nonEmpty).map(_.get).toVector
  }
  private def payload(out:ByteArrayOutputStream):Array[Byte]=frames(out).flatMap(_.data).toArray
  private def ends(out:ByteArrayOutputStream):Boolean=frames(out).exists(f=>new ObjectMapper().readTree(f.metadata).path("type").asText=="end")

  test("InputStream traverses empty chunks and batch boundaries; output flushes before end") {
    val input=Array.tabulate[Byte](200000)(i=>(i%251).toByte)
    fixture(Seq(Array.emptyByteArray,input.take(1),input.slice(1,70000),Array.emptyByteArray,input.drop(70000))){(args,out)=>
      val op=new ByteStreamOperator {
        def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={
          assert(in.read(new Array[Byte](0))==0)
          target.write(in.read()) // unsigned byte read, then bulk transfer across chunks
          in.transferTo(target)
          assert(in.read()== -1);assert(in.read(new Array[Byte](0))==0)
          target.write(params.path("suffix").asText.getBytes("UTF-8"))
        }
      }
      OperatorRunner.run(args,out)(op.process)
      assert(payload(out).sameElements(input++Array('!'.toByte)))
      assert(frames(out).forall(_.data.length<=65536));assert(ends(out))
    }
  }
  test("large Arrow reads never probe available on non-seekable inputs") {
    val input=Array.tabulate[Byte](300000)(i=>(i%251).toByte)
    fixture(Seq(input)){(args,out)=>
      val raw=new ByteArrayInputStream(Files.readAllBytes(Path.of(args(1)).resolve("0.arrow"))) {
        override def available():Int=throw new IOException("Illegal seek")
      }
      val op=new ByteStreamOperator {
        def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={in.transferTo(target);()}
      }
      val emitted=scala.collection.mutable.ArrayBuffer.empty[OperatorFrame]
      OperatorRunner.runStreams(Path.of(args(1)),Path.of(args(2)),Map("source"->raw),f=>emitted+=f)(op.process)
      assert(emitted.flatMap(_.data).toArray.sameElements(input))
      assert(new ObjectMapper().readTree(emitted.last.metadata).path("type").asText=="end")
    }
  }
  test("standard Java gzip library can close its stream without closing protocol stdout") {
    val input="中文字符跨块也可以压缩".getBytes("UTF-8")
    fixture(input.grouped(2).toSeq){(args,out)=>
      val op=new ByteStreamOperator {
        override def outputFormat:String="gzip"
        def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={
          val gzip=new GZIPOutputStream(target)
          try in.transferTo(gzip)finally gzip.close()
        }
      }
      OperatorRunner.run(args,out)(op.process)
      val gzip=new GZIPInputStream(new ByteArrayInputStream(payload(out)))
      try assert(gzip.readAllBytes().sameElements(input))finally gzip.close()
      assert(ends(out));assert(frames(out).head.metadata.nonEmpty)
    }
  }
  test("empty input produces a valid empty output") {
    fixture(Seq.empty){(args,out)=>
      val op=new ByteStreamOperator {def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={assert(in.read()== -1)}}
      OperatorRunner.run(args,out)(op.process);assert(payload(out).isEmpty);assert(ends(out))
    }
  }
  test("early close and business failure do not emit successful end events") {
    fixture(Seq("input".getBytes("UTF-8"))){(args,out)=>
      val op=new ByteStreamOperator {def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={in.read();in.close()}}
      intercept[IllegalArgumentException]{OperatorRunner.run(args,out)(op.process)}
      assert(!ends(out))
    }
    fixture(Seq("input".getBytes("UTF-8"))){(args,out)=>
      val op=new ByteStreamOperator {def transform(in:InputStream,target:OutputStream,params:JsonNode):Unit={target.write(1);throw new IOException("business failure")}}
      assert(intercept[IOException]{OperatorRunner.run(args,out)(op.process)}.getMessage=="business failure")
      assert(payload(out).isEmpty);assert(!ends(out))
    }
  }
  test("context stream API preserves lexical input and output declaration rules") {
    fixture(Seq("input".getBytes("UTF-8"))){(args,out)=>
      OperatorRunner.run(args,out){ctx=>
        val source=ctx.input("source").byteStream()
        val target=ctx.bytesOutput("result").byteStream()
        target.write(1) // buffered writes must already prevent later declarations
        intercept[IllegalArgumentException]{ctx.bytesOutput("late")}
        source.transferTo(target);source.close()
        intercept[IOException]{source.read()}
        target.close();target.close()
        intercept[IOException]{target.write(1)}
      }
      assert(payload(out).sameElements(Array(1.toByte)++"input".getBytes("UTF-8")));assert(ends(out))
    }
  }
}
