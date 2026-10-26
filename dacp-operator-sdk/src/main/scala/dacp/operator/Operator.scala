package dacp.operator

import java.io.{InputStream,OutputStream,ByteArrayOutputStream,BufferedOutputStream,IOException}
import java.nio.file.{Files,Path}
import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader,ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.Schema
import dacp.{OperatorFrame,OperatorFrames,PluginSchemas,StreamDescriptor,InferenceRequest,InferenceResult,StructureInference}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Business logic only. The runner owns streams, output framing, end events and cleanup. */
trait Operator {
  def inferOutputs(inputs:Map[String,StreamDescriptor],params:JsonNode):InferenceResult
  def process(context:OperatorContext):Unit
  final def main(args:Array[String]):Unit={
    if(args.headOption.exists(Set("infer","infer-worker")))OperatorRunner.infer(args)(inferOutputs)
    else OperatorRunner.run(args)(process)
  }
}
abstract class BytesOperator extends Operator {
  def transform(chunk:Array[Byte],params:JsonNode):Array[Byte]
  def outputFormat:String="binary"
  final def inferOutputs(inputs:Map[String,StreamDescriptor],params:JsonNode):InferenceResult={
    require(inputs.keySet==Set("source"),"expected source input")
    require(inputs("source").representations.isEmpty || inputs("source").representations("bytes"),"bytes input required")
    InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set(outputFormat),certainty="known")))
  }
  final def process(ctx:OperatorContext):Unit={
    val out=ctx.bytesOutput("result",outputFormat)
    ctx.input("source").foreachBytes(chunk=>out.write(transform(chunk,ctx.params)))
  }
}
/** Byte-oriented libraries can read across Arrow chunk boundaries without whole-item buffering. */
abstract class ByteStreamOperator extends Operator {
  def transform(input:InputStream,output:OutputStream,params:JsonNode):Unit
  def outputFormat:String="binary"
  final def inferOutputs(inputs:Map[String,StreamDescriptor],params:JsonNode):InferenceResult={
    require(inputs.keySet==Set("source"),"expected source input")
    require(inputs("source").representations.isEmpty || inputs("source").representations("bytes"),"bytes input required")
    InferenceResult(Map("result"->StreamDescriptor(Set("bytes"),Set(outputFormat),certainty="known")))
  }
  final def process(ctx:OperatorContext):Unit={
    val output=ctx.bytesOutput("result",outputFormat).byteStream()
    val input=ctx.input("source").byteStream()
    try transform(input,output,ctx.params)finally input.close()
  }
}
abstract class RecordsOperator extends Operator {
  def outputSchema(input:Schema,params:JsonNode):Schema=input
  final def inferOutputs(inputs:Map[String,StreamDescriptor],params:JsonNode):InferenceResult={
    require(inputs.keySet==Set("source"),"expected source input")
    val input=inputs("source");require(input.representations.isEmpty || input.representations("records"),"records input required")
    val schema=input.schema.map(outputSchema(_,params))
    InferenceResult(Map("result"->StreamDescriptor(Set("records"),Set("arrow"),schema,if(schema.nonEmpty)"known"else "unknown")))
  }
  /** Returned roots are borrowed; the SDK serializes synchronously and does not close them. */
  def transform(batch:VectorSchemaRoot,params:JsonNode):Option[VectorSchemaRoot]
  final def process(ctx:OperatorContext):Unit={
    val in=ctx.input("source")
    val out=ctx.recordsOutput("result",outputSchema(in.schema,ctx.params))
    in.foreachRecords(root=>transform(root,ctx.params).foreach(out.write))
  }
}
object OperatorRunner {
  def infer(args:Array[String],out:OutputStream=System.out)(method:(Map[String,StreamDescriptor],JsonNode)=>InferenceResult):Unit={
    if(args.headOption.contains("infer-worker")){
      require(args.length==3,"expected infer-worker REQUEST_FIFO RESPONSE_FIFO")
      val input=new java.io.DataInputStream(Files.newInputStream(Path.of(args(1))))
      try {
        val output=new java.io.DataOutputStream(Files.newOutputStream(Path.of(args(2))))
        try inferWorker(input,output)(method)finally output.close()
      }finally input.close()
      return
    }
    require(args.length==2 && args(0)=="infer","expected infer REQUEST_JSON")
    val path=Path.of(args(1));require(Files.size(path)<=StructureInference.MaxBytes,"inference request too large")
    val request=StructureInference.request(StructureInference.mapper.readTree(Files.readAllBytes(path)))
    val result=Console.withOut(System.err){method(request.inputs,request.params)}
    val bytes=StructureInference.mapper.writeValueAsBytes(StructureInference.json(result))
    require(bytes.length<=StructureInference.MaxBytes,"inference result too large");out.write(bytes);out.flush()
  }
  /** Length-prefixed requests are bounded independently; no data streams or result cache. */
  def inferWorker(in:java.io.DataInputStream,out:java.io.DataOutputStream)(method:(Map[String,StreamDescriptor],JsonNode)=>InferenceResult):Unit={
    out.writeInt(0);out.flush()
    var running=true
    while(running){
      // A partial frame is an error; only EOF between requests is a clean shutdown.
      val first=in.read()
      if(first<0)running=false else {
        val length=(first<<24)|(in.readUnsignedByte()<<16)|(in.readUnsignedByte()<<8)|in.readUnsignedByte()
        require(length>0 && length<=StructureInference.MaxBytes,"inference request exceeds limits")
        val raw=new Array[Byte](length);in.readFully(raw)
        val request=StructureInference.request(StructureInference.mapper.readTree(raw))
        val result=Console.withOut(System.err){method(request.inputs,request.params)}
        val response=StructureInference.mapper.writeValueAsBytes(StructureInference.json(result))
        require(response.length<=StructureInference.MaxBytes,"inference result too large")
        out.writeInt(response.length);out.write(response);out.flush()
      }
    }
  }
  def runStreams(directory:Path,params:Path,inputs:Map[String,InputStream],emit:OperatorFrame=>Unit)(process:OperatorContext=>Unit):Unit={
    val ctx=new OperatorContext(directory,params,OutputStream.nullOutputStream(),inputs,Some(emit))
    try {Console.withOut(System.err){process(ctx)};ctx.finish()}finally ctx.close()
  }
  def run(args:Array[String],out:OutputStream=System.out)(process:OperatorContext=>Unit):Unit={
    require(args.length==3 && args(0)=="operator","expected operator INPUT PARAMS")
    val ctx=new OperatorContext(Path.of(args(1)),Path.of(args(2)),out)
    try {Console.withOut(System.err){process(ctx)};ctx.finish()}finally ctx.close()
  }
}
final class OperatorContext(directory:Path,paramsFile:Path,out:OutputStream,
                            inputStreams:Map[String,InputStream]=Map.empty,
                            emit:Option[OperatorFrame=>Unit]=None) extends AutoCloseable {
  private val mapper=new ObjectMapper()
  val params:JsonNode=mapper.readTree(Files.readAllBytes(paramsFile))
  require(params!=null && params.isObject,"params must be a JSON object")
  private val manifest=mapper.readTree(Files.readAllBytes(directory.resolve("manifest.json")))
  require(manifest!=null && manifest.isArray,"manifest must be an array")
  private val entries=manifest.elements.asScala.toVector
  private def validName(s:String):Boolean=s.matches("[A-Za-z_][A-Za-z0-9_-]{0,127}")
  require(entries.forall(e=>validName(e.path("port").asText)) && entries.map(_.path("port").asText).distinct.size==entries.size,"invalid or duplicate input port")
  private val allocator=new RootAllocator()
  private val names=entries.map(_.path("port").asText).sorted
  private var index=0
  private var writing=false
  private var finished=false
  private var closed=false
  private val opened=mutable.Map.empty[String,Input]
  private val outputs=mutable.LinkedHashMap.empty[String,Output]
  def inputNames:Vector[String]=names
  private def active():Unit={require(!closed && !finished,"operator context ended");if(Thread.currentThread().isInterrupted)throw new java.util.concurrent.CancellationException()}
  private def frame(kind:String,port:String,data:Array[Byte]=Array.emptyByteArray,repr:String="",format:String="",schema:Option[Schema]=None):Unit={
    active();val h=mapper.createObjectNode().put("type",kind).put("port",port)
    if(kind=="start") {h.put("representation",repr).put("format",format);schema.foreach(s=>h.put("schema",java.util.Base64.getEncoder.encodeToString(s.serializeAsMessage())))}
    val event=OperatorFrame(mapper.writeValueAsBytes(h),data)
    emit match {case Some(send)=>send(event);case None=>OperatorFrames.write(out,event)}
  }
  def input(name:String):Input={active();require(names.contains(name),"unknown input port");opened.getOrElseUpdate(name,new Input(name,entries.find(_.path("port").asText==name).get))}
  private def output(name:String,repr:String,format:String,schema:Option[Schema]):Output={
    active();require(validName(name) && !outputs.contains(name),"invalid or duplicate output port")
    require(!writing,"declare all outputs before writing data")
    val value=new Output(name,repr,schema);frame("start",name,repr=repr,format=format,schema=schema);outputs(name)=value;value
  }
  def bytesOutput(name:String,format:String="binary"):Output=output(name,"bytes",format,None)
  def recordsOutput(name:String,schema:Schema,format:String="arrow"):Output=output(name,"records",format,Some(schema))
  final class Input private[OperatorContext](val name:String,entry:JsonNode) {
    val representation:String=entry.path("representation").asText
    val format:String=entry.path("format").asText
    require(Set("bytes","records")(representation),"unsupported input representation")
    private var reader:ArrowStreamReader=null
    private var consumed=false
    private var iterating=false
    private def open():ArrowStreamReader={
      active();require(index<names.size && names(index)==name && !consumed && !iterating,"consume inputs once in lexical port order")
      if(reader==null){
        val relative=Path.of(entry.path("file").asText)
        require(!relative.isAbsolute && relative.getNameCount==1 && relative.toString!=".." && relative.toString!=".","invalid input filename")
        // Arrow's Channels adapter probes available() for large reads. JDK file
        // channels implement that probe with seek(), which is invalid for FIFOs.
        val raw=new java.io.FilterInputStream(inputStreams.getOrElse(name,Files.newInputStream(directory.resolve(relative)))) {
          override def available():Int=0
        }
        try {reader=new ArrowStreamReader(raw,allocator);reader.getVectorSchemaRoot}catch{case t:Throwable=>if(reader!=null)reader.close()else raw.close();throw t}
        if(representation=="bytes")require(reader.getVectorSchemaRoot.getSchema==PluginSchemas.bytes,"invalid bytes schema")
      };reader
    }
    def schema:Schema=open().getVectorSchemaRoot.getSchema
    private def foreachBatch(f:VectorSchemaRoot=>Unit):Unit={
      val r=open()
      iterating=true
      try {while({active();r.loadNextBatch()})f(r.getVectorSchemaRoot);consumed=true;index+=1}finally {r.close();reader=null}
    }
    /** Read bytes continuously across IPC batches. Closing early does not consume the input. */
    def byteStream():InputStream={
      require(representation=="bytes","bytes input required")
      val r=open();iterating=true
      new InputStream {
        private var chunk=Array.emptyByteArray
        private var position=0
        private var row=0
        private var eof=false
        private var streamClosed=false
        private def readable():Unit={if(streamClosed)throw new IOException("input stream closed");active()}
        private def advance():Boolean={
          readable()
          while(!eof && position==chunk.length){
            val root=r.getVectorSchemaRoot
            if(row<root.getRowCount){
              val vector=root.getVector(0).asInstanceOf[VarBinaryVector]
              require(!vector.isNull(row),"null bytes chunk")
              chunk=vector.get(row);row+=1;position=0
            }else if(r.loadNextBatch()){row=0}
            else {eof=true;consumed=true;index+=1;closeReader()}
          }
          !eof
        }
        override def read():Int=if(!advance()) -1 else {val value=chunk(position)&0xff;position+=1;value}
        override def read(bytes:Array[Byte],offset:Int,length:Int):Int={
          java.util.Objects.checkFromIndexSize(offset,length,bytes.length)
          readable()
          if(length==0)0
          else if(!advance()) -1
          else {val count=math.min(length,chunk.length-position);System.arraycopy(chunk,position,bytes,offset,count);position+=count;count}
        }
        override def close():Unit=if(!streamClosed){streamClosed=true;closeReader()}
      }
    }
    /** Roots are borrowed and reused on the next callback; copy values before retaining them. */
    def foreachRecords(f:VectorSchemaRoot=>Unit):Unit={require(representation=="records","records input required");foreachBatch(f)}
    def foreachBytes(f:Array[Byte]=>Unit):Unit={require(representation=="bytes","bytes input required");foreachBatch(r=>{
      val v=r.getVector(0).asInstanceOf[VarBinaryVector]
      (0 until r.getRowCount).foreach(i=>{require(!v.isNull(i),"null bytes chunk");f(v.get(i))})
    })}
    private[OperatorContext] def closeReader():Unit=if(reader!=null){reader.close();reader=null}
  }
  final class Output private[OperatorContext](val name:String,repr:String,schema:Option[Schema]) {
    def write(bytes:Array[Byte]):Unit={active();require(repr=="bytes","bytes output required");writing=true;bytes.grouped(65536).foreach(b=>frame("data",name,b))}
    private var stream:OutputStream=null
    private var streamClosed=false
    /** Buffers at most 64 KiB. flush publishes pending bytes; close never closes protocol stdout. */
    def byteStream():OutputStream={
      active();require(repr=="bytes","bytes output required")
      if(stream==null){
        val sink=new OutputStream {
          private var streamClosed=false
          private def writable():Unit={if(streamClosed)throw new IOException("output stream closed");active()}
          override def write(value:Int):Unit={writable();Output.this.write(Array(value.toByte))}
          override def write(bytes:Array[Byte],offset:Int,length:Int):Unit={
            writable();java.util.Objects.checkFromIndexSize(offset,length,bytes.length)
            var start=offset
            while(start<offset+length){val end=start+math.min(65536,offset+length-start);frame("data",name,java.util.Arrays.copyOfRange(bytes,start,end));writing=true;start=end}
          }
          override def flush():Unit={writable();out.flush()}
          override def close():Unit={streamClosed=true}
        }
        stream=new BufferedOutputStream(sink,65536) {
          override def write(value:Int):Unit={if(streamClosed)throw new IOException("output stream closed");active();writing=true;super.write(value)}
          override def write(bytes:Array[Byte],offset:Int,length:Int):Unit={if(streamClosed)throw new IOException("output stream closed");active();writing=true;super.write(bytes,offset,length)}
          override def close():Unit=if(!streamClosed){try super.close()finally streamClosed=true}
        }
      }
      stream
    }
    private[OperatorContext] def flushStream():Unit=if(stream!=null && !streamClosed)stream.flush()
    /** Serializes before returning; caller keeps ownership of the root. */
    def write(root:VectorSchemaRoot):Unit={
      active();require(repr=="records" && schema.contains(root.getSchema),"output schema mismatch");writing=true
      val bytes=new ByteArrayOutputStream();val writer=new ArrowStreamWriter(root,null,bytes)
      try {writer.start();writer.writeBatch();writer.end();frame("data",name,bytes.toByteArray)}finally writer.close()
    }
  }
  private[operator] def finish():Unit={active();require(index==names.size,"all inputs must be consumed to Arrow EOS");outputs.values.foreach(_.flushStream());outputs.keys.foreach(p=>frame("end",p));finished=true}
  def close():Unit=if(!closed){closed=true;var failure:Throwable=null;opened.values.foreach(i=>try i.closeReader()catch{case scala.util.control.NonFatal(t)=>if(failure==null)failure=t else failure.addSuppressed(t)});try allocator.close()catch{case scala.util.control.NonFatal(t)=>if(failure==null)failure=t else failure.addSuppressed(t)};if(failure!=null)throw failure}
}
