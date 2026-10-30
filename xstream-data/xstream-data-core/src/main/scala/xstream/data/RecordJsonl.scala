package xstream.data

import java.io.OutputStream
import com.fasterxml.jackson.core.{JsonFactory,JsonGenerator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.pojo.Field
import scala.jdk.CollectionConverters._

/** UTF-8 JSONL representation for records-only Items. Chunks are bounded independently of rows. */
object RecordJsonl {
  private val factory=new JsonFactory()
  private def write(g:JsonGenerator,value:Any):Unit=value match {
    case null=>g.writeNull()
    case b:java.lang.Boolean=>g.writeBoolean(b)
    case n:java.lang.Byte=>g.writeNumber(n.intValue)
    case n:java.lang.Short=>g.writeNumber(n.intValue)
    case n:java.lang.Integer=>g.writeNumber(n.intValue)
    case n:java.lang.Long=>g.writeNumber(n.longValue)
    case n:java.math.BigDecimal=>g.writeNumber(n)
    case n:java.math.BigInteger=>g.writeNumber(n)
    case n:java.lang.Float=>if(java.lang.Float.isFinite(n))g.writeNumber(n.floatValue)else g.writeString(n.toString)
    case n:java.lang.Double=>if(java.lang.Double.isFinite(n))g.writeNumber(n.doubleValue)else g.writeString(n.toString)
    case b:Array[Byte]=>g.writeBinary(b)
    case s:org.apache.arrow.vector.util.Text=>g.writeString(s.toString)
    case s:CharSequence=>g.writeString(s.toString)
    case t:java.time.temporal.TemporalAccessor=>g.writeString(t.toString)
    case t:java.time.temporal.TemporalAmount=>g.writeString(t.toString)
    case m:java.util.Map[_,_]=>
      g.writeStartObject();m.asScala.foreach {case(k:String,v)=>g.writeFieldName(k);write(g,v);case _=>throw new IllegalArgumentException("JSONL object keys must be strings")};g.writeEndObject()
    case values:java.lang.Iterable[_]=>g.writeStartArray();values.asScala.foreach(write(g,_));g.writeEndArray()
    case _=>throw new IllegalArgumentException("unsupported JSONL value type: "+value.getClass.getName)
  }
  def encode(listener:DataConsumer[ByteChunk],ready:()=>Unit,chunkSize:Int=65536)(read:DataConsumer[RecordBatch]=>Unit):Unit={
    require(chunkSize>0,"byte chunk size must be positive")
    var input:VectorSchemaRoot=null
    var dictionaries:DictionaryProvider=null
    var generator:JsonGenerator=null
    var ended=false;var aborted=false
    def active():Unit=ready()
    val sink=new OutputStream {
      private val buffer=new Array[Byte](chunkSize)
      private var used=0
      override def write(b:Int):Unit={val one=Array(b.toByte);write(one,0,1)}
      override def write(bytes:Array[Byte],offset:Int,length:Int):Unit=if(!aborted){
        var at=offset;var left=length
        while(left>0){active();val n=math.min(left,buffer.length-used);System.arraycopy(bytes,at,buffer,used,n);used+=n;at+=n;left-=n;if(used==buffer.length)flush()}
      }
      override def flush():Unit=if(!aborted && used>0){
        active();listener.accept(ByteChunk(buffer,0,used));used=0
      }
    }
    val wrapped=new DataConsumer[RecordBatch] {
      def start(descriptor:DataDescriptor[RecordBatch]):Unit={
        val records=descriptor.asInstanceOf[RecordsDescriptor]
        val dict=records.dictionaries
        require(generator==null,"records stream started twice")
        val fields=records.schema.getFields.asScala
        require(fields.map(_.getName).distinct.size==fields.size,"JSONL requires unique column names")
        def validateNested(f:Field):Unit={
          require(f.getDictionary==null,"nested dictionaries are unsupported in JSONL")
          val children=f.getChildren.asScala
          require(children.map(_.getName).distinct.size==children.size,"JSONL requires unique nested field names")
          children.foreach(validateNested)
        }
        fields.foreach { f=>
          f.getChildren.asScala.foreach(validateNested)
          if(f.getDictionary!=null)require(dict!=null && dict.lookup(f.getDictionary.getId)!=null,"missing JSONL dictionary")
        }
        active();dictionaries=dict
        generator=factory.createGenerator(sink)
        generator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        generator.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT)
        generator.setRootValueSeparator(null)
        listener.start(BytesDescriptor("application/x-ndjson"))
      }
      def accept(batch:RecordBatch):Unit={
        input=batch.root
        require(generator!=null && !ended,"invalid records stream lifecycle")
        (0 until input.getRowCount).foreach { row=>
          active();generator.writeStartObject()
          input.getFieldVectors.asScala.foreach { v=>
            generator.writeFieldName(v.getName)
            val raw=v.getObject(row);val encoding=v.getField.getDictionary
            val value=if(raw==null || encoding==null)raw else {
              val values=dictionaries.lookup(encoding.getId).getVector
              val index=raw.asInstanceOf[Number].longValue
              require(index>=0 && index<values.getValueCount,"invalid dictionary index")
              values.getObject(index.toInt)
            }
            write(generator,value)
          }
          generator.writeEndObject();generator.writeRaw('\n')
        }
        generator.flush();sink.flush()
      }

    }
    try {
      read(wrapped)
      require(generator!=null,"records stream has no descriptor")
      ended=true;active();generator.flush();sink.flush()
    } finally {
      // Never flush buffered partial JSON after a provider error or downstream cancellation.
      aborted=true
      if(generator!=null)generator.close()
    }
  }
}
