package dacp

import _root_.xstream.data._
import com.fasterxml.jackson.databind.JsonNode

/** Metadata and bounded samples from typed views, without starting an operator worker. */
object DataViewInference {
  private object Complete extends scala.util.control.ControlThrowable
  def inspect(codec:DataViewSupport)(stream:ArrowDataStream,selection:codec.Selection,format:String,limits:Option[PreviewLimits],check:()=>Unit):StreamDescriptor={
    var result:StreamDescriptor=null
    var rows=Vector.empty[JsonNode]
    var truncated=false
    try stream.consume(new ArrowConsumer {
      def start(schema:org.apache.arrow.vector.types.pojo.Schema,d:org.apache.arrow.vector.dictionary.DictionaryProvider):Unit={
        check();codec.validateSelection(selection,schema)
        result=StreamDescriptor(Set(codec.kind.name),Set(format),certainty="known",structure=Some(codec.descriptor(schema)))
        if(limits.isEmpty)throw Complete
      }
      def accept(batch:ArrowBatch):Unit={
        check();val l=limits.get
        if(rows.size>=l.maxRows){truncated=true;throw Complete}
        codec.preview(batch,l.maxRows-rows.size+1,math.max(1,math.min(64,l.maxBytes/32))).foreach { row=>
          if(rows.size>=l.maxRows){truncated=true;throw Complete}
          val candidate=OutputSample(codec.kind.name,format,rows=rows:+row)
          if(OutputSamples.size(candidate)>l.maxBytes){truncated=true;throw Complete}
          rows=rows:+row
        }
        if(rows.size>=l.maxRows){truncated=true;throw Complete}
      }
    })catch{case Complete=>()}finally stream.close()
    require(result!=null,"view did not supply a descriptor")
    result.copy(sample=limits.map(l=>OutputSample(codec.kind.name,format,rows=rows,truncated=truncated).bounded(l)))
  }
}
