package xstream.data.fasta

import _root_.xstream.data._
import java.io.{BufferedReader,InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8

final class FastaFormat extends StructuredFileHandler {
  val id="fasta";val extensions=Set("fa","fasta","fna","faa","ffn","frn")
  override def validateOptions(o:FileOptions):Unit={
    require(o.columns.isEmpty && o.parameters.keySet.subsetOf(Set("alphabet","chunk_symbols")),"invalid FASTA options")
    o.parameters.get("alphabet").foreach(v=>require(Set("dna","rna","protein","unknown")(v.toString),"invalid FASTA alphabet"))
    o.parameters.get("chunk_symbols").foreach(v=>require(v.isInstanceOf[Number] && v.asInstanceOf[Number].longValue>0 && v.asInstanceOf[Number].longValue<=1048576,"invalid FASTA chunk size"))
  }
  def openStructure(source:ByteSource,options:FileOptions,context:DatasetContext,check:()=>Unit):DatasetAccess={
    validateOptions(options)
    val chunk=options.parameters.get("chunk_symbols").map(_.asInstanceOf[Number].intValue).getOrElse(65536)
    val alphabet=options.parameters.get("alphabet").map(_.toString).getOrElse("unknown")
    new StructuredDataAccess(source,context.byteChunk) {
      val entries=Seq(Item("/sequences","fasta",None,views=Some(Seq("sequence"))))
      def content[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]=DataViews.when(kind,ScientificViews.Sequences)(
        DataViews[SequenceChunk,SequenceSelection]((selection,c,out)=>{
          val reader=new BufferedReader(new InputStreamReader(source.open(()=>{check();c.check()}),UTF_8),65536)
          try {
            out.start(SequenceDescriptor(alphabet))
            var id:String=null;var description:Option[String]=None;var position=0L;var selected=false
            val buffer=new StringBuilder(chunk);var start=0L;var lineStart=true
            def flush(last:Boolean):Unit=if(selected){c.check();val offset=if(buffer.nonEmpty)start else BigInt(position).min(BigInt(selection.offset)+BigInt(selection.length.getOrElse(Long.MaxValue-selection.offset))).toLong;out.accept(SequenceChunk(id,offset,buffer.toString,last,description));buffer.clear()}
            var ch=reader.read()
            while(ch>=0){
              c.check()
              if(lineStart && ch=='>'){
                if(id!=null)flush(true)
                val header=new StringBuilder;ch=reader.read()
                while(ch>=0 && ch!='\n' && ch!='\r'){require(header.length<16384,"FASTA header too long");header.append(ch.toChar);ch=reader.read()}
                val text=header.toString.trim;require(text.nonEmpty,"empty FASTA header")
                val split=text.indexWhere(_.isWhitespace);id=if(split<0)text else text.take(split);description=if(split<0)None else Some(text.drop(split).trim)
                position=0;selected=selection.ids.isEmpty || selection.ids(id);buffer.clear();lineStart=true
              } else if(lineStart && ch==';'){
                while(ch>=0 && ch!='\n')ch=reader.read();lineStart=true
              } else if(ch=='\n' || ch=='\r'){lineStart=true}
              else if(!ch.toChar.isWhitespace){
                require(id!=null,"FASTA sequence before header")
                require(ch<128 && (ch.toChar.isLetter || "*-?.".contains(ch.toChar)),"invalid FASTA symbol")
                if(selected && position>=selection.offset && selection.length.forall(n=>position-selection.offset<n)){
                  if(buffer.isEmpty)start=position
                  buffer.append(ch.toChar);if(buffer.length==chunk)flush(false)
                }
                position=Math.addExact(position,1L);lineStart=false
              }
              ch=reader.read()
            }
            if(id!=null)flush(true)
          }finally reader.close()
        }))
    }
  }
}
