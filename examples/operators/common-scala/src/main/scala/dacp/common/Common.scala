package dacp.common

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction,StandardCharsets}
import java.nio.file.{Files,Path}
import java.security.MessageDigest
import java.text.Normalizer
import java.util.{Base64,Locale}
import com.fasterxml.jackson.databind.{JsonNode,ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode,ObjectNode}
import dacp.operator.{Operator,OperatorContext}
import scala.jdk.CollectionConverters._

/** Document operators explicitly buffer at most 16 MiB; Arrow transport remains streamed. */
abstract class DocumentOperator(val operation:String) extends Operator {
  final def inferOutputs(inputs:Map[String,dacp.StreamDescriptor],params:JsonNode):dacp.InferenceResult={
    require(inputs.keySet==Set("source"),"expected source input")
    require(inputs("source").representations.isEmpty || inputs("source").representations==Set("bytes"),"bytes input required")
    val spec=Common.specs.get(operation)
    require(params.fieldNames.asScala.toSet--Set("max_input_bytes")==spec.get("params").fieldNames.asScala.toSet,"unexpected or missing parameters")
    dacp.InferenceResult(Map("result"->dacp.StreamDescriptor(Set("bytes"),Set(spec.get("format").asText),certainty="known")))
  }
  override def previewOutputs(inputs:Map[String,dacp.StreamDescriptor],params:JsonNode,limits:dacp.PreviewLimits):Map[String,dacp.OutputSample]={
    val sample=inputs.get("source").flatMap(_.sample).getOrElse(throw new IllegalArgumentException("Upstream sample unavailable: source"))
    require(sample.representation=="bytes","bytes sample required")
    Map("result"->dacp.OutputSample("bytes",Common.specs.get(operation).path("format").asText,data=Common.transform(operation,sample.bounded(limits).data.toArray,params).toVector,truncated=sample.truncated).bounded(limits))
  }
  final def process(ctx:OperatorContext):Unit={
    val p=ctx.params
    val spec=Common.specs.get(operation)
    val required=spec.get("params").fieldNames.asScala.toSet
    require(p.fieldNames.asScala.toSet--Set("max_input_bytes")==required,"unexpected or missing parameters")
    val maximum=p.path("max_input_bytes")
    val limit=if(maximum.isMissingNode)16777216 else {require(maximum.isIntegralNumber && maximum.canConvertToInt,"invalid size limit");maximum.asInt}
    require(limit>0 && limit<=16777216,"max_input_bytes must be 1..16777216")
    val source=ctx.input("source");require(source.representation=="bytes","bytes input required")
    val bytes=new ByteArrayOutputStream()
    val output=ctx.bytesOutput("result",spec.get("format").asText)
    source.foreachBytes { chunk=>require(bytes.size.toLong+chunk.length<=limit,"document exceeds max_input_bytes");bytes.write(chunk) }
    output.write(Common.transform(operation,bytes.toByteArray,p))
  }
}
object Common {
  val mapper=new ObjectMapper()
  val specs:JsonNode={val s=getClass.getResourceAsStream("/catalog.json");try mapper.readTree(s)finally s.close()}
  def utf8(b:Array[Byte]):String=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b)).toString
  def encoded(s:String):Array[Byte]=s.getBytes(StandardCharsets.UTF_8)
  def arr(xs:Iterable[JsonNode]):ArrayNode={val a=mapper.createArrayNode();xs.foreach(a.add);a}
  def fields(v:JsonNode):Vector[(String,JsonNode)]={require(v.isObject,"JSON object required");v.fields.asScala.map(e=>e.getKey->e.getValue).toVector}
  def values(v:JsonNode):Vector[JsonNode]={require(v.isArray,"JSON array required");v.elements.asScala.toVector}
  def objectOf(xs:Iterable[(String,JsonNode)]):ObjectNode={val o=mapper.createObjectNode();xs.foreach{case(k,v)=>o.set[JsonNode](k,v)};o}
  def n(p:JsonNode):Int={val v=p.get("n");require(v.isIntegralNumber && v.canConvertToInt && v.asInt>=0,"n must be nonnegative integer");v.asInt}
  def sortJson(v:JsonNode):JsonNode=if(v.isObject)objectOf(fields(v).sortBy(_._1).map{case(k,x)=>k->sortJson(x)})else if(v.isArray)arr(values(v).map(sortJson))else v
  def compare(a:JsonNode,b:JsonNode):Boolean={require((a.isNumber&&b.isNumber)||(a.isTextual&&b.isTextual),"comparable numbers or strings required");if(a.isNumber)a.decimalValue.compareTo(b.decimalValue)<0 else a.asText.compareTo(b.asText)<0}
  def lines(s:String):Vector[String]=s.split("\\r\\n|[\\n\\r]",-1).toVector match {case v if v.lastOption.contains("")=>v.dropRight(1);case v=>v}
  def html(s:String):String=s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\"","&quot;").replace("'","&#x27;")
  // Strict CSV parser: quotes, escaped quotes, embedded newlines, CRLF and final empty cells.
  def csv(s:String):Vector[Vector[String]]={
    val rows=Vector.newBuilder[Vector[String]];var row=Vector.empty[String];val cell=new StringBuilder
    var quoted=false;var closed=false;var i=0;var touched=false
    def endCell():Unit={row=row:+cell.toString;cell.clear();closed=false}
    def endRow():Unit={endCell();rows+=row;row=Vector.empty;touched=false}
    while(i<s.length){val c=s(i);touched=true
      if(quoted){if(c=='"'){if(i+1<s.length&&s(i+1)=='"'){cell+='"';i+=1}else{quoted=false;closed=true}}else cell+=c}
      else if(c==',')endCell()
      else if(c=='\n'||c=='\r'){endRow();if(c=='\r'&&i+1<s.length&&s(i+1)=='\n')i+=1}
      else if(c=='"'){require(cell.isEmpty && !closed,"unexpected CSV quote");quoted=true}
      else{require(!closed,"characters after CSV quote");cell+=c}
      i+=1
    }
    require(!quoted,"unterminated CSV quote");if(touched||row.nonEmpty||cell.nonEmpty)endRow()
    val r=rows.result();require(r.nonEmpty&&r.head.distinct.size==r.head.size,"unique CSV header required");require(r.tail.forall(_.size==r.head.size),"CSV row width differs from header");r
  }
  def csvOut(rows:Vector[Vector[String]]):String=rows.map(_.map(x=>if(x.exists(c=>c==','||c=='"'||c=='\n'||c=='\r'))"\""+x.replace("\"","\"\"")+"\"" else x).mkString(",")+"\n").mkString
  def transform(name:String,b:Array[Byte],p:JsonNode):Array[Byte]={
    def param(k:String):String={val v=p.get(k);require(v!=null && v.isTextual,s"$k must be text");v.asText}
    if(name.startsWith("bytes_"))return name match {
      case "bytes_base64_encode"=>Base64.getEncoder.encode(b)
      case "bytes_base64_decode"=>Base64.getDecoder.decode(b)
      case "bytes_hex_encode"=>encoded(b.map(x=>f"${x&255}%02x").mkString)
      case "bytes_hex_decode"=>val s=utf8(b);require(s.length%2==0 && s.matches("[0-9a-fA-F]*"),"invalid hex");s.grouped(2).map(Integer.parseInt(_,16).toByte).toArray
      case "bytes_count"=>encoded(s"{\"bytes\":${b.length}}")
      case "bytes_crc32"=>val c=new java.util.zip.CRC32();c.update(b);encoded(c.getValue.toString+"\n")
      case x=>val algorithm=Map("bytes_sha256"->"SHA-256","bytes_sha512"->"SHA-512","bytes_sha3_256"->"SHA3-256","bytes_sha384"->"SHA-384")(x);encoded(MessageDigest.getInstance(algorithm).digest(b).map(x=>f"${x&255}%02x").mkString+"\n")
    }
    val s=utf8(b)
    if(name.startsWith("text_"))return encoded(name.stripPrefix("text_") match {
      case "uppercase"=>s.toUpperCase(Locale.ROOT)
      case "lowercase"=>s.toLowerCase(Locale.ROOT)
      case "fold_german"=>s.toLowerCase(Locale.ROOT).replace("ß","ss")
      case "titlecase"=>"\\b\\p{L}+".r.replaceAllIn(s,m=>java.util.regex.Matcher.quoteReplacement(m.matched.take(1).toUpperCase(Locale.ROOT)+m.matched.drop(1).toLowerCase(Locale.ROOT)))
      case "capitalize"=>s.take(1).toUpperCase(Locale.ROOT)+s.drop(1).toLowerCase(Locale.ROOT)
      case "swapcase"=>s.map(c=>if(c.isUpper)c.toLower else c.toUpper)
      case "strip"=>s.strip();case "lstrip"=>s.replaceFirst("^\\s+","");case "rstrip"=>s.replaceFirst("\\s+$","")
      case "normalize_spaces"=>s.trim.split("\\s+").mkString(" ")
      case "normalize_newlines"=>s.replace("\r\n","\n").replace("\r","\n")
      case "remove_blank_lines"=>lines(s).filter(_.trim.nonEmpty).mkString("\n")
      case "trim_lines"=>lines(s).map(_.strip()).mkString("\n")
      case "sort_lines"=>lines(s).sorted.mkString("\n")
      case "reverse_lines"=>lines(s).reverse.mkString("\n")
      case "unique_lines"=>lines(s).distinct.mkString("\n")
      case "number_lines"=>lines(s).zipWithIndex.map{case(x,i)=>s"${i+1}\t$x"}.mkString("\n")
      case "head_lines"=>lines(s).take(n(p)).mkString("\n");case "tail_lines"=>lines(s).takeRight(n(p)).mkString("\n")
      case "filter_lines"=>lines(s).filter(_.contains(param("text"))).mkString("\n")
      case "exclude_lines"=>lines(s).filterNot(_.contains(param("text"))).mkString("\n")
      case "replace"=>s.replace(param("old"),param("new"))
      case "prefix_lines"=>lines(s).map(param("prefix")+_).mkString("\n")
      case "suffix_lines"=>lines(s).map(_+param("suffix")).mkString("\n")
      case "normalize_nfc"=>Normalizer.normalize(s,Normalizer.Form.NFC)
      case "normalize_nfkc"=>Normalizer.normalize(s,Normalizer.Form.NFKC)
      case "html_escape"=>html(s)
      case "html_unescape_basic"=>s.replace("&lt;","<").replace("&gt;",">").replace("&quot;","\"").replace("&#x27;","'").replace("&amp;","&")
      case "url_encode"=>java.net.URLEncoder.encode(s,"UTF-8").replace("+","%20")
      case "url_decode"=>java.net.URLDecoder.decode(s.replace("+","%2B"),"UTF-8")
      case "reverse_characters"=>new String(s.codePoints.toArray.reverse,0,s.codePointCount(0,s.length))
      case "count_characters"=>s.codePointCount(0,s.length).toString+"\n"
      case "count_lines"=>lines(s).size.toString+"\n"
      case "count_words"=>s.trim.split("\\s+").count(_.nonEmpty).toString+"\n"
      case "remove_digits"=>s.replaceAll("[0-9]","")
      case "keep_digits"=>s.replaceAll("[^0-9]","")
      case "remove_ascii_punctuation"=>s.filterNot(c=>c<=127 && !c.isLetterOrDigit && !c.isWhitespace)
      case "tabs_to_spaces"=>s.replace("\t","    ")
      case "spaces_to_tabs"=>s.replace("    ","\t")
      case "join_lines"=>lines(s).mkString(param("separator"))
      case "split_words"=>s.trim.split("\\s+").filter(_.nonEmpty).mkString("\n")
      case "sort_lines_desc"=>lines(s).sorted.reverse.mkString("\n")
      case "nonempty_count"=>lines(s).count(_.trim.nonEmpty).toString+"\n"
      case "remove_bom"=>s.stripPrefix("\ufeff")
      case "ensure_newline"=>if(s.endsWith("\n"))s else s+"\n"
      case "remove_newlines"=>s.replace("\r","").replace("\n","")
      case "ascii_only"=>s.filter(_<=127)
      case "normalize_nfd"=>Normalizer.normalize(s,Normalizer.Form.NFD)
      case "normalize_nfkd"=>Normalizer.normalize(s,Normalizer.Form.NFKD)
      case "strip_accents"=>Normalizer.normalize(s,Normalizer.Form.NFD).replaceAll("\\p{M}","")
    })
    if(name.startsWith("json_")){
      val v=mapper.readTree(s);require(v!=null,"JSON value required")
      def key=param("key")
      def keys=values(p.get("keys")).map(_.asText)
      val result:JsonNode=name.stripPrefix("json_") match {
        case "identity"|"pretty"=>v
        case "sort_keys"=>sortJson(v)
        case "keys"=>arr(fields(v).map(x=>mapper.getNodeFactory.textNode(x._1)))
        case "values"=>arr(fields(v).map(_._2))
        case "entries"=>arr(fields(v).map{case(k,x)=>arr(Vector(mapper.getNodeFactory.textNode(k),x))})
        case "get"=>require(v.has(key),"missing key");v.get(key)
        case "select"=>objectOf(keys.map(k=>{require(v.has(k),"missing key");k->v.get(k)}))
        case "drop"=>objectOf(fields(v).filterNot(x=>keys.contains(x._1)))
        case "rename"=>objectOf(fields(v).map{case(k,x)=>(if(k==param("old"))param("new")else k)->x})
        case "set"=>objectOf(fields(v).filterNot(_._1==key):+(key->p.get("value")))
        case "array_length"=>mapper.getNodeFactory.numberNode(values(v).size)
        case "array_reverse"=>arr(values(v).reverse)
        case "array_sort"=>arr(values(v).sortWith(compare))
        case "array_unique"=>arr(values(v).distinct)
        case "array_head"=>arr(values(v).take(n(p)))
        case "array_flatten"=>arr(values(v).flatMap(values))
        case "pluck"=>arr(values(v).map(x=>{require(x.has(key),"missing key");x.get(key)}))
        case "filter_equal"=>arr(values(v).filter(x=>{require(x.has(key),"missing key");x.get(key)==p.get("value")}))
        case "to_ndjson"=>return encoded(values(v).map(x=>mapper.writeValueAsString(x)+"\n").mkString)
      }
      if(name=="json_pretty")return encoded(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result))
      return mapper.writeValueAsBytes(result)
    }
    val all=csv(s);val h=all.head;val r=all.tail
    def column(k:String="column"):Int={val i=h.indexOf(param(k));require(i>=0,"unknown CSV column");i}
    def selected=values(p.get("columns")).map(_.asText)
    val result:Vector[Vector[String]]=name.stripPrefix("csv_") match {
      case "identity"=>all
      case "select"=>val indexes=selected.map(k=>{val i=h.indexOf(k);require(i>=0,"unknown CSV column");i});selected+:r.map(x=>indexes.map(x))
      case "drop"=>val indexes=h.indices.filterNot(i=>selected.contains(h(i)));h.indices.filterNot(i=>selected.contains(h(i))).map(h).toVector+:r.map(x=>indexes.map(x).toVector)
      case "rename"=>h.map(x=>if(x==param("old"))param("new")else x)+:r
      case "head"=>h+:r.take(n(p));case "tail"=>h+:r.takeRight(n(p));case "reverse"=>h+:r.reverse
      case "unique"=>h+:r.distinct
      case "sort"=>h+:r.sortBy(_(column()))
      case "sort_numeric"=>h+:r.sortBy(x=>BigDecimal(x(column())))
      case "filter_equal"=>h+:r.filter(_(column())==param("value"))
      case "filter_not_equal"=>h+:r.filterNot(_(column())==param("value"))
      case "filter_contains"=>h+:r.filter(_(column()).contains(param("value")))
      case "trim"=>h+:r.map(_.map(_.strip()))
      case "uppercase"=>h+:r.map(x=>x.updated(column(),x(column()).toUpperCase(Locale.ROOT)))
      case "lowercase"=>h+:r.map(x=>x.updated(column(),x(column()).toLowerCase(Locale.ROOT)))
      case "fill_empty"=>h+:r.map(_.map(x=>if(x.isEmpty)param("value")else x))
      case "add_constant"=>(h:+param("column"))+:r.map(_:+param("value"))
      case "to_ndjson"=>return encoded(r.map(x=>mapper.writeValueAsString(objectOf(h.zip(x.map(mapper.getNodeFactory.textNode))))+"\n").mkString)
      case "count"=>return encoded(s"{\"rows\":${r.size}}")
    }
    encoded(csvOut(result))
  }
}
object Verify {
  def main(args:Array[String]):Unit={
    val names=Common.specs.fieldNames.asScala.toVector
    require(names.size==100)
    names.foreach { name=>val s=Common.specs.get(name);val input=Base64.getDecoder.decode(s.get("sample").asText);val expected=Base64.getDecoder.decode(s.get("expected").asText)
      val actual=Common.transform(name,input,s.get("params"));require(java.util.Arrays.equals(actual,expected),s"$name mismatch: ${Common.utf8(actual)}")
      println("Verified scala_"+name+"@1.0")
    }
  }
}

/** Protocol contract check using real Arrow fixtures and the SDK supplied by the runtime. */
object VerifyStreams {
  def main(args:Array[String]):Unit={
    Common.specs.fieldNames.asScala.foreach { name=>
      val directory=Path.of(args(0)).resolve(name)
      val output=new ByteArrayOutputStream()
      val operator=new DocumentOperator(name){}
      dacp.operator.OperatorRunner.run(Array("operator",directory.toString,directory.resolve("params.json").toString),output)(operator.process)
      val in=new java.io.DataInputStream(new java.io.ByteArrayInputStream(output.toByteArray))
      val payload=new ByteArrayOutputStream();var started=false;var ended=false
      while(in.available()>0){val h=new Array[Byte](in.readInt());val d=new Array[Byte](in.readInt());in.readFully(h);in.readFully(d)
        val header=Common.mapper.readTree(h);require(header.path("port").asText=="result")
        header.path("type").asText match {
          case "start"=>require(!started && !ended);started=true;require(header.path("representation").asText=="bytes")
          case "data"=>require(started && !ended);payload.write(d)
          case "end"=>require(started && !ended);ended=true
        }
      }
      require(started && ended)
      require(java.util.Arrays.equals(payload.toByteArray,Base64.getDecoder.decode(Common.specs.get(name).get("expected").asText)),name+" protocol result mismatch")
      println("Protocol verified scala_"+name+"@1.0")
    }
  }
}
