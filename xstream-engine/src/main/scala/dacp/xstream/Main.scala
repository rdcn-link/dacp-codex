package dacp.xstream

import _root_.xstream.data._

import dacp._
import dacp.client.{Source,TlsConfig,WorkflowVariables}
import dacp.union.{Json,YamlDocument}
import java.nio.file.{Files,Path}
import scala.util.Using
import scala.jdk.CollectionConverters._

/** Standalone launcher: no server, listening port, server certificate or dashboard. */
object Main {
  private def sampleValue(value:Any):com.fasterxml.jackson.databind.JsonNode=value match {
    case null=>Json.mapper.nullNode()
    case v:org.apache.arrow.vector.util.Text=>Json.mapper.getNodeFactory.textNode(v.toString)
    case v:java.util.Map[_,_]=>val n=Json.obj();v.asScala.foreach{case(k,x)=>n.set[com.fasterxml.jackson.databind.JsonNode](k.toString,sampleValue(x))};n
    case v:java.util.List[_]=>val n=Json.mapper.createArrayNode();v.asScala.foreach(x=>n.add(sampleValue(x)));n
    case v:java.time.temporal.TemporalAccessor=>Json.mapper.getNodeFactory.textNode(v.toString)
    case v:Array[Byte]=>Json.mapper.getNodeFactory.textNode(java.util.Base64.getEncoder.encodeToString(v))
    case v=>Json.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v)
  }
  def main(args:Array[String]):Unit={
    require(args.length>=2 && args.length<=4,"Usage: xstream-runner CONFIG.yaml JOB.yaml [VARIABLES.yaml] [POLICY.yaml]")
    val config=YamlDocument.parse(Files.readString(Path.of(args(0))))
    val variables=if(args.length>=3)YamlDocument.parse(Files.readString(Path.of(args(2))))else Json.obj()
    val policy=if(args.length>=4)YamlDocument.parse(Files.readString(Path.of(args(3))))else Json.obj()
    val yaml=WorkflowVariables.resolve(Files.readString(Path.of(args(1))),variables)
    require(Jobs.parse(yaml).outputs.size==1,"xstream-runner stdout requires exactly one selected output; embedded JobEngine supports multiple outputs")
    val representations=scala.collection.mutable.Map.empty[String,String]
    Using.resource(new PluginEnvironment(Option(config.get("plugin_directory")).map(n=>Path.of(n.asText)))){plugins=>
      Using.resource(new LocalEngine(config,plugins,(binding,allocator,cancel)=>RemoteItems.open(config,binding,cancel))){host=>
        Using.resource(new StreamCancel(timeoutSeconds=host.executionTimeoutSeconds)){cancel=>
          host.engine.run(yaml,frame=>{
            val h=StreamProtocol.header(frame)
            val port=h.path("port").asText
            if(h.path("type").asText=="start")representations(port)=h.path("representation").asText
            if(h.path("type").asText=="data"){
              if(representations.get(port).contains("records"))Using.resource(new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(frame.data),host.allocator)){reader=>
                import scala.jdk.CollectionConverters._
                while(reader.loadNextBatch()){
                  val root=reader.getVectorSchemaRoot
                  (0 until root.getRowCount).foreach {i=>val row=Json.obj();root.getFieldVectors.asScala.foreach(v=>row.set[com.fasterxml.jackson.databind.JsonNode](v.getName,sampleValue(v.getObject(i))));System.out.println(row.toString)}
                }
              }else System.out.write(frame.data)
              System.out.flush()
            }
          },cancel,policy)
        }
      }
    }
  }
}

object RemoteItems {
  private def clientPredicate(p:Predicate):dacp.client.Predicate={
    import dacp.client.Expressions.col
    def literal(v:Literal):Any=v.kind match {
      case "integer"=>BigInt(v.value);case "decimal"=>BigDecimal(v.value);case "float"=>v.value.toDouble
      case "boolean"=>v.value.toBoolean;case "string"=>v.value
    }
    val c=col(p.column)
    p.op match {
      case "and"=>p.children.map(clientPredicate).reduce(_ && _)
      case "or"=>p.children.map(clientPredicate).reduce(_ || _)
      case "not"=> !clientPredicate(p.children.head)
      case "is_null"=>c.isNull;case "is_not_null"=>c.isNotNull;case "in"=>c.isin(p.values.map(literal))
      case "eq"=>c === literal(p.values.head);case "ne"=>c =!= literal(p.values.head)
      case "lt"=>c < literal(p.values.head);case "le"=>c <= literal(p.values.head)
      case "gt"=>c > literal(p.values.head);case "ge"=>c >= literal(p.values.head)
    }
  }
  def open(config:com.fasterxml.jackson.databind.JsonNode,binding:InputBinding,cancel:StreamCancel):FrameSource={
    val value=binding.uri.get;val uri=java.net.URI.create(value);val origin=DacpUri.origin(value)
    val nodes=config.path("nodes");import scala.jdk.CollectionConverters._
    val node=nodes.elements.asScala.find(n=>DacpUri.origin(n.path("url").asText)==origin).getOrElse(throw new IllegalArgumentException("Remote Item node is not configured"))
    val tls=TlsConfig(ca=Option(node.get("ca")).map(n=>Path.of(n.asText)),tokenFile=Option(node.get("token_file")).map(n=>Path.of(n.asText)))
    val parts=uri.getPath.stripPrefix("/").split("/",2)
    val dataset=cancel.add(Source.open(origin+"/"+parts(0),tls));val path="/"+parts(1)
    val metadata=dataset.describe(path);val view=ItemViews.select(binding.view,metadata.views)
    val item=cancel.add(dataset.open(path,predicate=binding.scan.flatMap(_.predicate).map(clientPredicate),columns=binding.scan.flatMap(_.columns),limit=binding.scan.flatMap(_.limit)));val bytes=if(view=="bytes")Some(cancel.add(item.asBytes()))else None
    if(!Set("records","bytes")(view)){
      val plugins=new PluginEnvironment()
      val codec=DataViewCodecs.named(plugins,view)
      return ItemFrameStreams.remote(metadata.format,cancel){out=>
        try {
          val stream=item.asView(codec.kind,codec.selection(binding.selection.getOrElse(Data.mapper.createObjectNode())))
          try stream.consume(new DataConsumer[codec.Block]{def start(d:DataDescriptor[codec.Block]):Unit=out.start(codec.descriptor(d));def accept(b:codec.Block):Unit={cancel.active();out.accept(codec.encode(b))}})finally stream.close()
        }finally {item.close();dataset.close();plugins.close()}
      }
    }
    val records=if(view=="records")Some(cancel.add(item.asRecords()))else None
    new FrameSource {
      private var began=false;private var ended=false
      def next():Option[OperatorFrame]={
        cancel.active();if(!began){began=true;return Some(StreamProtocol.start("",view,metadata.format,records.map(_.root.getSchema)))}
        if(ended)return None
        val data=bytes.map(_.readNBytes(65536)).orElse(records.filter(_.next()).map(r=>StreamProtocol.batch(r.root)))
        data.filter(_.nonEmpty) match {case Some(b)=>Some(StreamProtocol.frame("data",data=b));case None=>ended=true;Some(StreamProtocol.frame("end"))}
      }
      def close():Unit={try item.close()finally dataset.close()}
    }
  }
}
