package dacp.union

import com.fasterxml.jackson.databind.JsonNode
import org.yaml.snakeyaml.{DumperOptions,LoaderOptions,Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import scala.jdk.CollectionConverters._

/** Bounded, data-only YAML documents shared by jobs, policies and variables. */
object YamlDocument {
  def parse(text:String):JsonNode={
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=1024*1024,"YAML document exceeds 1 MiB")
    val options=new LoaderOptions();options.setAllowDuplicateKeys(false);options.setMaxAliasesForCollections(0);options.setNestingDepthLimit(100);options.setCodePointLimit(1024*1024)
    val value=try new Yaml(new SafeConstructor(options)).load[Object](text)catch{case e:org.yaml.snakeyaml.error.YAMLException=>throw new IllegalArgumentException("invalid YAML document",e)}
    def check(v:Any):Unit=v match {
      case null|_:String|_:java.lang.Boolean|_:java.lang.Byte|_:java.lang.Short|_:java.lang.Integer|_:java.lang.Long|_:java.math.BigInteger|_:java.math.BigDecimal=>()
      case n:java.lang.Number=>require(java.lang.Double.isFinite(n.doubleValue),"YAML numbers must be finite")
      case m:java.util.Map[_,_]=>m.asScala.foreach{case(k,x)=>require(k.isInstanceOf[String],"YAML mapping keys must be strings");check(x)}
      case xs:java.util.List[_]=>xs.asScala.foreach(check)
      case _=>throw new IllegalArgumentException("unsupported YAML value; quote dates and other textual values")
    }
    check(value);Json.mapper.valueToTree[JsonNode](value)
  }
  def dump(value:JsonNode):String={
    val options=new DumperOptions();options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);options.setPrettyFlow(false);options.setSplitLines(false);options.setIndent(2)
    new Yaml(options).dump(Json.mapper.convertValue(value,classOf[Object]))
  }
}
