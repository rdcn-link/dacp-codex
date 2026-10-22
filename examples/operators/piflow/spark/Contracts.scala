// Private, minimal embedding contracts for the vendored EcoFlow algorithms.
// This does not embed PiFlow's scheduler, server, checkpointing or configuration system.
package cn.piflow {
  import org.apache.spark.sql.{DataFrame,SparkSession}
  trait ProcessContext
  trait JobContext { def get[T]()(implicit m:Manifest[T]):T }
  trait JobInputStream { def read():DataFrame; def read(port:String):DataFrame }
  trait JobOutputStream { def write(data:DataFrame):Unit }
}
package cn.piflow.conf {
  import cn.piflow._
  import cn.piflow.conf.bean.PropertyDescriptor
  abstract class ConfigurableStop extends Serializable {
    val authorEmail:String
    val description:String
    val inportList:List[String]
    val outportList:List[String]
    def setProperties(p:Map[String,Any]):Unit
    def perform(in:JobInputStream,out:JobOutputStream,ctx:JobContext):Unit
    def initialize(ctx:ProcessContext):Unit
    def getPropertyDescriptor():List[PropertyDescriptor]
    def getIcon():Array[Byte]
    def getGroup():List[String]
  }
  object Port { val DefaultPort="Default"; val LeftPort="Left"; val RightPort="Right" }
  object StopGroup { val ExcelGroup="Excel"; val CleanGroup="Clean" }
}
package cn.piflow.conf.bean {
  final class PropertyDescriptor {
    def name(v:String):this.type=this
    def displayName(v:String):this.type=this
    def description(v:String):this.type=this
    def defaultValue(v:String):this.type=this
    def required(v:Boolean):this.type=this
    def example(v:String):this.type=this
    def allowableValues(v:Set[String]):this.type=this
  }
}
package cn.piflow.conf.util {
  object MapUtil { def get(map:Map[String,Any],key:String):Any=map.getOrElse(key,throw new IllegalArgumentException("missing parameter: "+key)) }
  object ImageUtil { def getImage(path:String,cls:String=""):Array[Byte]=Array.emptyByteArray }
}
