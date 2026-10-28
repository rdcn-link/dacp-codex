package org.apache.spark.sql.dacp

import cn.piflow.{JobContext,JobInputStream,JobOutputStream}
import cn.piflow.conf.ConfigurableStop
import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.{Operator,OperatorContext}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector,ColumnarBatch}
import java.io.ByteArrayInputStream
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** Shared Spark environment, thin wrappers, explicit bounded input materialization. */
abstract class EcoFlowOperator(className:String,ports:Map[String,String],required:Set[String]) extends Operator {
  final def inferOutputs(inputs:Map[String,dacp.StreamDescriptor],params:JsonNode):dacp.InferenceResult={
    require(required.subsetOf(params.fieldNames.asScala.toSet),"missing required parameters")
    dacp.InferenceResult(Map("result"->dacp.StreamDescriptor(Set("records"),Set("arrow"),certainty="unknown")),
      Vector(dacp.InferenceDiagnostic("warning","spark_dynamic_schema","Spark output schema requires execution")))
  }

  override def previewOutputs(inputs:Map[String,dacp.StreamDescriptor],params:JsonNode,limits:dacp.PreviewLimits):Map[String,dacp.OutputSample]=
    throw new IllegalArgumentException("This Spark adapter cannot safely preview arbitrary upstream EcoFlow programs")

  def process(ctx:OperatorContext):Unit={
    require(ctx.inputNames.toSet==ports.keySet,"unexpected input ports")
    require(ctx.params.fieldNames.asScala.toSet==required,"missing or unexpected parameters")
    val stop=Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[ConfigurableStop]
    val p=ctx.params.fields.asScala.map { e=>
      val v=e.getValue
      val value:Any=if(e.getKey=="flagValue" && className.endsWith("ReichsteinCheck")){
        require(v.isIntegralNumber && v.canConvertToInt,"flagValue must be an integer");Integer.valueOf(v.asInt)
      }else if(e.getKey=="percentFlux"){
        require(v.isNumber,"percentFlux must be numeric");v.asDouble
      }else {require(v.isTextual,"parameter must be a string: "+e.getKey);v.asText}
      e.getKey->value
    }.toMap
    stop.setProperties(p)
    p.get("percentFlux").foreach(x=>require(x.toString.toDouble>0 && x.toString.toDouble<=1,"percentFlux must be in (0,1]"))
    if(className.contains("StorageItemPM_")){
      val heights=p("equipmentHeightDifference").toString.split(",").map(_.trim.toDouble)
      require(heights.length==7 && heights.forall(x=>x.isFinite && x>=0),"seven finite nonnegative layer heights required")
      require(p("everyTaFields").toString.split(",").length==7,"seven temperature fields required")
      p.get("everyPvaporFields").foreach(x=>require(x.toString.split(",").length==7,"seven vapor fields required"))
      require(p("time_interval").toString.toInt>0,"positive interval in seconds required")
    }
    if(className.contains("WildSpotCulling")){
      require(!p("fields").toString.contains(",") && p("fields").toString.nonEmpty,"this outlier adapter accepts one field per step")
      require(Set("stddev","variance").contains(p("statisticalType").toString),"statisticalType must be stddev or variance")
      require(p("multiple").toString.toDouble>=0,"multiple must be nonnegative")
      require(p("flagValue").toString.toDouble.isFinite,"flagValue must be finite")
    }
    p.get("mdv_num").foreach { value=>
      val window=value.toString.toInt
      require(window>0 && (!className.endsWith("MDV_G") || window%2==1),"positive window required; sliding MDV requires odd window")
    }
    val spark=SparkSession.builder().master("local[2]").appName("DACP EcoFlow")
      .config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1")
      .config("spark.driver.bindAddress","127.0.0.1").config("spark.sql.shuffle.partitions","2")
      .config("spark.sql.adaptive.enabled","false").config("spark.sql.autoBroadcastJoinThreshold","-1")
      .config("spark.rpc.netty.dispatcher.numThreads","2").config("spark.shuffle.io.serverThreads","2")
      .config("spark.shuffle.io.clientThreads","2")
      .config("spark.sql.session.timeZone","UTC").config("spark.sql.ansi.enabled","false")
      .config("spark.sql.legacy.allowUntypedScalaUDF","true").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val allocator=new RootAllocator()
    try {
      val inputs=ctx.inputNames.map { name=>
        val in=ctx.input(name);require(in.representation=="records","records input required")
        val schema=ArrowUtils.fromArrowSchema(in.schema)
        val projection=UnsafeProjection.create(schema)
        val rows=ArrayBuffer.empty[InternalRow];var bytes=0L
        in.foreachRecords { root=>
          bytes+=root.getFieldVectors.asScala.map(_.getBufferSize.toLong).sum
          require(bytes<=128L*1024*1024 && rows.size+root.getRowCount<=1000000,"Spark input exceeds 128 MiB or one million rows")
          val batch=new ColumnarBatch(root.getFieldVectors.asScala.map(v=>new ArrowColumnVector(v)).toArray,root.getRowCount)
          batch.rowIterator().asScala.foreach(row=>rows+=projection(row).copy())
          // The SDK owns and closes the vectors; do not close the borrowed ColumnarBatch.
        }
        val classic=spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
        ports(name)->classic.internalCreateDataFrame(spark.sparkContext.parallelize(rows.toVector,2),schema)
      }.toMap
      val input=new JobInputStream {
        def read():DataFrame={require(inputs.size==1,"named input required");inputs.values.head}
        def read(port:String):DataFrame=inputs.getOrElse(port,throw new IllegalArgumentException("unknown upstream input: "+port))
      }
      var result:Option[DataFrame]=None
      val output=new JobOutputStream { def write(df:DataFrame):Unit={require(result.isEmpty,"single output expected");result=Some(df)} }
      val context=new JobContext { def get[T]()(implicit m:Manifest[T]):T={require(m.runtimeClass==classOf[SparkSession]);spark.asInstanceOf[T]} }
      stop.perform(input,output,context)
      val df=result.getOrElse(throw new IllegalStateException("upstream operator produced no result"))
      val schema=ArrowUtils.toArrowSchema(df.schema,"UTC",true,false)
      val out=ctx.recordsOutput("result",schema)
      // Spark execution iterators reuse UnsafeRow storage; copy before partition collection.
      val batches=ArrowConverters.toBatchWithSchemaIterator(df.queryExecution.toRdd.map(_.copy()).toLocalIterator,
        df.schema,1024,1024*1024,"UTC",true,false)
      try while(batches.hasNext){
        val reader=new ArrowStreamReader(new ByteArrayInputStream(batches.next()),allocator)
        try while(reader.loadNextBatch())out.write(reader.getVectorSchemaRoot)finally reader.close()
      }finally batches.close()
    }finally {allocator.close();spark.stop()}
  }
}
