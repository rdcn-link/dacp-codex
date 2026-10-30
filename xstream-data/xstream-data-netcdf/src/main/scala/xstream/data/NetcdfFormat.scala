package xstream.data.netcdf

import _root_.xstream.data._
import java.nio.{ByteBuffer,ByteOrder}
import ucar.nc2.{NetcdfFiles,Group,Variable,Attribute}
import scala.jdk.CollectionConverters._

final class NetcdfFormat extends StructuredFileHandler {
  val id="netcdf";val extensions=Set("nc","nc4","cdf")
  override def signatureMatches(b:Array[Byte]):Boolean=b.length>=4 && b(0)=='C' && b(1)=='D' && b(2)=='F'
  override def validateOptions(o:FileOptions):Unit={
    require(o.columns.isEmpty && o.parameters.keySet.subsetOf(Set("chunk_bytes")),"invalid NetCDF options")
    o.parameters.get("chunk_bytes").foreach(v=>require(v.isInstanceOf[Number] && v.asInstanceOf[Number].longValue>=8 && v.asInstanceOf[Number].longValue<=DataViewCodecs.MaxBlockBytes,"invalid NetCDF chunk size"))
  }
  private def encoding(v:Variable):Option[NumericEncoding]={
    val types=Map("byte"->"int8","ubyte"->"uint8","short"->"int16","ushort"->"uint16","int"->"int32","uint"->"uint32","long"->"int64","ulong"->"uint64","float"->"float32","double"->"float64","enum1"->"uint8","enum2"->"uint16","enum4"->"uint32")
    types.get(v.getDataType.toString.toLowerCase(java.util.Locale.ROOT)).map(NumericEncoding(_,ByteOrder.LITTLE_ENDIAN))
  }
  private def attributes(attrs:Iterable[Attribute]):Map[String,com.fasterxml.jackson.databind.JsonNode]=attrs.map { a=>
    val n=Data.mapper.createArrayNode()
    (0 until a.getLength).foreach { i=>if(a.isString)n.add(a.getStringValue(i))else n.add(Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](a.getNumericValue(i))) }
    a.getShortName->(if(a.getLength==1)n.get(0)else n)
  }.toMap
  def openStructure(source:ByteSource,options:FileOptions,context:DatasetContext,check:()=>Unit):DatasetAccess={
    validateOptions(options);check()
    val lease=ByteSource.materialize(source,check)
    val file=try NetcdfFiles.open(lease.path.toString)catch{case t:Throwable=>lease.close();throw t}
    try {
      val chunkBytes=options.parameters.get("chunk_bytes").map(_.asInstanceOf[Number].intValue).getOrElse(65536)
      val groups=scala.collection.mutable.ArrayBuffer.empty[Group]
      def collect(g:Group):Unit={check();groups+=g;g.getGroups.asScala.foreach(collect)}
      collect(file.getRootGroup)
      val variables=groups.flatMap(_.getVariables.asScala).map(v=>{v.setCaching(false);("/"+v.getFullName)->v}).toMap
      def dims(v:Variable):Vector[Dimension]=v.getDimensions.asScala.zipWithIndex.map{case(d,i)=>
        val name=Option(d.getShortName).filter(_.nonEmpty).getOrElse("axis"+i)
        val coordinate=Option(v.getParentGroup.findVariableOrInParent(name)).filter(c=>c.getRank==1 && c.getDimension(0)==d)
        Dimension(name,d.getLength.toLong,coordinate.map(c=>"/"+c.getFullName),coordinate.flatMap(c=>Option(c.findAttributeString("units",null))))
      }.toVector
      new StructuredDataAccess(source,context.byteChunk) {
        val entries=groups.filter(_!=file.getRootGroup).map(g=>Item("/"+g.getFullName,"netcdf-group",None,"folder",views=Some(Seq.empty))).toVector ++
          variables.toVector.sortBy(_._1).map{case(path,v)=>Item(path,"netcdf-variable",None,views=Some(if(encoding(v).nonEmpty)Seq("array")else Seq.empty))}
        def content[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]=variables.get(path).flatMap { v=>encoding(v).flatMap { e=>
          DataViews.when(kind,ScientificViews.Arrays)(DataViews[ArrayChunk,ArraySelection]((request,c,out)=>{
            check();c.check()
            val dimensions=dims(v)
            var attrs=attributes(v.getAttributes.asScala)
            val groupAttrs=Data.mapper.createObjectNode();var group:Group=v.getParentGroup
            while(group!=null){val a=Data.mapper.createObjectNode();attributes(group.getAttributes.asScala).foreach{case(k,v)=>a.set[com.fasterxml.jackson.databind.JsonNode](k,v)};groupAttrs.set[com.fasterxml.jackson.databind.JsonNode]("/"+group.getFullName,a);group=group.getParentGroup}
            attrs=attrs ++ Map("group_attributes"->groupAttrs,"coordinate_scope"->Data.mapper.getNodeFactory.textNode("file"),"values"->Data.mapper.getNodeFactory.textNode("raw"))
            if(v.getEnumTypedef!=null)attrs += "enum_values"->Data.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](v.getEnumTypedef.getMap)
            out.start(ArrayDescriptor(dimensions,e,attrs))
            ArrayBlocks.sections(ArrayBlocks.selected(request,dimensions),dimensions,e,chunkBytes){ section=>
              check();c.check()
              val ranges=section.origin.indices.map(i=>new ucar.ma2.Range(Math.toIntExact(section.origin(i)),Math.toIntExact(section.origin(i)+(section.shape(i)-1)*section.stride(i)),Math.toIntExact(section.stride(i))))
              val array=if(ranges.isEmpty)v.read()else v.read(ranges.asJava)
              val buffer=ByteBuffer.allocate(Math.toIntExact(array.getSize*e.bytesPerValue)).order(e.byteOrder)
              val iterator=array.getIndexIterator
              while(iterator.hasNext){val n=iterator.getObjectNext.asInstanceOf[Number];e.dataType match {
                case "int8"|"uint8"=>buffer.put(n.byteValue)
                case "int16"|"uint16"=>buffer.putShort(n.shortValue)
                case "int32"|"uint32"=>buffer.putInt(n.intValue)
                case "int64"|"uint64"=>buffer.putLong(n.longValue)
                case "float32"=>buffer.putFloat(n.floatValue)
                case "float64"=>buffer.putDouble(n.doubleValue)
              }}
              buffer.flip();c.check();out.accept(ArrayChunk(section,e,buffer))
            }
          }))
        }}
        override def close():Unit=try file.close()finally lease.close()
      }
    }catch{case t:Throwable=>try file.close()finally lease.close();throw t}
  }
}
