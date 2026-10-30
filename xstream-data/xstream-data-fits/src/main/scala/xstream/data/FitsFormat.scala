package xstream.data.fits

import _root_.xstream.data._
import nom.tam.fits.{Fits,BasicHDU,ImageHDU,BinaryTableHDU}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.{Schema,Field,FieldType,ArrowType}
import scala.jdk.CollectionConverters._
import java.nio.{ByteBuffer,ByteOrder}

/** Uncompressed random-access HDUs; payload values retain FITS raw scaling/null metadata. */
final class FitsFormat extends StructuredFileHandler {
  val id="fits";val extensions=Set("fits","fit","fts")
  override def signatureMatches(b:Array[Byte]):Boolean=new String(b,java.nio.charset.StandardCharsets.US_ASCII).startsWith("SIMPLE")
  override def validateOptions(o:FileOptions):Unit={require(o.columns.isEmpty && o.parameters.keySet.subsetOf(Set("chunk_bytes")),"invalid FITS options");o.parameters.get("chunk_bytes").foreach(v=>require(v.isInstanceOf[Number] && v.asInstanceOf[Number].longValue>=8 && v.asInstanceOf[Number].longValue<=DataViewCodecs.MaxBlockBytes,"invalid FITS chunk size"))}
  private def encoding(h:ImageHDU):NumericEncoding=NumericEncoding(h.getBitPix match {case 8=>"uint8";case 16=>"int16";case 32=>"int32";case 64=>"int64";case -32=>"float32";case -64=>"float64";case _=>throw new IllegalArgumentException("unsupported FITS BITPIX")},ByteOrder.BIG_ENDIAN)
  private def attrs(h:BasicHDU[_],index:Int):Map[String,com.fasterxml.jackson.databind.JsonNode]={
    val cards=Data.mapper.createArrayNode();val it=h.getHeader.iterator()
    while(it.hasNext)cards.add(it.next().toString)
    Map("header_cards"->cards,"hdu_index"->Data.mapper.getNodeFactory.numberNode(index),"values"->Data.mapper.getNodeFactory.textNode("raw"))
  }
  private def scalar(c:Class[_]):ArrowType=if(c==java.lang.Byte.TYPE)new ArrowType.Int(8,false)else if(c==java.lang.Short.TYPE)new ArrowType.Int(16,true)else if(c==java.lang.Integer.TYPE)new ArrowType.Int(32,true)else if(c==java.lang.Long.TYPE)new ArrowType.Int(64,true)else if(c==java.lang.Float.TYPE)new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)else if(c==java.lang.Double.TYPE)new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)else if(c==java.lang.Boolean.TYPE)new ArrowType.Bool()else if(c==classOf[String])new ArrowType.Utf8()else throw new IllegalArgumentException("unsupported FITS column type")
  private def tableSchema(h:BinaryTableHDU,index:Int,limit:Int):Option[Schema]=try {
    val fields=(0 until h.getNCols).map { i=>
      val d=h.getColumnDescriptor(i)
      require(!d.isVariableSize && !d.isComplex && d.rowLen<=limit,"unsupported or oversized FITS table cell")
      val name=Option(h.getColumnName(i)).filter(_.nonEmpty).getOrElse("column"+i)
      val base=if(d.isString)classOf[String]else d.getBase
      val shape=if(d.isString && d.isSingleton)Vector.empty else d.getEntryShape.toVector
      def field(n:String,depth:Int):Field=if(depth<shape.size)new Field(n,FieldType.nullable(new ArrowType.List()),List(field("item",depth+1)).asJava)else new Field(n,FieldType.nullable(scalar(base)),null)
      field(name,0)
    }
    require(fields.nonEmpty && fields.map(_.getName).distinct.size==fields.size,"invalid FITS table columns")
    Some(new Schema(fields.asJava,Map("fits.metadata"->Data.mapper.writeValueAsString(attrs(h,index).asJava)).asJava))
  }catch{case _:IllegalArgumentException=>None}
  private def write(v:FieldVector,row:Int,value:Any):Unit={
    if(value==null){v.setNull(row);return}
    v match {
      case l:ListVector=>
        require(value.getClass.isArray,"FITS cell shape does not match schema")
        val count=java.lang.reflect.Array.getLength(value);val at=l.startNewValue(row)
        for(i<-0 until count)write(l.getDataVector,at+i,java.lang.reflect.Array.get(value,i))
        l.endValue(row,count)
      case x:TinyIntVector=>x.setSafe(row,value.asInstanceOf[Number].byteValue)
      case x:UInt1Vector=>x.setSafe(row,value.asInstanceOf[Number].byteValue & 255)
      case x:SmallIntVector=>x.setSafe(row,value.asInstanceOf[Number].shortValue)
      case x:IntVector=>x.setSafe(row,value.asInstanceOf[Number].intValue)
      case x:BigIntVector=>x.setSafe(row,value.asInstanceOf[Number].longValue)
      case x:Float4Vector=>x.setSafe(row,value.asInstanceOf[Number].floatValue)
      case x:Float8Vector=>x.setSafe(row,value.asInstanceOf[Number].doubleValue)
      case x:BitVector=>x.setSafe(row,if(value.asInstanceOf[Boolean])1 else 0)
      case x:VarCharVector=>x.setSafe(row,value.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      case _=>throw new IllegalArgumentException("unsupported FITS table vector")
    }
  }
  def openStructure(source:ByteSource,options:FileOptions,context:DatasetContext,check:()=>Unit):DatasetAccess={
    validateOptions(options);check();val lease=ByteSource.materialize(source,check)
    val file=try new Fits(lease.path.toFile)catch{case t:Throwable=>lease.close();throw t}
    try {
      val maxBytes=options.parameters.get("chunk_bytes").map(_.asInstanceOf[Number].intValue).getOrElse(65536)
      val hdus=scala.collection.mutable.ArrayBuffer.empty[BasicHDU[_]];var next=file.readHDU()
      while(next!=null){check();require(hdus.size<10000,"too many FITS HDUs");hdus+=next;next=file.readHDU()}
      val tables=hdus.zipWithIndex.collect{case(h:BinaryTableHDU,i)=>(i,tableSchema(h,i,maxBytes))}.toMap
      def dimensions(h:ImageHDU):Vector[Dimension]={val shape=Option(h.getAxes).map(_.toVector).getOrElse(Vector.empty);shape.zipWithIndex.map{case(n,i)=>val axis=shape.size-i;Dimension(Option(h.getHeader.getStringValue("CTYPE"+axis)).filter(_.nonEmpty).getOrElse("axis"+axis),n.toLong,None,Option(h.getHeader.getStringValue("CUNIT"+axis)))}}
      new StructuredDataAccess(source,context.byteChunk) {
        val entries=hdus.zipWithIndex.map {case(h,i)=>
          val views=h match {case image:ImageHDU if dimensions(image).nonEmpty=>if(dimensions(image).size==2)Seq("array","image")else Seq("array");case _:BinaryTableHDU if tables(i).nonEmpty=>Seq("records");case _=>Seq.empty}
          Item("/hdu"+i,"fits-hdu",tables.get(i).flatten,views=Some(views),recordCount=if(views.contains("records"))Some(h.asInstanceOf[BinaryTableHDU].getNRows.toLong)else None)
        }.toVector
        def content[K,S](path:String,kind:DataViewType[K,S]):Option[DataView[K,S]]={
          val index=path.stripPrefix("/hdu").toInt;val h=hdus(index)
          h match {
            case image:ImageHDU=>
              val dims=dimensions(image);val e=encoding(image);val metadata=attrs(image,index)
              def arrays(request:ArraySelection,c:StreamContext,out:DataConsumer[ArrayChunk]):Unit={
                check();c.check();out.start(ArrayDescriptor(dims,e,metadata))
                ArrayBlocks.sections(ArrayBlocks.selected(request,dims),dims,e,maxBytes){section=>
                  check();c.check()
                  val contiguous=section.stride.forall(_==1)
                  val tile=if(contiguous)image.getTiler.getTile(section.origin.map(Math.toIntExact).toArray,section.shape.map(Math.toIntExact).toArray)else null
                  val count=section.shape.foldLeft(1L)(Math.multiplyExact).toInt;val buffer=ByteBuffer.allocate(Math.multiplyExact(count,e.bytesPerValue)).order(e.byteOrder)
                  for(i<-0 until count){
                    c.check()
                    val n=if(contiguous)java.lang.reflect.Array.get(tile,i).asInstanceOf[Number]else {
                      var linear=i.toLong;val origin=Array.fill(section.shape.size)(0)
                      for(axis<-section.shape.indices.reverse){origin(axis)=Math.toIntExact(section.origin(axis)+(linear%section.shape(axis))*section.stride(axis));linear/=section.shape(axis)}
                      java.lang.reflect.Array.get(image.getTiler.getTile(origin,Array.fill(origin.length)(1)),0).asInstanceOf[Number]
                    }
                    e.dataType match {case "uint8"=>buffer.put(n.byteValue);case "int16"=>buffer.putShort(n.shortValue);case "int32"=>buffer.putInt(n.intValue);case "int64"=>buffer.putLong(n.longValue);case "float32"=>buffer.putFloat(n.floatValue);case "float64"=>buffer.putDouble(n.doubleValue)}}
                  buffer.flip();c.check();out.accept(ArrayChunk(section,e,buffer))
                }
              }
              DataViews.when(kind,ScientificViews.Arrays)(DataViews[ArrayChunk,ArraySelection](arrays)).orElse {
                if(dims.size!=2)None else DataViews.when(kind,ScientificViews.Images)(DataViews[ImageTile,ImageSelection]((selection,c,out)=>{
                  val region=selection.region.getOrElse(ImageRegion(0,0,dims(1).length,dims(0).length))
                  require(BigInt(region.x)+region.width<=dims(1).length && BigInt(region.y)+region.height<=dims(0).length,"image region out of bounds")
                  out.start(ImageDescriptor(dims(1).length,dims(0).length,1,e,metadata))
                  arrays(ArraySelection(Some(ArraySection(Vector(region.y,region.x),Vector(region.height,region.width),Vector(1,1)))),c,new DataConsumer[ArrayChunk]{def start(d:DataDescriptor[ArrayChunk]):Unit=();def accept(b:ArrayChunk):Unit=out.accept(ImageTile(ImageRegion(b.section.origin(1),b.section.origin(0),b.section.shape(1),b.section.shape(0)),1,e,b.values))})
                }))
              }
            case table:BinaryTableHDU=>DataViews.when(kind,DataViews.Records)(DataViews[RecordBatch,Scan]((scan,c,out)=>{
              val schema=tables(index).get
              ScanStreams.stream(scan,c.allocator,BatchStreams.records(out,c.check)){receiver=>
                val root=VectorSchemaRoot.create(schema,c.allocator)
                try {
                  receiver.start(root);var at=0
                  val rowsPerBatch=math.max(1,math.min(context.batchSize,maxBytes/math.max(1,table.getHeader.getIntValue("NAXIS1",1))))
                  while(at<table.getNRows){check();c.check();root.clear();root.allocateNew();val n=math.min(rowsPerBatch,table.getNRows-at)
                    for(row<-0 until n;col<-0 until table.getNCols){c.check();var value:Any=table.getElement(at+row,col);val f=root.getVector(col)
                      if(!f.isInstanceOf[ListVector] && value!=null && value.getClass.isArray){require(java.lang.reflect.Array.getLength(value)==1,"non-scalar FITS cell");value=java.lang.reflect.Array.get(value,0)}
                      write(f,row,value)
                    }
                    root.setRowCount(n);receiver.putNext();at+=n
                  }
                }finally root.close()
              }
            }))
            case _=>None
          }
        }
        override def close():Unit=try file.close()finally lease.close()
      }
    }catch{case t:Throwable=>try file.close()finally lease.close();throw t}
  }
}
