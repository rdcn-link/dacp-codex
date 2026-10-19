package dacp

import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Composes namespaces without inspecting backend types or changing representations. */
class MountedDatasetProvider extends DatasetProvider {
  val id="mounted"
  override val itemSettingsKey="mounts"
  def create(definition:DatasetDefinition,context:DatasetContext):PublishedDataset={
    require(definition.source.isEmpty,"mounted source must be empty")
    val paths=definition.items.keys.toVector.sorted
    paths.foreach(p=>require(LogicalPath.validate(p)!="/","cannot mount at root"))
    require(!paths.exists(a=>paths.exists(b=>a!=b && b.startsWith(a+"/"))),"overlapping mounts")
    val children=scala.collection.mutable.ArrayBuffer.empty[(String,String,PublishedDataset)]
    try paths.foreach { prefix=>
      val settings=definition.items(prefix)
      require(settings.keySet.subsetOf(Set("dataset","root")) && settings.contains("dataset"),"mount requires dataset and optional root")
      val raw=settings("dataset").asInstanceOf[java.util.Map[String,Object]].asScala.toMap
      val root=LogicalPath.validate(settings.get("root").map(_.toString).getOrElse("/"))
      children+=((prefix,root,context.createDataset(context.plugins.datasetDefinition(raw))))
    } catch {case scala.util.control.NonFatal(t)=>children.reverseIterator.foreach(c=>try c._3.close()catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)});throw t}
    new PublishedDataset {
      def open(check:()=>Unit):DatasetAccess=new DatasetAccess {
        private val opened=scala.collection.mutable.Map.empty[String,DatasetAccess]
        private def access(prefix:String,published:PublishedDataset):DatasetAccess=opened.getOrElseUpdate(prefix,{check();published.open(check)})
        private val folders=(Set("/")++paths.flatMap { p=>Iterator.iterate(LogicalPath.parent(p))(LogicalPath.parent).takeWhile(_!="/").toVector }).toVector.sorted
        private def folder(p:String)=Item(p,"",None,kind="folder",views=Some(Seq.empty))
        private def route(path:String):(DatasetAccess,String,String,String)={
          LogicalPath.validate(path);check()
          val (prefix,root,published)=children.find(c=>path==c._1 || path.startsWith(c._1+"/")).getOrElse(throw new java.io.FileNotFoundException("logical resource unavailable"))
          val suffix=path.substring(prefix.length)
          (access(prefix,published),(if(root=="/")suffix else root+suffix) match {case ""=>"/";case p=>p},prefix,root)
        }
        private def translate(item:Item,prefix:String,root:String):Item={
          LogicalPath.validate(item.path)
          require(root=="/" || item.path==root || item.path.startsWith(root+"/"),"plugin returned item outside mounted root")
          val suffix=if(root=="/"){if(item.path=="/")"" else item.path}else item.path.substring(root.length)
          item.copy(path=prefix+suffix)
        }
        def describe(path:String):Item=if(folders.contains(path))folder(path)else {val(a,p,prefix,root)=route(path);translate(a.describe(p),prefix,root)}
        def list():Seq[Item]={
          check()
          val result=folders.filterNot(_=="/").map(folder)++children.toVector.flatMap {case(prefix,root,published)=>
            val a=access(prefix,published);val parent=a.describe(root)
            Seq(translate(parent,prefix,root))++(if(parent.kind=="folder")a.list().filter(i=>i.path!=root && (root=="/" || i.path.startsWith(root+"/"))).map(translate(_,prefix,root))else Seq.empty)
          }
          require(result.map(_.path).distinct.size==result.size,"duplicate mounted item")
          result.sortBy(_.path)
        }
        def list(path:String):Seq[Item]={require(describe(path).kind=="folder","listing requires a folder");list().filter(i=>LogicalPath.parent(i.path)==path)}
        def count(path:String):Option[Long]={val(a,p,_,_)=route(path);a.count(p)}
        override def scan(path:String,request:Scan,allocator:BufferAllocator,listener:ServerStreamListener,ready:()=>Unit):Unit={val(a,p,_,_)=route(path);a.scan(p,request,allocator,listener,ready)}
        def stream(path:String,view:String,allocator:BufferAllocator,listener:ServerStreamListener,ready:()=>Unit):Unit={val(a,p,_,_)=route(path);a.stream(p,view,allocator,listener,ready)}
        override def close():Unit={var error:Throwable=null;opened.values.toVector.reverse.foreach(a=>try a.close()catch{case scala.util.control.NonFatal(t)=>if(error==null)error=t else error.addSuppressed(t)});opened.clear();if(error!=null)throw error}
      }
      override def close():Unit={var error:Throwable=null;children.reverseIterator.foreach(c=>try c._3.close()catch{case scala.util.control.NonFatal(t)=>if(error==null)error=t else error.addSuppressed(t)});children.clear();if(error!=null)throw error}
    }
  }
}
