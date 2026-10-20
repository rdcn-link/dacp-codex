package dacp

import java.net.URLClassLoader
import java.nio.file.{Files,Path}
import java.util.ServiceLoader
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Implementations are trusted code. Initialization and closure are scoped to an environment. */
trait Plugin extends AutoCloseable {
  def id: String
  def initialize(environment: PluginEnvironment): Unit = ()
  def close(): Unit = ()
}
case class PluginRegistration(plugin: Plugin, owned: Boolean = false)

/** One registry per server/application. No thread-context classloader or JVM-global state is changed.
  * Explicit registrations replace discovered implementations with the same contract and ID.
  * Injected plugins are application-owned unless marked owned; discovered plugins are owned here.
  */
final class PluginEnvironment(directory: Option[Path] = None,
                              registrations: Seq[PluginRegistration] = Seq.empty,
                              parent: ClassLoader = classOf[Plugin].getClassLoader) extends AutoCloseable {
  private val loader: Option[URLClassLoader] = directory.map { raw =>
    val path=raw.toRealPath();require(Files.isDirectory(path),"plugin directory must be a directory")
    val stream=Files.list(path)
    val jars=try stream.iterator.asScala.filter(p=>Files.isRegularFile(p) && p.toString.endsWith(".jar")).toVector.sortBy(_.toString) finally stream.close()
    new URLClassLoader(jars.map(_.toUri.toURL).toArray,parent)
  }
  val classLoader: ClassLoader = loader.getOrElse(parent)
  private val cache=mutable.Map.empty[Class[_],Vector[Plugin]]
  private val owned=mutable.ArrayBuffer.empty[Plugin] ++ registrations.filter(_.owned).map(_.plugin)
  private val initialized=new java.util.IdentityHashMap[Plugin,java.lang.Boolean]()
  private var closed=false
  private def initialize(p:Plugin,own:Boolean):Unit={
    if(!initialized.containsKey(p)) {
      initialized.put(p,true)
      if(own)owned+=p // Failed initialization must also roll back allocated resources.
      p.initialize(this)
    }
  }
  def services[A <: Plugin](contract: Class[A]): Vector[A] = synchronized {
    require(!closed,"plugin environment is closed")
    cache.getOrElseUpdate(contract,{
      val injected=registrations.filter(r=>contract.isInstance(r.plugin))
      require(injected.map(_.plugin.id).distinct.size==injected.size,"duplicate injected plugin id")
      val replaced=injected.map(_.plugin.id).toSet
      val discovered=ServiceLoader.load(contract,classLoader).iterator.asScala.toVector
      // Even a shadowed discovery instance may own constructor resources.
      discovered.foreach(p=>if(!owned.exists(_ eq p))owned+=p)
      val selected=discovered.filterNot(p=>replaced(p.id)) ++ injected.map(_.plugin)
      require(selected.forall(_.id.matches("[a-z][a-z0-9-]*")),"invalid plugin id")
      require(selected.map(_.id).distinct.size==selected.size,"duplicate plugin id")
      try {
        selected.foreach(p=>initialize(p,discovered.exists(_ eq p) || injected.exists(r=>(r.plugin eq p) && r.owned)))
        selected.toVector
      } catch {case scala.util.control.NonFatal(t)=>try close() catch{case scala.util.control.NonFatal(e)=>t.addSuppressed(e)};throw t}
    }).asInstanceOf[Vector[A]]
  }
  def byId[A <: Plugin](contract:Class[A],id:String):A = services(contract).find(_.id==id)
    .getOrElse(throw new IllegalArgumentException(s"unsupported ${contract.getSimpleName} provider: $id"))
  lazy val containers=new Containers(this)
  lazy val formats=new Formats(this)
  def views(item:Item):Seq[String]=item.views.getOrElse(if(item.format.isEmpty)Seq.empty else formats.views(item.format))
  def datasetDefinition(raw:Map[String,Object]):DatasetDefinition={
    require(raw.get("provider").exists(_.isInstanceOf[String]),"dataset provider is required")
    val provider=raw("provider").toString
    val key=byId(classOf[DatasetProvider],provider).itemSettingsKey
    require(key.matches("[a-z][a-z0-9_]*") && !Set("provider","source","metadata")(key),"invalid dataset settings key")
    require(raw.keySet.subsetOf(Set("provider","source","metadata",key)),"unknown dataset setting")
    def mapping(v:Object):Map[String,Object]=v match {
      case m:java.util.Map[_,_]=>m.asScala.map{case(k:String,value)=>k->value.asInstanceOf[Object];case _=>throw new IllegalArgumentException("settings keys must be strings")}.toMap
      case _=>throw new IllegalArgumentException("settings must be mappings")
    }
    val source=raw.get("source").map(mapping).getOrElse(Map.empty)
    val items=raw.get(key).map(mapping).getOrElse(Map.empty).map{case(path,value)=>path->mapping(value)}
    val metadata=raw.get("metadata").map { value=>
      val m=mapping(value);require(m.keySet.subsetOf(Set("title","description","tags")),"unknown public dataset metadata")
      def text(key:String)=m.get(key).map { v=>require(v.isInstanceOf[String],"dataset metadata text must be a string");v.toString }.getOrElse("")
      val tags=m.get("tags").map {case values:java.util.List[_]=>values.asScala.map { v=>require(v.isInstanceOf[String],"dataset tags must be strings");v.toString }.toVector;case _=>throw new IllegalArgumentException("dataset tags must be a list")}.getOrElse(Vector.empty)
      DatasetMetadata(text("title"),text("description"),tags)
    }.getOrElse(DatasetMetadata())
    DatasetDefinition(provider,source,items,metadata)
  }
  def close():Unit=synchronized {
    if(!closed){
      closed=true
      var failure:Throwable=null
      val seen=new java.util.IdentityHashMap[Plugin,java.lang.Boolean]()
      owned.reverseIterator.foreach(p=>if(!seen.containsKey(p)){seen.put(p,true);try p.close()catch{case scala.util.control.NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)}})
      try loader.foreach(_.close())catch{case scala.util.control.NonFatal(e)=>if(failure==null)failure=e else failure.addSuppressed(e)}
      cache.clear()
      if(failure!=null)throw failure
    }
  }
}
