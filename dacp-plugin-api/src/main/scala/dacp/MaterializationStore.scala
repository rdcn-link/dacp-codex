package dacp

import _root_.xstream.data._

import com.fasterxml.jackson.databind.JsonNode

/** A task's selected output ports become visible together, only on commit.
  * Chunks are opaque (at most 9 MiB); readers must validate integrity and EOF.
  * Closing an uncommitted writer aborts it. Open artifacts are leased until close.
  */
trait MaterializationStore extends AutoCloseable {
  def begin(key:String,metadata:JsonNode,ports:Set[String]):MaterializationWriter
  def get(key:String):Option[MaterializedTask]
  def cleanup():Unit
}
trait MaterializationWriter extends AutoCloseable {
  def append(port:String,chunk:Array[Byte]):Unit
  def commit():Unit
}
trait MaterializedTask extends AutoCloseable {
  def metadata:JsonNode
  def open(port:String):MaterializedChunks
}
trait MaterializedChunks extends AutoCloseable { def next():Option[Array[Byte]] }
trait MaterializationStoreProvider extends Plugin {
  def open(settings:Map[String,Object]):MaterializationStore
}
case class MaterializationStoreDefinition(provider:String,settings:Map[String,Object]) {
  override def toString:String=s"MaterializationStoreDefinition($provider,<private settings>)"
}
