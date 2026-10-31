package dacp

import _root_.xstream.data._

import com.fasterxml.jackson.databind.JsonNode

/** A task's selected output ports become visible together, only on commit.
  * Data is stored as ordered Arrow IPC File segments; replay validates integrity and EOF.
  * Closing an uncommitted writer aborts it. Open artifacts are leased until close.
  */
trait MaterializationStore extends AutoCloseable {
  def begin(key:String,metadata:JsonNode,ports:Set[String]):MaterializationWriter
  def get(key:String):Option[MaterializedTask]
  def cleanup():Unit
}
trait MaterializationWriter extends AutoCloseable {
  def append(port:String,frame:OperatorFrame):Unit
  def commit():Unit
}
trait MaterializedTask extends AutoCloseable {
  def metadata:JsonNode
  def open(port:String):MaterializedFrames
}
trait MaterializedFrames extends AutoCloseable { def next():Option[OperatorFrame] }
trait MaterializationStoreProvider extends Plugin {
  def open(settings:Map[String,Object]):MaterializationStore
}
case class MaterializationStoreDefinition(provider:String,settings:Map[String,Object]) {
  override def toString:String=s"MaterializationStoreDefinition($provider,<private settings>)"
}
