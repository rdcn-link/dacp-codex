package dacp

import _root_.xstream.data._
import com.fasterxml.jackson.databind.JsonNode

/** Atomic persistent metadata operations. Implementations return detached values. */
trait ExecutionStore extends AutoCloseable {
  def get(id:String):Option[JsonNode]
  def records:Vector[JsonNode]
  def create(record:JsonNode):Unit
  def put(record:JsonNode):Unit
}
trait ExecutionStoreProvider extends Plugin {
  def open(settings:Map[String,Object],readOnly:Boolean):ExecutionStore
}
case class ExecutionStoreDefinition(provider:String,settings:Map[String,Object]) {
  override def toString:String=s"ExecutionStoreDefinition($provider,<private settings>)"
}
