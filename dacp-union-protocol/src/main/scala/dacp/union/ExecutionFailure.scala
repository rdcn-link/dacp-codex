package dacp.union

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

/** Public diagnostics deliberately exclude exception messages, paths and stack traces. */
object ExecutionFailure {
  private val Missing="operator not found locally or in configured libraries: ([A-Za-z0-9_-]+@[A-Za-z0-9_.-]+)".r
  private val fields=Vector("code","phase","message","request_id","execution_id","step_id","operator","node")
  def publicCopy(value:JsonNode):ObjectNode={
    val out=Json.obj()
    fields.foreach(k=>if(value.path(k).isTextual)out.put(k,value.path(k).asText.take(512)))
    out
  }
  def summary(t:Throwable,phase:String,request:String,execution:String):ObjectNode={
    val out=Json.obj().put("code","EXECUTION_FAILED").put("phase",phase)
      .put("message",phase match {
        case "preparation"=>"Runtime environment preparation failed"
        case "commit"=>"Result commit failed; automatic retry is forbidden"
        case _=>"Step execution failed"
      }).put("request_id",request).put("execution_id",execution)
    var cause=t;var depth=0
    while(cause!=null && depth<16){
      Option(cause.getMessage).getOrElse("").stripPrefix("requirement failed: ") match {
        case Missing(operator)=>out.put("code","OPERATOR_NOT_FOUND").put("message","Operator is unavailable in the configured operator libraries: "+operator).put("operator",operator)
        case _=>()
      }
      cause=cause.getCause;depth+=1
    }
    out
  }
}
