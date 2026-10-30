package dacp


import com.fasterxml.jackson.databind.JsonNode
import dacp.union.{Json,YamlDocument}
import scala.jdk.CollectionConverters._

/** Read-only projection of stored scripts; never validates or executes operator parameters. */
object RecordedJobGraph {
  def apply(script:String):JsonNode={
    val document=YamlDocument.parse(script)
    require(document.isObject && document.path("tasks").isArray,"Invalid recorded Job")
    val graph=Json.obj();val tasks=graph.putArray("tasks")
    document.path("tasks").elements.asScala.foreach { source=>
      val task=tasks.addObject().put("id",source.path("id").asText).put("operator",source.path("operator").asText)
      if(source.has("title"))task.set[JsonNode]("title",source.path("title"))
      task.set[JsonNode]("params",source.path("params"));val inputs=task.putArray("inputs")
      source.path("inputs").fields.asScala.foreach { e=>
        val (producer,port)=Jobs.reference(e.getValue.asText)
        inputs.addObject().put("port",e.getKey).put("task",producer).put("output_port",port)
      }
    }
    val outputs=graph.putObject("outputs")
    document.path("outputs").fields.asScala.foreach { e=>
      val (task,port)=Jobs.reference(e.getValue.asText)
      outputs.putObject(e.getKey).put("task",task).put("port",port)
    }
    graph
  }
}
