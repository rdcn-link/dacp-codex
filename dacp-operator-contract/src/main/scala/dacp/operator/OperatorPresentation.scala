package dacp.operator


import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Portable presentation metadata. Icons are local 24×24 SVG paths, never executable SVG or URLs. */
object OperatorPresentation {
  def validateCategories(n:JsonNode,required:Boolean):Unit={
    require(n!=null && n.isArray && n.size()<=16 && (!required || n.size()>0),"categories must contain 1–16 labels")
    val labels=n.elements.asScala.toVector
    require(labels.forall(v=>v.isTextual && v.asText.nonEmpty && v.asText==v.asText.trim && v.asText.length<=80 && !v.asText.exists(Character.isISOControl)),"invalid category label")
    require(labels.map(_.asText.toLowerCase(java.util.Locale.ROOT)).distinct.size==labels.size,"duplicate category labels")
  }
  def validate(n: JsonNode): Unit = {
    require(n != null && n.isObject, "operator metadata must be an object")
    require(!n.has("category"), "category was removed; repository administrators assign categories")
    if(n.has("categories")) validateCategories(n.get("categories"),required=true)
    val icon = n.path("icon")
    require(icon.isObject && icon.fieldNames.asScala.toSet == Set("path", "color"), "icon requires path and color")
    val path = icon.path("path")
    require(path.isTextual && path.asText.length <= 2048 && path.asText.matches("[Mm][0-9\\s.,+eE\\-MmLlHhVvCcSsQqTtAaZz]+") && path.asText.exists(_.isDigit), "icon.path must be a 24x24 SVG path (maximum 2048 characters)")
    require(icon.path("color").isTextual && icon.path("color").asText.matches("#[0-9a-fA-F]{6}"), "icon.color must be #RRGGBB")
  }
}
