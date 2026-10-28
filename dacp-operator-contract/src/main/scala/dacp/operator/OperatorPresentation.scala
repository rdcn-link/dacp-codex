package dacp.operator

import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._

/** Portable presentation metadata. Icons are local 24×24 SVG paths, never executable SVG or URLs. */
object OperatorPresentation {
  def validate(n: JsonNode): Unit = {
    require(n != null && n.isObject, "operator metadata must be an object")
    val category = n.path("category")
    require(category.isTextual && category.asText.nonEmpty && category.asText == category.asText.trim &&
      category.asText.length <= 80 && !category.asText.exists(Character.isISOControl), "category must be a nonempty label of at most 80 characters")
    val icon = n.path("icon")
    require(icon.isObject && icon.fieldNames.asScala.toSet == Set("path", "color"), "icon requires path and color")
    val path = icon.path("path")
    require(path.isTextual && path.asText.length <= 2048 && path.asText.matches("[Mm][0-9\\s.,+eE\\-MmLlHhVvCcSsQqTtAaZz]+") && path.asText.exists(_.isDigit), "icon.path must be a 24x24 SVG path (maximum 2048 characters)")
    require(icon.path("color").isTextual && icon.path("color").asText.matches("#[0-9a-fA-F]{6}"), "icon.color must be #RRGGBB")
  }
}
