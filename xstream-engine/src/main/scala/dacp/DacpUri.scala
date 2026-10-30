package dacp

import java.net.URI
object DacpUri {
  def parse(value: String, datasetRequired: Boolean = true): URI = {
    val u = new URI(value)
    require(u.getScheme == "dacp" && u.getHost != null && u.getUserInfo == null && u.getQuery == null && u.getFragment == null,
      "expected dacp://host[:port]/dataset[/item]")
    require(u.getPort == -1 || (u.getPort > 0 && u.getPort <= 65535),"invalid node port")
    val path = Option(u.getPath).getOrElse("")
    require(!path.contains("\\") && !path.contains("\u0000"),"invalid URI path")
    val segments = path.stripPrefix("/").stripSuffix("/").split("/",-1)
    require(!segments.exists(s => s == "." || s == ".."),"invalid URI path")
    if (datasetRequired) require(segments.head.nonEmpty && !segments.contains(""),"missing dataset or empty path segment")
    u
  }
  def origin(value: String): String = {
    val u = parse(value,false)
    val host = u.getHost.toLowerCase(java.util.Locale.ROOT)
    val authority = if (host.contains(":") && !host.startsWith("[")) s"[$host]" else host
    s"dacp://$authority:${if (u.getPort < 0) 8815 else u.getPort}"
  }
  def node(value: String): String = {
    val u = parse(value,false)
    require(u.getPath == "" || u.getPath == "/","node must be a dacp URI without dataset path")
    origin(value)
  }
  def canonical(value: String): String = {
    val u = parse(value)
    origin(value) + u.getRawPath.stripSuffix("/")
  }
  def within(value: String, prefix: String): Boolean = {
    val v = canonical(value)
    val root = parse(prefix,false)
    if (root.getPath == "" || root.getPath == "/") origin(value) == origin(prefix)
    else { val p = canonical(prefix); v == p || v.startsWith(p + "/") }
  }
}
