package dacp.repository

import com.fasterxml.jackson.databind.JsonNode
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale
import scala.jdk.CollectionConverters._

/** Server-side view models for resource-backed HTML templates. Escape data at each leaf. */
object WebPages {
  def escape(value:String):String=value.flatMap {
    case '&'=>"&amp;";case '<'=>"&lt;";case '>'=>"&gt;";case '"'=>"&quot;";case '\''=>"&#39;";case c=>c.toString
  }
  private val slots="\\{\\{([a-z_][a-z0-9_]*)\\}\\}".r
  private val templates=Seq("layout","index","operator-grid","operator-card","empty","browse-link","detail","version-link","artifact-download","source-review","repository-link","code-block","paragraph","404").map {name=>
    val in=Option(getClass.getResourceAsStream(s"/web/templates/$name.html")).getOrElse(throw new IllegalStateException(s"missing HTML template: $name"))
    try name->new String(in.readAllBytes(),UTF_8)finally in.close()
  }.toMap
  private def render(name:String,fields:(String,String)*):String={
    val values=fields.toMap
    slots.replaceAllIn(templates(name),m=>java.util.regex.Matcher.quoteReplacement(values.getOrElse(m.group(1),throw new IllegalStateException(s"missing template field: ${m.group(1)}"))))
  }
  private def layout(title:String,content:String):String=render("layout","title"->escape(title),"content"->content)
  private def description(item:JsonNode):String=item.path("description").asText("")
  private def icon(item:JsonNode):String={
    dacp.operator.OperatorPresentation.validate(item)
    s"""<svg viewBox="0 0 24 24" aria-hidden="true" color="${escape(item.path("icon").path("color").asText)}"><path d="${escape(item.path("icon").path("path").asText)}"/></svg>"""
  }
  private def trust(item:JsonNode):String={
    val value=item.path("trust").path("score")
    if(!value.isIntegralNumber || value.asInt<0 || value.asInt>5)return "Not assessed"
    val score=value.asInt
    val icons=(0 until 5).map {i=>
      val state=if(i>=5-score)"is-colored"else "is-gray"
      s"""<svg class="trust-icon $state" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2.5 20 6v5.5c0 4.7-3.3 8-8 10-4.7-2-8-5.3-8-10V6z"/><path class="trust-check" d="m8.5 12 2.3 2.3 4.8-5"/></svg>"""
    }.mkString
    s"""<span class="trust-rating" role="img" aria-label="$score/5 · Administrator assessed" title="$score/5 · Administrator assessed">$icons</span>"""
  }
  private def path(item:JsonNode):String=s"/operators/${item.path("name").asText}/${item.path("version").asText}"
  private def bytes(size:Long):String={
    if(size<1024)s"$size B"
    else if(size<1024*1024)f"${size/1024.0}%.1f KiB"
    else if(size<1024L*1024*1024)f"${size/(1024.0*1024)}%.1f MiB"
    else f"${size/(1024.0*1024*1024)}%.1f GiB"
  }
  private def primary(item:JsonNode):JsonNode=Option(item.get("artifact")).getOrElse(item.path("runtime").path("archive"))
  private def download(a:JsonNode):String="/v1/blobs/sha256/"+a.path("sha256").asText+"/"+a.path("filename").asText
  // Natural ordering for version labels: numeric runs compare as integers;
  // a release sorts after its hyphenated prerelease suffix.
  private[repository] val versionOrdering: Ordering[String] = new Ordering[String] {
    def compare(a:String,b:String):Int={
      if(a==b)return 0
      if(a.startsWith(b+"-"))return -1
      if(b.startsWith(a+"-"))return 1
      val left="[0-9]+|[^0-9]+".r.findAllIn(a).toVector
      val right="[0-9]+|[^0-9]+".r.findAllIn(b).toVector
      left.zip(right).foreach {case(x,y)=>
        val c=if(x.head.isDigit && y.head.isDigit)BigInt(x).compare(BigInt(y))else x.compareTo(y)
        if(c!=0)return c
      }
      val length=left.size.compare(right.size)
      if(length!=0)length else a.compareTo(b)
    }
  }

  def index(items:Seq[JsonNode],query:String):String={
    val needle=query.toLowerCase(Locale.ROOT)
    val matches=items.groupBy(_.path("name").asText).toSeq.sortBy(_._1).collect {
      case(_,versions) if versions.exists(item=>Seq(item.path("name").asText,item.path("version").asText,item.path("categories").elements.asScala.map(_.asText).mkString(" · "),description(item)).exists(_.toLowerCase(Locale.ROOT).contains(needle)))=>
        versions.maxBy(_.path("version").asText)(versionOrdering)
    }
    val cards=matches.map {item=>
      val artifact=primary(item)
      render("operator-card","name"->escape(item.path("name").asText),"version"->escape(item.path("version").asText),"link"->escape(path(item)),
        "icon"->icon(item),"category"->escape(item.path("categories").elements.asScala.map(_.asText).mkString(" · ")),"trust"->trust(item),"description"->escape(description(item)),"executor"->escape(item.path("execution").path("executor").asText),"size"->escape(bytes(artifact.path("size").asLong)),
        "download"->escape(download(artifact)),"filename"->escape(artifact.path("filename").asText))
    }.mkString
    val content=if(matches.nonEmpty)render("operator-grid","cards"->cards)else render("empty",
      "heading"->(if(items.isEmpty)"No operators published yet"else "No matching operators"),
      "message"->(if(items.isEmpty)"Published operators will appear here once the repository administrator adds them."else "Try a different name, version, or keyword."),
      "link"->(if(items.isEmpty)""else render("browse-link")))
    val count=s"${matches.size} operator${if(matches.size==1)""else "s"}${if(query.isEmpty)""else s" matching “$query”"}"
    layout("Explore operators",render("index","query"->escape(query),"heading"->(if(query.isEmpty)"All operators"else "Search results"),"count"->escape(count),"catalog"->content))
  }
  private def runtimeAnchor(r:JsonNode):String="runtime-"+r.path("name").asText+"@"+r.path("version").asText
  def detail(item:JsonNode,versions:Seq[JsonNode],runtimeCatalog:Seq[JsonNode]=Seq.empty):String={
    val name=item.path("name").asText;val version=item.path("version").asText;val reference=s"$name@$version";val evidence=item.path("provenance")
    val links=versions.sortBy(_.path("version").asText)(versionOrdering.reverse).map {entry=>render("version-link",
      "link"->escape(path(entry)),"version"->escape(entry.path("version").asText),"selected"->(if(entry.path("version").asText==version)" aria-current=\"page\""else ""))}.mkString(" ")
    val artifacts=Option(item.get("artifact")).map("Operator package"->_).toSeq++
      Seq("Source archive"->"source_archive","License text"->"license_file","Dependency SBOM"->"sbom").flatMap {case(label,key)=>Option(evidence.get(key)).map(label->_)}
    val repository=Option(evidence.get("source_repository")).map {url=>
      val value=url.asText;val allowed=scala.util.Try(java.net.URI.create(value)).toOption.exists(u=>Set("http","https")(u.getScheme)&&u.getHost!=null&&u.getUserInfo==null)
      if(allowed)render("repository-link","url"->escape(value))else escape(value)
    }.getOrElse("Source repository not provided")
    val build=Option(evidence.get("build_instructions")).map(v=>render("code-block","text"->escape(v.asText))).getOrElse(render("paragraph","text"->"Build instructions not provided"))
    val review=render("source-review","source"->(if(evidence.has("source_archive"))"Source archive available below"else "Source archive not provided"),
      "repository"->repository,"revision"->escape(evidence.path("source_revision").asText("Revision not declared")),"license"->escape(evidence.path("license").asText("License not declared")),
      "license_file"->(if(evidence.has("license_file"))"Available below"else "License text not provided"),"sbom"->(if(evidence.has("sbom"))"Available below"else "Dependency SBOM not provided"),"build"->build)
    val downloads=artifacts.map {case(label,a)=>render("artifact-download","label"->escape(label),"size"->escape(bytes(a.path("size").asLong)),
      "filename"->escape(a.path("filename").asText),"sha256"->escape(a.path("sha256").asText),"download"->escape(download(a)))}.mkString
    val commands=artifacts.map {case(_,a)=>s"curl -fLo ${a.path("filename").asText} https://YOUR-REPOSITORY-HOST${download(a)}\nsha256sum ${a.path("filename").asText}"}.mkString("\n")
    val runtimeLink=runtimeCatalog.find(r=>r.path("image")==item.path("runtime").path("image") && r.path("archive")==item.path("runtime").path("archive"))
      .map(r=>"/runtimes#"+runtimeAnchor(r)).getOrElse("/runtimes")
    val ports=Repository.mapper.createObjectNode().set[com.fasterxml.jackson.databind.node.ObjectNode]("inputs",item.path("inputs")).toPrettyString
    layout(reference,render("detail","name"->escape(name),"version"->escape(version),"icon"->icon(item),"category"->escape(item.path("categories").elements.asScala.map(_.asText).mkString(" · ")),"trust"->trust(item),"description"->escape(description(item)),"endpoint"->escape(s"/v1/operators/$name/$version"),
      "reference"->escape(s"operator: $reference"),"ports"->escape(ports),"parameters"->escape(item.path("parameters").toPrettyString),"commands"->escape(commands),"review"->review,"executor"->escape(item.path("execution").path("executor").asText),
      "entrypoint"->escape(item.path("execution").path("entrypoint").asText),"runtime_link"->escape(runtimeLink),"image"->escape(item.path("runtime").path("image").asText),"downloads"->downloads,"versions"->links))
  }
  def runtimes(items:JsonNode):String={
    val cards=items.elements.asScala.map{r=>
      s"<article id=\"${escape(runtimeAnchor(r))}\" class=\"operator-card runtime-public-card\"><h2>${escape(r.path("name").asText)} <small>${escape(r.path("version").asText)}</small></h2><p>${escape(r.path("description").asText)}</p><p>Build languages: ${escape(r.path("build").path("languages").elements.asScala.map(_.asText).mkString(", "))}</p><p>Executors: ${escape(r.path("executors").elements.asScala.map(_.asText).mkString(", "))}</p><pre>${escape(r.path("dependencies").toPrettyString)}</pre><code class=\"runtime-digest\">${escape(r.path("image").asText)}</code><p><a href=\"${escape(download(r.path("archive")))}\">Runtime archive</a> · <a href=\"${escape(download(r.path("build").path("archive")))}\">Build environment</a></p></article>"
    }.mkString
    layout("Supported runtimes",s"<h1>Supported runtimes</h1><p>Select a pinned execution environment and its compatible source-build environment.</p><div class=\"operator-grid\">$cards</div>")
  }
  def notFound:String=layout("Operator not found",render("404"))
}
