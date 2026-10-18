package dacp.repository

import com.fasterxml.jackson.databind.JsonNode
import scala.jdk.CollectionConverters._
import java.util.Locale

/** Server-rendered public pages. All artifact metadata and search input are escaped. */
object WebPages {
  def escape(value: String): String = value.flatMap {
    case '&' => "&amp;"
    case '<' => "&lt;"
    case '>' => "&gt;"
    case '"' => "&quot;"
    case '\'' => "&#39;"
    case c => c.toString
  }
  private def layout(title: String, content: String): String = s"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escape(title)} · DACP Operator Repository</title><link rel="stylesheet" href="/assets/repository.css"></head>
<body><header class="site-header"><a class="brand" href="/"><span class="brand-mark" aria-hidden="true">D</span>DACP<span class="brand-sub">Operator Repository</span></a><a class="api-link" href="/v1/operators">Catalog JSON ↗</a></header>
<main>$content</main><footer><span>DACP Operator Repository</span><span>Public access · Immutable versions · Arrow IPC</span></footer></body></html>"""
  private def description(item: JsonNode): String = item.path("description").asText("")
  private def path(item: JsonNode): String = s"/operators/${item.path("name").asText}/${item.path("version").asText}"
  private def bytes(size: Long): String = {
    if (size < 1024) s"$size B"
    else if (size < 1024 * 1024) f"${size / 1024.0}%.1f KiB"
    else if (size < 1024L * 1024 * 1024) f"${size / (1024.0 * 1024)}%.1f MiB"
    else f"${size / (1024.0 * 1024 * 1024)}%.1f GiB"
  }
  private def primary(item: JsonNode): JsonNode = Option(item.get("artifact")).getOrElse(item.path("runtime").path("archive"))
  private def download(a: JsonNode): String = "/v1/blobs/sha256/" + a.path("sha256").asText
  def index(items: Seq[JsonNode], query: String): String = {
    val needle = query.toLowerCase(Locale.ROOT)
    val matches = items.filter { item =>
      Seq(item.path("name").asText, item.path("version").asText, description(item)).exists(_.toLowerCase(Locale.ROOT).contains(needle))
    }
    val cards = matches.map { item =>
      val name = escape(item.path("name").asText)
      val version = escape(item.path("version").asText)
      val link = escape(path(item))
      val download = escape(WebPages.download(primary(item)))
      s"""<article class="operator-card"><div class="card-top"><span class="operator-icon" aria-hidden="true">↗</span><span class="version">v$version</span></div>
<h2><a href="$link">$name</a></h2><p class="description">${escape(description(item))}</p>
<div class="card-meta"><span>${escape(item.path("execution").path("executor").asText)}</span><span>${bytes(primary(item).path("size").asLong)}</span></div>
<div class="card-actions"><a class="text-link" href="$link">View operator →</a><a class="download-small" href="$download" download="${escape(primary(item).path("filename").asText)}">Download ↓</a></div></article>"""
    }.mkString
    val empty = if (items.isEmpty) """<section class="empty"><h2>No operators published yet</h2><p>Published operators will appear here once the repository administrator adds them.</p></section>"""
      else """<section class="empty"><h2>No matching operators</h2><p>Try a different name, version, or keyword.</p><a class="text-link" href="/">Browse all operators →</a></section>"""
    layout("Explore operators", s"""<section class="hero"><span class="eyebrow">THE PUBLIC OPERATOR CATALOG</span><h1>Find your next<br><span>data operation.</span></h1>
<p>Discover reusable data processing programs. Explore a version, download its code and shared runtime, and run it with DACP.</p>
<form class="search-form" action="/" method="get" role="search"><label class="sr-only" for="search">Search operators</label><span aria-hidden="true">⌕</span><input id="search" type="search" name="q" value="${escape(query)}" maxlength="200" placeholder="Search by name, version, or keyword"><button type="submit">Search</button></form></section>
<section class="catalog-heading"><div><span class="eyebrow">READY TO RUN</span><h2>${if (query.isEmpty) "All operators" else "Search results"}</h2></div><span class="count">${matches.size} version${if (matches.size == 1) "" else "s"}${if (query.isEmpty) "" else s" matching “${escape(query)}”"}</span></section>
${if (matches.isEmpty) empty else s"<section class=\"operator-grid\" aria-label=\"Operators\">$cards</section>"}
<section class="how-it-works"><span class="eyebrow">FROM DISCOVERY TO EXECUTION</span><h2>One contract. Any language.</h2><p>Operators can be written in Python, Scala, or any language. DACP connects their isolated runtime environments through a shared Arrow IPC input and output contract.</p><div class="steps"><div><b>01</b><h3>Explore</h3><p>Find an operator and choose its immutable version.</p></div><div><b>02</b><h3>Download</h3><p>Download code and a shared runtime, verified by SHA-256.</p></div><div><b>03</b><h3>Run with DACP</h3><p>Reference the operator in a job. Your execution node reuses cached runtime and code artifacts.</p></div></div></section>""")
  }
  def detail(item: JsonNode, versions: Seq[JsonNode]): String = {
    val name = item.path("name").asText
    val version = item.path("version").asText
    val reference = s"$name@$version"
    val endpoint = s"/v1/operators/$name/$version"
    val links = versions.map { entry =>
      val selected = if (entry.path("version").asText == version) " aria-current=\"page\"" else ""
      s"""<a class="version" href="${escape(path(entry))}"$selected>${escape(entry.path("version").asText)}</a>"""
    }.mkString(" ")
    val artifacts = Seq("Shared runtime" -> item.path("runtime").path("archive")) ++ Option(item.get("artifact")).map("Operator code" -> _)
    val downloads = artifacts.map { case (label,a) =>
      s"""<section><h3>${escape(label)}</h3><p>${bytes(a.path("size").asLong)} · <code>${escape(a.path("filename").asText)}</code></p><p class="digest">SHA-256: ${escape(a.path("sha256").asText)}</p><a class="button" href="${escape(download(a))}" download="${escape(a.path("filename").asText)}">Download ↓</a></section>"""
    }.mkString
    val commands = artifacts.map { case (_,a) =>
      s"curl -fLo ${a.path("filename").asText} https://YOUR-REPOSITORY-HOST${download(a)}\nsha256sum ${a.path("filename").asText}"
    }.mkString("\n")
    layout(reference, s"""<nav class="breadcrumb" aria-label="Breadcrumb"><a href="/">All operators</a><span>/</span>${escape(name)}</nav>
<section class="detail-heading"><div><span class="eyebrow">OPERATOR / VERSION ${escape(version)}</span><h1>${escape(name)}</h1><p>${escape(description(item))}</p></div><a class="text-link" href="${escape(endpoint)}">View manifest JSON ↗</a></section>
<div class="detail-grid"><section class="panel"><h2>Use this operator</h2><pre><code>${escape(s"operator: $reference")}</code></pre><p>Configure <code>remote_operator_library_urls</code> with this repository’s origin. DACP verifies and caches artifacts by digest, sharing them across operators.</p><h3>Manual download</h3><pre><code>${escape(commands)}</code></pre><p>Compare checksums with the manifest. Load <code>runtime.tar</code> with <code>docker load -i runtime.tar</code>.</p></section>
<aside class="panel"><h2>Artifact details</h2><dl><dt>Executor</dt><dd>${escape(item.path("execution").path("executor").asText)}</dd><dt>Entrypoint</dt><dd>${escape(item.path("execution").path("entrypoint").asText)}</dd><dt>Runtime image ID</dt><dd class="digest">${escape(item.path("runtime").path("image").asText)}</dd></dl>$downloads<h3>Published versions</h3><div class="versions">$links</div></aside></div>""")
  }
  def notFound: String = layout("Operator not found", """<section class="empty"><span class="eyebrow">404</span><h1>Operator not found</h1><p>This operator version has not been published.</p><a class="button" href="/">Browse operators</a></section>""")
}
