scalaVersion := "2.13.16"
name := "dacp-website"
version := "0.1.0-SNAPSHOT"
enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("dacp.website.Main")
libraryDependencies ++= Seq(
  "org.commonmark" % "commonmark" % "0.24.0",
  "org.commonmark" % "commonmark-ext-heading-anchor" % "0.24.0",
  "org.commonmark" % "commonmark-ext-gfm-tables" % "0.24.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Compile / resourceGenerators += Def.task {
  val root = baseDirectory.value.getParentFile
  val dest = (Compile / resourceManaged).value / "site"
  val docs = (root ** "*_en.md").get.filterNot(f => f.getPath.contains("/target/") || f.getPath.contains("/."))
  docs.map { f => val out = dest / IO.relativize(root, f).get; IO.copyFile(f, out); out } ++
    Seq("dacp-logo.png", "dacp-favicon.png", "dacp-favicon.ico").map { n =>
      val out = dest / "assets" / n; IO.copyFile(root / "assets" / n, out); out
    }
}.taskValue
