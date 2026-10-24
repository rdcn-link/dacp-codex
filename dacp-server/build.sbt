scalaVersion := "2.13.16"
Compile / PB.protoSources += baseDirectory.value / ".." / "proto"
Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value)
libraryDependencies ++= Seq(
 "com.google.protobuf" % "protobuf-java" % "3.25.5",
 "org.apache.arrow" % "flight-core" % "18.3.0",
 "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
 "org.apache.pekko" %% "pekko-stream" % "1.1.3",
 "com.github.docker-java" % "docker-java" % "3.4.1",
 "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.4.1",
 "org.yaml" % "snakeyaml" % "2.3",
 "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2",
 "org.slf4j" % "slf4j-simple" % "2.0.16",
 "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Compile / mainClass := Some("dacp.Main")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Test / javaOptions ++= (Compile / javaOptions).value

enablePlugins(JavaAppPackaging)
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")

lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
lazy val operatorRuntime = RootProject(file("../dacp-operator-runtime"))
dependsOn(pluginApi, operatorRuntime % "test->compile")

// Compile the standalone contributor example against the same API during contract tests.
Test / unmanagedSourceDirectories += baseDirectory.value / ".." / "examples" / "plugins" / "src" / "main" / "scala"

lazy val operatorExecutors = RootProject(file("../plugins/dacp-plugin-operator-executors"))
dependsOn(operatorExecutors % "runtime->compile;test->compile")

lazy val unionProtocol = RootProject(file("../dacp-union-protocol"))
dependsOn(unionProtocol)

// Standard distribution bundles plugins at runtime. Core compilation sees only the public API.
lazy val pluginDatasets = RootProject(file("../plugins/dacp-plugin-datasets"))
dependsOn(pluginDatasets % "runtime->compile;test->compile")
lazy val pluginFormatsBasic = RootProject(file("../plugins/dacp-plugin-formats-basic"))
dependsOn(pluginFormatsBasic % "runtime->compile;test->compile")
lazy val pluginParquet = RootProject(file("../plugins/dacp-plugin-parquet"))
dependsOn(pluginParquet % "runtime->compile;test->compile")
lazy val pluginExcel = RootProject(file("../plugins/dacp-plugin-excel"))
dependsOn(pluginExcel % "runtime->compile;test->compile")
lazy val pluginArchives = RootProject(file("../plugins/dacp-plugin-archives"))
dependsOn(pluginArchives % "runtime->compile;test->compile")
lazy val pluginCatalogStatic = RootProject(file("../plugins/dacp-plugin-catalog-static"))
dependsOn(pluginCatalogStatic % "runtime->compile;test->compile")

lazy val pluginMounted = RootProject(file("../plugins/dacp-plugin-mounted"))
dependsOn(pluginMounted % "runtime->compile;test->compile")

lazy val pluginExecutionFile = RootProject(file("../plugins/dacp-plugin-execution-file"))
dependsOn(pluginExecutionFile % "runtime->compile;test->compile")

// Reuse the Union G6 renderer and its offline assets in the node dashboard.
Compile / resourceGenerators += Def.task {
  val shared = baseDirectory.value.getParentFile / "dacp-union-server" / "src" / "main" / "resources" / "web"
  val output = (Compile / resourceManaged).value / "node-web"
  val files = Seq("dag.js", "attempts.js", "style.css", "vendor/g6-5.1.1.min.js", "vendor/G6-LICENSE.txt")
  val copied = files.map { name => val target=output / name; IO.copyFile(shared / name,target); target }
  val branding=Seq("dacp-logo.png", "dacp-favicon.png", "dacp-favicon.ico").map { name =>
    val target=output / name
    IO.copyFile(baseDirectory.value.getParentFile / "assets" / name,target)
    target
  }
  copied ++ branding
}.taskValue
