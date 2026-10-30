name := "xstream-designer"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val client = RootProject(file("../dacp-client-scala"))
dependsOn(client)
enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("xstream.designer.Main")
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
Compile / resourceGenerators += Def.task {
  Seq("dacp-logo.png", "dacp-favicon.png").map { name =>
    val out = (Compile / resourceManaged).value / "web" / name
    IO.copyFile(baseDirectory.value.getParentFile / "assets" / name, out); out
  }
}.taskValue

lazy val operatorContract = RootProject(file("../dacp-operator-sdk/contract"))
dependsOn(operatorContract)

lazy val operatorInference = RootProject(file("../dacp-operator-inference"))
dependsOn(operatorInference)

lazy val operatorExecutors = RootProject(file("../plugins/dacp-plugin-operator-executors"))
dependsOn(operatorExecutors % "runtime->compile;test->compile")
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.16"

Compile / resourceGenerators += Def.task {
  val out = (Compile / resourceManaged).value / "designer-version.txt"
  IO.write(out, version.value)
  Seq(out)
}.taskValue

lazy val engine = RootProject(file("../xstream-engine"))
dependsOn(engine)
lazy val datasetPlugins = RootProject(file("../xstream-data/xstream-data-files"))
lazy val staticCatalog = RootProject(file("../xstream-data/xstream-data-catalog-static"))
lazy val formats = RootProject(file("../xstream-data/xstream-data-formats-basic"))
dependsOn(datasetPlugins % "runtime->compile;test->compile", staticCatalog % "runtime->compile;test->compile", formats % "runtime->compile;test->compile")

lazy val dataScientificcodecs = RootProject(file("../xstream-data/xstream-data-scientific-codecs"))
dependsOn(dataScientificcodecs % "runtime->compile;test->compile")

lazy val dataNetcdf = RootProject(file("../xstream-data/xstream-data-netcdf"))
dependsOn(dataNetcdf % "runtime->compile;test->compile")

lazy val dataFits = RootProject(file("../xstream-data/xstream-data-fits"))
dependsOn(dataFits % "runtime->compile;test->compile")

lazy val dataFasta = RootProject(file("../xstream-data/xstream-data-fasta"))
dependsOn(dataFasta % "runtime->compile;test->compile")
