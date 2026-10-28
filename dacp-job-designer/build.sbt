name := "dacp-job-designer"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val client = RootProject(file("../dacp-client-scala"))
dependsOn(client)
enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("dacp.designer.Main")
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

lazy val operatorContract = RootProject(file("../dacp-operator-contract"))
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
