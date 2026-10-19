name := "dacp-union-server"
scalaVersion := "2.13.16"
lazy val worker = RootProject(file("../dacp-server"))
dependsOn(worker % "compile->compile;runtime->runtime;test->runtime")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
Compile / mainClass := Some("dacp.unionserver.Main")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Test / javaOptions ++= (Compile / javaOptions).value
enablePlugins(JavaAppPackaging)
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")

// Package the shared project logo into both websites.
Compile / resourceGenerators += Def.task {
  val output = (Compile / resourceManaged).value / "web" / "dacp-logo.png"
  IO.copyFile(baseDirectory.value.getParentFile / "assets" / "dacp-logo.png", output)
  Seq(output)
}.taskValue
