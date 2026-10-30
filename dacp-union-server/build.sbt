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

// Package the full logo and compact C favicons.
Compile / resourceGenerators += Def.task {
  Seq("dacp-logo.png", "dacp-favicon.png", "dacp-favicon.ico").map { name =>
    val output = (Compile / resourceManaged).value / "web" / name
    IO.copyFile(baseDirectory.value.getParentFile / "assets" / name, output)
    output
  }
}.taskValue

lazy val collaborationClient=RootProject(file("../dacp-collaboration-client-scala"))
dependsOn(collaborationClient)

// Bundle codec/parser dependencies explicitly in the Union distribution and embedded-node tests.
lazy val scientificCodecs=RootProject(file("../xstream-data/xstream-data-scientific-codecs"))
lazy val scientificNetcdf=RootProject(file("../xstream-data/xstream-data-netcdf"))
lazy val scientificFits=RootProject(file("../xstream-data/xstream-data-fits"))
lazy val scientificFasta=RootProject(file("../xstream-data/xstream-data-fasta"))
dependsOn(scientificCodecs % "runtime->compile;test->compile",scientificNetcdf % "runtime->compile;test->compile",scientificFits % "runtime->compile;test->compile",scientificFasta % "runtime->compile;test->compile")
