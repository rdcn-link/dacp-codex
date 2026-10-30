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
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-fasta" % "0.1.0-SNAPSHOT" % "runtime,test"
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-fits" % "0.1.0-SNAPSHOT" % "runtime,test"
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-netcdf" % "0.1.0-SNAPSHOT" % "runtime,test"
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-scientific-codecs" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-core" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.github.rdcn-link" %% "xstream-operator-inference" % "0.1.0-SNAPSHOT"
