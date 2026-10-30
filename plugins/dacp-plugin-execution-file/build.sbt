name := "dacp-plugin-execution-file"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-core" % "0.1.0-SNAPSHOT"
