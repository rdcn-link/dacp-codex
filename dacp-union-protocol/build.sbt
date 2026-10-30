organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
name := "dacp-union-protocol"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.arrow" % "flight-core" % "18.3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
  "org.yaml" % "snakeyaml" % "2.3")

lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies += "org.apache.arrow" % "arrow-memory-netty" % "18.3.0"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-core" % "0.1.0-SNAPSHOT"
