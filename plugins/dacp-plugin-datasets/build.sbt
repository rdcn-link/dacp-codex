name := "dacp-plugin-datasets"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies ++= Seq(
  "com.h2database" % "h2" % "2.3.232",
  "org.postgresql" % "postgresql" % "42.7.8"
)
