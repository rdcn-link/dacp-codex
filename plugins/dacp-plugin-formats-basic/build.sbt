name := "dacp-plugin-formats-basic"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.13.0"
)
