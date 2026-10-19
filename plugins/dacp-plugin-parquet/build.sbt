name := "dacp-plugin-parquet"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-avro" % "1.15.0",
  "org.apache.hadoop" % "hadoop-common" % "3.4.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1"
)
