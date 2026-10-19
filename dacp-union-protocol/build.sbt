name := "dacp-union-protocol"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
  "org.yaml" % "snakeyaml" % "2.3")

lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies += "org.apache.arrow" % "arrow-memory-netty" % "18.3.0"
