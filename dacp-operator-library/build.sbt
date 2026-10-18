scalaVersion := "2.13.16"
name := "dacp-operator-library"
libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Compile / mainClass := Some("dacp.library.Main")
fork := true
enablePlugins(JavaAppPackaging)
