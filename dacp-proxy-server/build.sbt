name := "dacp-proxy-server"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.3",
  "org.slf4j" % "slf4j-simple" % "2.0.16",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Compile / mainClass := Some("dacp.proxy.Main")
Test / fork := true
enablePlugins(JavaAppPackaging)
