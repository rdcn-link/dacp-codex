name := "dacp-plugin-api"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.arrow" % "flight-core" % "18.3.0",
  "org.apache.arrow" % "arrow-memory-core" % "18.3.0")
