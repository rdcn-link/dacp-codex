name := "xstream-data-core"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "18.3.0",
  "org.apache.arrow" % "arrow-memory-core" % "18.3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2")
