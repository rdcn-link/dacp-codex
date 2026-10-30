name := "xstream-data-excel"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore = RootProject(file("../xstream-data-core"))
dependsOn(dataCore)
libraryDependencies ++= Seq(
  "org.apache.poi" % "poi-ooxml" % "5.5.1"
)
