name := "xstream-data-archives"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore = RootProject(file("../xstream-data-core"))
dependsOn(dataCore)
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-compress" % "1.28.0"
)
