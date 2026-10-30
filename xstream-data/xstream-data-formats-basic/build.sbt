name := "xstream-data-formats-basic"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore = RootProject(file("../xstream-data-core"))
dependsOn(dataCore)
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.13.0"
)
