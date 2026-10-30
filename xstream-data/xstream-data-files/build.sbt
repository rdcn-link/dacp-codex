name := "xstream-data-files"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore = RootProject(file("../xstream-data-core"))
dependsOn(dataCore)
libraryDependencies ++= Seq(
  "com.h2database" % "h2" % "2.3.232",
  "org.postgresql" % "postgresql" % "42.7.8"
)
