name := "xstream-data-fits"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore=RootProject(file("../xstream-data-core"))
dependsOn(dataCore)
libraryDependencies ++= Seq("gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.20.0")
