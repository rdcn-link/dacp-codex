name := "dacp-plugin-api"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val dataCore = RootProject(file("../xstream-data/xstream-data-core"))
dependsOn(dataCore)
