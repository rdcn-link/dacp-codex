name := "dacp-collaboration-client-scala"
organization := "io.github.rdcn-link"
version := "0.1.0"
scalaVersion := "2.13.16"
lazy val sharedProtocol=RootProject(file("../dacp-union-protocol"))
dependsOn(sharedProtocol)
