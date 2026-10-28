name := "dacp-operator-inference"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
lazy val operatorContract = RootProject(file("../dacp-operator-contract"))
lazy val unionProtocol = RootProject(file("../dacp-union-protocol"))
dependsOn(pluginApi, operatorContract, unionProtocol)
libraryDependencies ++= Seq(
  "com.github.docker-java" % "docker-java-core" % "3.4.1",
  "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.4.1",
  "org.yaml" % "snakeyaml" % "2.3",
  "commons-codec" % "commons-codec" % "1.18.0",
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "com.google.guava" % "guava" % "33.4.8-jre",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
