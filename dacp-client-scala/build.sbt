name := "dacp-client-scala"
organization := "io.github.rdcn-link"
version := "0.1.0"
scalaVersion := "2.13.16"
Compile / PB.protoSources += baseDirectory.value / ".." / "proto"
Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value)
libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.25.5",
  "org.apache.arrow" % "flight-core" % "18.3.0",
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
