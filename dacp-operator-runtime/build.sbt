name := "dacp-operator-runtime"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
  "org.apache.commons" % "commons-csv" % "1.13.0",
  "org.apache.parquet" % "parquet-avro" % "1.15.0",
  "org.apache.hadoop" % "hadoop-common" % "3.4.1",
  // Parquet uses FileOutputFormat; no YARN/HDFS client dependency tree is needed.
  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1").excludeAll(ExclusionRule("org.apache.hadoop")),
  "org.slf4j" % "slf4j-simple" % "2.0.16",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
dependsOn(pluginApi)
Compile / mainClass := Some("dacp.OperatorMain")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Test / javaOptions ++= (Compile / javaOptions).value
enablePlugins(JavaAppPackaging)

// Align shared libraries with the data service's resolved dependency versions.
dependencyOverrides ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "com.google.guava" % "guava" % "33.4.8-jre",
  "javax.xml.bind" % "jaxb-api" % "2.2.11",
  "io.netty" % "netty-codec-http2" % "4.1.110.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.110.Final",
  "io.netty" % "netty-codec-http" % "4.1.110.Final",
  "io.netty" % "netty-codec-socks" % "4.1.110.Final",
  "io.netty" % "netty-handler" % "4.1.119.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.119.Final")
