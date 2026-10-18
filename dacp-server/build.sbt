scalaVersion := "2.13.16"
Compile / PB.protoSources += baseDirectory.value / ".." / "proto"
Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value)
libraryDependencies ++= Seq(
 "org.apache.commons" % "commons-compress" % "1.28.0",
 "org.apache.poi" % "poi-ooxml" % "5.5.1",
 "com.h2database" % "h2" % "2.3.232",
 "org.postgresql" % "postgresql" % "42.7.8",
 "com.google.protobuf" % "protobuf-java" % "3.25.5",
 "org.apache.arrow" % "flight-core" % "18.3.0",
 "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
 "org.apache.pekko" %% "pekko-stream" % "1.1.3",
 "com.github.docker-java" % "docker-java" % "3.4.1",
 "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.4.1",
 "org.yaml" % "snakeyaml" % "2.3",
 "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2",
 "org.apache.commons" % "commons-csv" % "1.13.0",
 "org.apache.parquet" % "parquet-avro" % "1.15.0",
 "org.apache.hadoop" % "hadoop-common" % "3.4.1",
 "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1",
 "org.slf4j" % "slf4j-simple" % "2.0.16",
 "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Compile / mainClass := Some("dacp.Main")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Test / javaOptions ++= (Compile / javaOptions).value

enablePlugins(JavaAppPackaging)
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")

lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
dependsOn(pluginApi)

// Compile the standalone contributor example against the same API during contract tests.
Test / unmanagedSourceDirectories += baseDirectory.value / ".." / "examples" / "plugins" / "src" / "main" / "scala"
