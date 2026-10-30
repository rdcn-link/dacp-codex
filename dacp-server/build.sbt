scalaVersion := "2.13.16"
Compile / PB.protoSources += baseDirectory.value / ".." / "proto"
Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value)
libraryDependencies ++= Seq(
 "com.google.protobuf" % "protobuf-java" % "3.25.5",
 "org.apache.arrow" % "flight-core" % "18.3.0",
 "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
 "org.apache.pekko" %% "pekko-stream" % "1.1.3",
 "com.github.docker-java" % "docker-java" % "3.4.1",
 "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.4.1",
 "org.yaml" % "snakeyaml" % "2.3",
 "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2",
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

libraryDependencies += "io.github.rdcn-link" %% "xstream-plugin-operator-executors" % "0.1.0-SNAPSHOT" % "runtime,test"

lazy val unionProtocol = RootProject(file("../dacp-union-protocol"))
dependsOn(unionProtocol)

// Standard distribution bundles plugins at runtime. Core compilation sees only the public API.
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-files" % "0.1.0-SNAPSHOT" % "runtime,test"
libraryDependencies += "io.github.rdcn-link" %% "xstream-data-formats-basic" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-parquet" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-excel" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-archives" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-catalog-static" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-mounted" % "0.1.0-SNAPSHOT" % "runtime,test"

lazy val pluginExecutionFile = RootProject(file("../plugins/dacp-plugin-execution-file"))
dependsOn(pluginExecutionFile % "runtime->compile;test->compile")

// Reuse the Union G6 renderer and its offline assets in the node dashboard.
Compile / resourceGenerators += Def.task {
  val shared = baseDirectory.value.getParentFile / "dacp-union-server" / "src" / "main" / "resources" / "web"
  val output = (Compile / resourceManaged).value / "node-web"
  val files = Seq("duration.js", "job-script.js", "dag.js", "attempts.js", "style.css", "vendor/g6-5.1.1.min.js", "vendor/G6-LICENSE.txt")
  val copied = files.map { name => val target=output / name; IO.copyFile(shared / name,target); target }
  val branding=Seq("dacp-logo.png", "dacp-favicon.png", "dacp-favicon.ico").map { name =>
    val target=output / name
    IO.copyFile(baseDirectory.value.getParentFile / "assets" / name,target)
    target
  }
  copied ++ branding
}.taskValue

lazy val collaborationClient=RootProject(file("../dacp-collaboration-client-scala"))
dependsOn(collaborationClient)

libraryDependencies += "io.github.rdcn-link" %% "xstream-operator-contract" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.github.rdcn-link" %% "xstream-operator-inference" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.github.rdcn-link" %% "xstream-engine" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-scientific-codecs" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-netcdf" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-fits" % "0.1.0-SNAPSHOT" % "runtime,test"

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-fasta" % "0.1.0-SNAPSHOT" % "runtime,test"


libraryDependencies += "io.github.rdcn-link" %% "xstream-data-core" % "0.1.0-SNAPSHOT"
