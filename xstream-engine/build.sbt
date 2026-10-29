name := "xstream-engine"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
lazy val operatorInference = RootProject(file("../dacp-operator-inference"))
lazy val unionProtocol = RootProject(file("../dacp-union-protocol"))
dependsOn(pluginApi, operatorInference, unionProtocol)
libraryDependencies ++= Seq("com.github.docker-java" % "docker-java-core" % "3.4.1", "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.4.1", "org.scalatest" %% "scalatest" % "3.2.19" % Test)
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
libraryDependencies ++= Seq("org.apache.arrow" % "arrow-memory-netty" % "18.3.0")
lazy val testDatasets = RootProject(file("../plugins/dacp-plugin-datasets"))
lazy val testStatic = RootProject(file("../plugins/dacp-plugin-catalog-static"))
lazy val testFormats = RootProject(file("../plugins/dacp-plugin-formats-basic"))
lazy val testHistory = RootProject(file("../plugins/dacp-plugin-execution-file"))
dependsOn(testDatasets % "test->compile", testStatic % "test->compile", testFormats % "test->compile", testHistory % "test->compile")

enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("dacp.xstream.Main")
executableScriptName := "xstream-runner"
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")
lazy val client = RootProject(file("../dacp-client-scala"))
dependsOn(client)
lazy val bundledDatasets = RootProject(file("../plugins/dacp-plugin-datasets"))
lazy val bundledCatalog = RootProject(file("../plugins/dacp-plugin-catalog-static"))
lazy val bundledFormats = RootProject(file("../plugins/dacp-plugin-formats-basic"))
lazy val bundledHistory = RootProject(file("../plugins/dacp-plugin-execution-file"))
lazy val bundledExecutors = RootProject(file("../plugins/dacp-plugin-operator-executors"))
dependsOn(bundledDatasets % "runtime->compile",bundledCatalog % "runtime->compile",bundledFormats % "runtime->compile",bundledHistory % "runtime->compile",bundledExecutors % "runtime->compile")
