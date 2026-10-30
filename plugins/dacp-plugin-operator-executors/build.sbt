organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
name := "dacp-plugin-operator-executors"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

lazy val operatorSdk = RootProject(file("../../dacp-operator-sdk/jvm"))
dependsOn(operatorSdk)
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")

Compile / resourceGenerators += Def.task {
  Seq("__init__.py" -> "group-sdk.py", "inference.py" -> "inference.py").map { case (file, name) =>
    val source = baseDirectory.value / "../../dacp-operator-sdk/python/dacp_operator" / file
    val destination = (Compile / resourceManaged).value / name
    IO.copyFile(source, destination)
    destination
  }
}.taskValue

libraryDependencies += "io.github.rdcn-link" %% "xstream-data-core" % "0.1.0-SNAPSHOT"
