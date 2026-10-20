name := "dacp-plugin-operator-executors"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

lazy val operatorSdk = RootProject(file("../../dacp-operator-sdk"))
dependsOn(operatorSdk)
Test / fork := true
Test / javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")

Compile / resourceGenerators += Def.task {
  val source = baseDirectory.value / "../../dacp-operator-sdk/python/dacp_operator/__init__.py"
  val destination = (Compile / resourceManaged).value / "group-sdk.py"
  IO.copyFile(source, destination)
  Seq(destination)
}.taskValue
