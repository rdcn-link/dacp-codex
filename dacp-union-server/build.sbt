name := "dacp-union-server"
scalaVersion := "2.13.16"
lazy val worker = RootProject(file("../dacp-server"))
dependsOn(worker)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
Compile / mainClass := Some("dacp.unionserver.Main")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
Test / javaOptions ++= (Compile / javaOptions).value
enablePlugins(JavaAppPackaging)
Universal / javaOptions ++= Seq("-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-J-Dio.netty.tryReflectionSetAccessible=true")

