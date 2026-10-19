ThisBuild / scalaVersion := "2.13.16"
lazy val server = RootProject(file("../../dacp-server"))
lazy val app = (project in file(".")).dependsOn(server % "compile->compile;runtime->runtime")
Compile / mainClass := Some("example.EmbeddedServerExample")
fork := true
javaOptions ++= Seq("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
