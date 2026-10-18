name := "dacp-operator-executors"
scalaVersion := "2.13.16"
lazy val pluginApi = RootProject(file("../dacp-plugin-api"))
dependsOn(pluginApi)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
