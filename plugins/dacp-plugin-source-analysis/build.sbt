name := "dacp-plugin-source-analysis"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-compress" % "1.27.1",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
lazy val contract = RootProject(file("../../dacp-operator-contract"))
dependsOn(contract)
