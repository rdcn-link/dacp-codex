name := "dacp-stream-benchmark"
scalaVersion := "2.13.16"
lazy val api = RootProject(file("../../../dacp-plugin-api"))
lazy val sdk = RootProject(file("../../../dacp-operator-sdk"))
dependsOn(sdk)
// Small SDK classes are bundled; runtime supplies Arrow/Jackson/Scala.
Compile / packageBin / mappings ++= {
  Seq((sdk / Compile / classDirectory).value, (api / Compile / classDirectory).value).flatMap { classes =>
    (classes ** "*.class").get.map(f => f -> IO.relativize(classes,f).get)
  }
}
