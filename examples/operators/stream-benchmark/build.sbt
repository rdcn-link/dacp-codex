name := "dacp-stream-benchmark"
scalaVersion := "2.13.16"
lazy val sdk = RootProject(file("../../../dacp-operator-sdk"))
dependsOn(sdk)
// Small SDK classes are bundled; runtime supplies Arrow/Jackson/Scala.
Compile / packageBin / mappings ++= {
  val classes = (sdk / Compile / classDirectory).value
  (classes ** "*.class").get.map(f => f -> IO.relativize(classes,f).get)
}
