name := "scala-identity"
organization := "example"
version := "1.0"
scalaVersion := "2.13.16"
// Supplied by dacp/operator-jvm:1.1; keep them out of the thin operator JAR.
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "18.3.0" % Provided,
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0" % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3" % Provided)
Compile / mainClass := Some("example.ScalaIdentity")
libraryDependencies += "io.github.rdcn-link" %% "dacp-operator-sdk" % "0.1.0-SNAPSHOT" % Provided

// Bundle SDK and its small public contracts; Arrow/Jackson stay in the runtime.
Compile / packageBin / mappings ++= {
  val jars = (Compile / dependencyClasspath).value.map(_.data).filter(f =>
    f.getName.startsWith("dacp-operator-sdk_") || f.getName.startsWith("dacp-plugin-api_"))
  jars.flatMap { jar =>
    val unpack = target.value / ("bundled-" + jar.getName)
    IO.delete(unpack)
    IO.unzip(jar, unpack)
    (unpack ** "*.class").get.map(f => f -> IO.relativize(unpack, f).get)
  }
}
