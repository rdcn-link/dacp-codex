name := "dacp-common-scala"
organization := "example"
version := "1.0"
scalaVersion := "2.13.16"
// Supplied by dacp/operator-jvm:1.1; keep them out of the thin operator JAR.
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "18.3.0" % Provided,
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0" % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3" % Provided)
Compile / mainClass := Some("dacp.common.Verify")
libraryDependencies += "io.github.rdcn-link" %% "dacp-operator-sdk" % "0.1.0-SNAPSHOT" % Provided
lazy val verifyOperators = taskKey[Unit]("Verify every common Scala operator")
verifyOperators := {
  val cp = (Compile / fullClasspath).value.files.mkString(java.io.File.pathSeparator)
  val status = scala.sys.process.Process(Seq("java", "-cp", cp, "dacp.common.Verify")).!
  require(status == 0, "operator verification failed")
}
// Bundle the small SDK itself; Arrow/Jackson/Scala remain in the shared runtime.
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
