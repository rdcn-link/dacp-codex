name := "dacp-jvm-runtime-dependencies"
organization := "io.github.rdcn-link"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.16"
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3",
  "org.apache.commons" % "commons-csv" % "1.13.0",
  "org.apache.parquet" % "parquet-avro" % "1.15.0",
  "org.apache.hadoop" % "hadoop-common" % "3.4.1",
  // Parquet uses FileOutputFormat; no YARN/HDFS client dependency tree is needed.
  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1").excludeAll(ExclusionRule("org.apache.hadoop")),
  "org.slf4j" % "slf4j-simple" % "2.0.16",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test)
lazy val pluginApi = RootProject(file("../../dacp-plugin-api"))
dependsOn(pluginApi)
// Align shared libraries with the data service's resolved dependency versions.
dependencyOverrides ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "com.google.guava" % "guava" % "33.4.8-jre",
  "javax.xml.bind" % "jaxb-api" % "2.2.11",
  "io.netty" % "netty-codec-http2" % "4.1.110.Final",
  "io.netty" % "netty-handler-proxy" % "4.1.110.Final",
  "io.netty" % "netty-codec-http" % "4.1.110.Final",
  "io.netty" % "netty-codec-socks" % "4.1.110.Final",
  "io.netty" % "netty-handler" % "4.1.119.Final",
  "io.netty" % "netty-transport-native-unix-common" % "4.1.119.Final")

lazy val operatorSdk = RootProject(file("../../dacp-operator-sdk/jvm"))
dependsOn(operatorSdk)

// A dependency environment contains no operator implementation or launcher.
lazy val stageRuntime = taskKey[File]("Stage shared JVM dependency JARs")
stageRuntime := {
  val output = target.value / "runtime" / "lib"
  IO.delete(output)
  IO.createDirectory(output)
  (Runtime / fullClasspath).value.map(_.data).zipWithIndex.foreach { case (entry, index) =>
    if (entry.isFile && entry.getName.endsWith(".jar")) IO.copyFile(entry, output / entry.getName)
    else if (entry.isDirectory) {
      val contents = Path.allSubpaths(entry).toSeq.filter(_._1.isFile)
      if (contents.nonEmpty) IO.jar(contents, output / s"project-$index.jar", new java.util.jar.Manifest())
    }
  }
  output
}
