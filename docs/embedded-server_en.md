# Starting DACP Server inside an application

[中文](embedded-server.md) · [Dynamic catalogs](dataset-catalogs_en.md)

`dacp.DacpServer` is the embeddable Scala/JVM API. The `dacp.Main` CLI uses the same implementation, so both modes include Flight TLS / Token, ordinary tasks, optional Union workers, execution records, and identical authorization checks.

## Startup and shutdown

With an existing server configuration:

```scala
import dacp.DacpServer

val server = DacpServer.builder()
  .configFile("/etc/dacp/server.yaml")
  .build()

try {
  server.start()             // Returns after listening starts; the application can continue.
  server.awaitTermination()  // Optional: block this thread until shutdown.
} finally {
  server.close()
}
```

`configFile` accepts a `String` or `Path`. Alternatively, `.config(config)` accepts a newly constructed `dacp.Config`. Builders allow one `build()`, and servers allow one start; create a new instance after closing. `boundPort` and `location` expose the actual Flight listener after startup.

`close()` is idempotent. Startup failure automatically releases allocated services, the catalog, execution records, and Arrow memory. Cleanup continues after an individual close fails. Once `build()` succeeds, the server owns its Config and catalog; close it even if never started. Do not share a Config or one closable catalog instance between servers.

The embedded API installs no JVM shutdown hook and does not close the process-wide plugin loader. The host application decides when to call `close()`; the CLI installs its own hook. Plugin directories and static extension registries are currently JVM-wide configuration, so servers in one JVM cannot use different plugin installation directories. Each instance should also have separate data directories, listener ports, and execution history directories.

## Injecting an application catalog

```scala
val catalog = new MyDatabaseCatalog(database)
val server = DacpServer.builder()
  .configFile("/etc/dacp/server.yaml")
  .catalog(catalog)
  .build()

server.start()
// The application continues managing its database or updating discovery metadata.
// Call server.close() on shutdown.
```

`MyDatabaseCatalog` is your implementation of `DatasetCatalog`. Direct injection needs no ServiceLoader registration. It replaces file-configured `datasets` or `dataset_catalog`, without creating the replaced provider. With `.config(config)`, inject before initializing that Config's catalog; do not call `Config.load` to initialize its default catalog and then replace it. Use `.configFile(...).catalog(...)` when loading a configuration file.

Each `catalog.open` returns a caller-owned `PublishedDataset` lease. The server closes the access handle and lease, and closes the catalog on shutdown. Catalogs must not close a shared database pool owned by the host. See [dynamic catalogs](dataset-catalogs_en.md) for the full contract.

## Runnable example and project dependencies

The [complete example](../examples/embedded-server/README_en.md) maintains a concurrent registry in the same JVM, adds a `reports` Dataset after startup, and publishes CSV data over the ordinary Flight interfaces.

Integration currently uses source-project dependencies with Scala 2.13.16 and JDK 17. An independent application's sbt configuration can follow the example:

```scala
ThisBuild / scalaVersion := "2.13.16"
lazy val server = RootProject(file("/path/to/dacp-codex/dacp-server"))
lazy val app = (project in file(".")).dependsOn(server)
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
)
```

Nodes still require valid TLS certificates and authorization configuration. Browsing and reading data need no Docker; executing operators requires their container environment.
