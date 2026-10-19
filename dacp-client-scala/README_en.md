# DACP Scala SDK

[中文](README.md)

A standalone Scala 2.13 SDK for reading DACP datasets over Apache Arrow Flight with mTLS. Requires JDK 17 and sbt 1.10.x. It uses the shared [protobuf definition](../proto/dacp.proto) and has no dependency on the DACP server project.

Build and publish to your local Ivy repository from this directory:

```bash
sbt test publishLocal
```

In a Scala 2.13 application's `build.sbt`:

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

This dependency is available after `publishLocal`; it is not published to Maven Central. Run the consuming JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED` and `-Dio.netty.tryReflectionSetAccessible=true`, as required by this Arrow version. For an sbt application, configure:

```scala
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true")
```

Configure `DACP_CA`, `DACP_CERT`, and `DACP_KEY` with PEM file paths, or pass explicit paths using `TlsConfig`. Server certificate verification remains enabled. A missing CA uses the default trust roots; a client certificate and key must be supplied together. The remote DACP server requires client certificates.

```scala
import dacp.client.{Source, TlsConfig}
import java.nio.file.Path
import scala.util.Using

val tls = TlsConfig(
  ca = Some(Path.of("examples/certs/ca.crt")),
  cert = Some(Path.of("examples/certs/client.crt")),
  key = Some(Path.of("examples/certs/client.key")))

Using.resource(Source.open("dacp://localhost:8815/data1", tls)) { ds =>
  ds.list("/").foreach(item => println(s"${item.path}: ${item.kind}, ${item.views}"))
  println(ds.list("/reports"))

  Using.resource(ds.open("/reports/orders")) { item =>
    Using.resource(item.asRecords()) { records =>
      println(records.schema)
      while (records.next()) {
        val batch = records.root
        println(batch.contentToTSVString())
      }
    }
  }

  Using.resource(ds.open("/image")) { item =>
    Using.resource(item.asBytes()) { bytes =>
      println(bytes.readNBytes(8).map(b => f"${b & 0xff}%02x").mkString(" "))
    }
  }
}
```

For direct Item URI access:

```scala
Using.resource(Source.openItem("dacp://localhost:8815/data1/image", tls)) { item =>
  Using.resource(item.asBytes()) { bytes =>
    println(bytes.readNBytes(8).length)
  }
}
```

`Source.open` accepts a dataset URI and returns `Dataset`; `Source.openItem` accepts an Item URI and returns `ItemStream`. `Dataset.list()` returns all Items recursively, including folders and data. `Dataset.list("/")` lists root children; `Dataset.list("/reports")` lists only immediate children of that folder. Each `Item` has an absolute logical `path`, `kind` (`folder` or `data`), `format`, supported `views`, and an optional Arrow schema. Paths never identify private files or SQL table names. Use `Dataset.open("/reports/orders")` for data; ordinary folders support listing only, while archive and compression folders also offer bytes. Relative paths are rejected. Raw CSV/NDJSON/Excel listing schemas may be absent; `asRecords().schema` obtains the record schema. The server startup log provides configured dataset names; dataset discovery is not implemented by the protocol.

Item statistics are optional: `lastModifiedTimeMs: Option[Long]` (UTC Unix milliseconds), `lastModifiedTime: Option[Instant]`, `totalSizeBytes: Option[Long]`, and `recordCount: Option[Long]`. `Some(0L)` means known zero; `None` means unknown or not applicable. File metadata supplies time and size; CSV/NDJSON/Excel counts remain unknown during browsing; `ds.count(path)` explicitly scans them. Parquet uses footer counts. SQL leaves count, time, and byte size unknown during browsing; `ds.count("/reports/orders")` explicitly requests an exact `COUNT(*)`. All folder properties are always `None`, including for empty folders; folder metadata never visits child Items. Binary data has unknown record count.

Listings avoid record scans. Explicit counts can be expensive because they scan files or SQL tables. SQL counts respect the configured statement timeout, and client RPC deadlines still apply. Statistics are observations and do not form an atomic snapshot with later reads.

`asRecords()` returns a closeable `RecordStream`. Call `next()` to load a batch and then read `root`. Arrow reuses the root on subsequent calls: consume or copy its contents before advancing, and do not retain it after closing the reader. CSV, NDJSON, Excel, DACP-generated Parquet with Arrow schema metadata, and SQL tables support the records view.

`asBytes()` returns a Java `InputStream`. File Items produce original file bytes; SQL tables produce Arrow IPC stream bytes. Reads consume chunks incrementally without collecting the complete Item. Standard `readNBytes` and `transferTo` are available; `readAllBytes` collects the complete Item in memory and should be reserved for small resources.

Use `Using.resource` for datasets, item handles, and readers, including when stopping early. Closing a reader cancels unfinished reads and releases Arrow buffers; closing an item closes its readers; closing a dataset closes its items, Flight connection, and allocator. Items opened through a dataset share its connection. Direct item access owns a private dataset connection and closes it with the item. These objects are intended for single-threaded use.

Each listing, item lookup, or content read has a deadline of 30 seconds by default. For long streams, set a larger `timeoutSeconds` on `Source.open` or `Source.openItem`; the deadline applies to the entire Flight RPC, not just connection establishment. Failed readers are terminal: close them and reopen the resource to retry from the beginning. There are no automatic retries or resumable reads.

Tests start an in-process Flight server and check metadata, records, byte chunk boundaries, partial reads, EOF, failure behavior, URI validation, cancellation, and resource cleanup. To also test against the running local example server, from this directory run:

```bash
DACP_CA=../examples/certs/ca.crt \
DACP_CERT=../examples/certs/client.crt \
DACP_KEY=../examples/certs/client.key \
DACP_SCALA_TEST_URI=dacp://localhost:8815/data1 \
sbt test
```

Transport failures surface as `DacpException`, with a stable `code` and readable message, without underlying transport descriptions or causes. Handle `NOT_FOUND`, `ACCESS_DENIED`, `UNAVAILABLE`, `TIMEOUT`, or other DACP codes rather than transport exceptions. Streaming failures are terminal; reopen the item to retry.

Formats describe source data: SQL reports `sql`, `.jsonl`/`.ndjson` report `ndjson`, `.xls`/`.xlsx` report `excel`. Ordinary `.json` has bytes only unless configured as `ndjson` on the server. Choose reading methods from each Item’s advertised `views`; new format handlers require no SDK changes.

ZIP archives appear as `kind=folder`, `format=zip`, with a `bytes` view. Browse entries with `ds.list("/archive")`, read CSV entries with `ds.open("/archive/reports/orders.csv").asRecords()`, and download the archive with `ds.open("/archive").asBytes()`. Check advertised views before opening a folder; ordinary folders have no streams. Nested archives remain binary data Items.

TAR (`tar`), GZIP (`gzip`), and compressed TAR (`tar-gzip`, `.tar.gz` / `.tgz`) use the same folder model as ZIP. In the default example, browse `/bundle` or `/bundle_gzip`, and open `/orders.csv.gz/orders.csv` for decompressed CSV records. Open `/orders.csv.gz` itself with `asBytes()` for the original compressed file. GZIP child names come from the public alias; aliases without a `.gz` suffix expose `content`. TAR scans are sequential and compressed TAR scans require decompression, so large archives can delay listing or opening. Nested containers remain binary Items.

Dataset construction is server-side: `directory`, `sqldb`, and `custom` providers all expose the same SDK API. The default server's `documents` Dataset discovers `examples/data`; connect to `dacp://localhost:8815/documents` and use `list("/")` or `open("/nested/orders.csv")`. Directory changes appear on subsequent requests. Public table aliases and private source bindings remain server-side.

## Filter, projection, and limit

```scala
import dacp.client.Expressions.col

Using.resource(ds.open("/reports/orders",
  predicate = Some((col("id") > 1) && col("amount").isNotNull),
  columns = Some(Seq("amount")), limit = Some(100L))) { item =>
  Using.resource(item.asRecords()) { records =>
    while (records.next()) println(records.root.contentToTSVString())
  }
}
```

Use an open `ds` inside the Dataset resource scope above. Equality uses `===`, inequality `=!=`, membership `.isin(Seq(...))`, and null checks `.isNull`/`.isNotNull`. The server applies filtering before projection and the global limit; bytes reject query options. See [shared semantics and examples](../docs/records-scans.md).

## Streaming jobs

```scala
import dacp.client.Job
import java.nio.file.{Files, Path}
import scala.util.Using

Using.resource(Job.run("dacp://localhost:8815",
  Files.readString(Path.of("examples/multi-node/job.yaml")), tls,
  timeoutSeconds = 180)) { events =>
  events.foreach { event =>
    if (event.kind == "data") println(s"${event.port}: ${event.data.length} bytes")
  }
}
```

`JobStream` is an `AutoCloseable` iterator. Use a resource scope even when stopping early: closing cancels the remaining job. Exhaustion or failure releases resources automatically. Errors after data delivery indicate partial results. Output port metadata includes representation, format, and records schema. Event payload arrays remain valid after advancing or closing the stream.

For a records data event, call `event.records(allocator)` with your own Arrow allocator and close the returned reader in a resource scope. Its single record batch is borrowed from that reader and remains valid until the reader advances or closes. Source datasets remain browsable; job results are streams and cannot be reopened.

## Union client

`dacp.client.Union` submits jobs to independent X, exposes separate `results()` and `events()` streams, and provides `status()` and `cancel()`. Close executions and streams with `Using.resource`. See the [Union server](../dacp-union-server/README_en.md) for complete code and mTLS configuration.
