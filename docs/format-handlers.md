# Adding a format handler

The server resolves file mappings through `Formats`. `SqlAdapter` handles SQL separately. Detection precedence is an explicit configuration override, compound container suffixes, provider signature matches (at most eight bytes), provider extension matches, then binary fallback. Extensions are lowercase and case-insensitive on filenames. IDs match `[a-z][a-z0-9-]*`; `sql` is reserved. Duplicate IDs or extensions fail registry initialization.

Implement `dacp.FormatHandler` in a JAR compiled against the standalone `dacp-plugin-api` with a `Provided` dependency. Scala providers use its trait directly; Java implementations can implement the generated JVM interface. Scala trait defaults are easiest to reuse from Scala. A complete independently built plugin project is in [examples/plugins](../examples/plugins/README.md). Providers must have a public no-argument constructor.

```scala
package example

import dacp.{Records, FileOptions, FormatHandler}
import java.nio.file.Path
import scala.jdk.CollectionConverters._

class LineProvider extends FormatHandler {
  val id = "lines"
  val extensions = Set("lines")
  override val records = true
  override def rows(path: Path, options: FileOptions): Records.Rows =
    new Records.Rows {
      private val input = java.nio.file.Files.lines(path)
      val names = Seq("value")
      val iterator = input.iterator().asScala.map(line => Seq(line))
      def close(): Unit = input.close()
    }
}
```

Add `META-INF/services/dacp.FormatHandler` to the provider JAR:

```text
example.LineProvider
```

Put the JAR and its private dependencies in the configured `plugin_directory` and restart. The registry discovers it through `ServiceLoader`. A Java provider must implement Scala collection signatures; a small Scala adapter can delegate parsing to any Java library, as Excel delegates to Apache POI.

`rows` supplies ordered column names, a lazy iterator, and an idempotent `close`. Shared default methods infer a schema from a bounded sample, stream record batches, and compute explicit counts. Override `schema`, `stream`, or `count` for formats such as Parquet that have specialized readers. `cheapSchema` and `cheapCount` run during listing and must avoid full data scans. Detection, listing, and reading must agree on the format ID and advertised capability. A format with `records = false` exposes bytes only.

The shared file reader sends original bytes without conversion. Opening records must not alter the source. Implementations must close parser resources on success, failure, and cancellation, and call the supplied `demand` callback before reading each record batch. Third-party exceptions must be mapped to `FormatFailure` with a fixed public message, without filenames, sheet names, data values, or library diagnostics. SDKs translate these into `FORMAT_ERROR`.

`FileOptions` carries private `format`, ordered Arrow columns, and a `parameters: Map[String,Object]` populated from YAML `options`. Override `validateOptions` to validate handler-owned keys, types, and values. The default rejects all parameters. Excel owns `options.sheet` and `options.header_row`; shared code contains no Excel-specific option validation. Nested YAML values retain Java map/list representations. The options stay server-side: public tickets contain only the dataset, logical Item path, and view. Schema is unavailable in ordinary CSV/NDJSON/Excel listings; opening records infers it or validates configured columns. Folder properties remain unknown and never visit descendants.

Excel uses HSSF events for XLS and SAX/XSSF events for XLSX, a 16-row queue, and cancellation checks between parser events. Shared-string dictionaries and individual rows/cells still consume memory. Numeric dates remain Excel serial values. Only cached formula results are read; formulas are never recalculated. Multiple sheets require a configured selection. This is read support, without workbook editing or spreadsheet rendering.

## Container readers

`ContainerReader` is the separate interface for enumerating children and staging an entry as a seekable file. Register a reader in `META-INF/services/dacp.ContainerReader`. Declare its `id`, `extensions`, and optional compound filename matcher; DACP automatically creates the matching format entry. Do not also register a format with the same ID. Built-in and external readers share the same interface. Duplicate IDs/extensions and ambiguous filename matches are rejected.

`NamespaceAdapter` publishes mapped containers as folders with a bytes view returning the original file, infers parent folders, and passes staged children to existing format readers. All folder properties stay unknown. Nested containers remain binary data. GZIP's one child is named from the public alias, ignoring private filenames. Compound `.tar.gz` and `.tgz` suffixes resolve before ordinary extension detection.

ZIP listings use the archive directory without decompressing payloads. TAR and compressed TAR scan sequentially, with decompression for the latter; GZIP listings only read the header. Entry reads stage one file and clean it on success, failure, or cancellation. TAR scans validate the whole archive, including entries after the requested one, before exposing staged content. Scans and decompression must honor the supplied demand/cancellation callback.

Limits are 10,000 entries, 512 MiB per decompressed entry, and 2 GiB total declared size. TAR also bounds total decompressed bytes to that size plus 16 MiB of metadata/padding. Duplicate entries, invalid paths, and file/parent conflicts are rejected. TAR links, sparse files, and special entries are unsupported. Container roots reject records; their data children advertise views through the regular format registry.

## Native structured files

Implement `StructuredFileHandler` (a `FormatHandler`) and register it in the format service file. `openStructure(path, options, context, check)` returns a request-scoped `DatasetAccess` rooted at `/`. It can expose logical objects without extracting files. List public absolute paths relative to that root; the server prefixes the published file alias. Implement both recursive `list()` and immediate-child `list(path)`, plus `describe`, `count`, and synchronous `stream`. Child Items must explicitly advertise their views when using native formats that do not have a file handler. Schema must match the selected stream; choose the schema of the first advertised view in Item metadata. The server closes access handles on success and failure. Honor `check` during discovery and `ready` before reading and emitting batches, and use `close` to release file handles even after cancellation.

The file root is a folder with original-file bytes and unknown properties. Folder metadata does not open its structure or compute descendants. Nested logical folders also have unknown properties. Native child bytes have handler-defined semantics that must be documented; the root bytes always preserve the physical file. Ordinary listing must not compute expensive counts. Startup counting skips structured roots.

Use `PluginChecks.listing(access)` for metadata contract tests. Streaming, cancellation, private errors, and cleanup tests are demonstrated by [PluginSpec](../dacp-server/src/test/scala/dacp/PluginSpec.scala). NetCDF and FITS plugins can expose groups, variables, or HDUs through this interface; actual parsers and array/image views are not included. Specify any record projection explicitly rather than silently flattening arrays.

## Records scans

`FormatHandler.scan` supplies bounded batch filtering, projection, and early limit termination for every records handler. Override it to optimize the source reader; directory-backed and mixed datasets invoke the same contract, including for archive children. Native structured children use `DatasetAccess.scan`. See [records scan contracts](records-scans.md#plugin-implementation-contract).
