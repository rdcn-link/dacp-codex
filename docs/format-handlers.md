# Adding a format handler

The server resolves file mappings through `Formats`. `SqlAdapter` handles SQL separately. Detection precedence is an explicit configuration override, compound container suffixes, provider signature matches (at most eight bytes), provider extension matches, then binary fallback. Extensions are lowercase and case-insensitive on filenames. IDs match `[a-z][a-z0-9-]*`; `sql` is reserved. Duplicate IDs or extensions fail registry initialization.

Implement `dacp.FormatHandler` in a JAR compiled against the server. Scala providers use its trait directly; Java implementations can implement the generated JVM interface. Scala trait defaults are easiest to reuse from Scala. Providers must have a public no-argument constructor.

```scala
package example

import dacp.{Data, FileOptions, FormatHandler}
import java.nio.file.Path

class DelimitedProvider extends FormatHandler {
  val id = "custom-csv"
  val extensions = Set("customcsv")
  override val records = true
  override def rows(path: Path, options: FileOptions): Data.Rows =
    Data.delimitedRows(path, "csv")
}
```

Add `META-INF/services/dacp.FormatHandler` to the provider JAR:

```text
example.DelimitedProvider
```

Put the JAR on the server classpath (for staged distributions, in `lib/`) and restart. The registry discovers it through `ServiceLoader`. A Java provider must implement Scala collection signatures; a small Scala adapter can delegate parsing to any Java library, as Excel delegates to Apache POI.

`rows` supplies ordered column names, a lazy iterator, and an idempotent `close`. Shared default methods infer a schema from a bounded sample, stream record batches, and compute explicit counts. Override `schema`, `stream`, or `count` for formats such as Parquet that have specialized readers. `cheapSchema` and `cheapCount` run during listing and must avoid full data scans. Detection, listing, and reading must agree on the format ID and advertised capability. A format with `records = false` exposes bytes only.

The shared file reader sends original bytes without conversion. Opening records must not alter the source. Implementations must close parser resources on success, failure, and cancellation, and call the supplied `demand` callback before reading each record batch. Third-party exceptions must be mapped to `FormatFailure` with a fixed public message, without filenames, sheet names, data values, or library diagnostics. SDKs translate these into `FORMAT_ERROR`.

`FileOptions` carries private `format`, Excel `sheet`/`headerRow`, and optional ordered Arrow columns. The options stay server-side: public tickets contain only the dataset, logical Item path, and view. Schema is unavailable in ordinary CSV/NDJSON/Excel listings; opening records infers it or validates configured columns. Folder properties remain unknown and never visit descendants.

Excel uses HSSF events for XLS and SAX/XSSF events for XLSX, a 16-row queue, and cancellation checks between parser events. Shared-string dictionaries and individual rows/cells still consume memory. Numeric dates remain Excel serial values. Only cached formula results are read; formulas are never recalculated. Multiple sheets require a configured selection. This is read support, without workbook editing or spreadsheet rendering.

## Container readers

`ContainerReader` is the separate interface for enumerating children and staging an entry as a seekable file. `Containers` registers built-in readers for `zip`, `tar`, `gzip`, and `tar-gzip`; adding a container currently requires registering it there and adding its format ID in `Formats`. Container readers are not loaded through the format ServiceLoader.

`NamespaceAdapter` publishes mapped containers as folders with a bytes view returning the original file, infers parent folders, and passes staged children to existing format readers. All folder properties stay unknown. Nested containers remain binary data. GZIP's one child is named from the public alias, ignoring private filenames. Compound `.tar.gz` and `.tgz` suffixes resolve before ordinary extension detection.

ZIP listings use the archive directory without decompressing payloads. TAR and compressed TAR scan sequentially, with decompression for the latter; GZIP listings only read the header. Entry reads stage one file and clean it on success, failure, or cancellation. TAR scans validate the whole archive, including entries after the requested one, before exposing staged content. Scans and decompression must honor the supplied demand/cancellation callback.

Limits are 10,000 entries, 512 MiB per decompressed entry, and 2 GiB total declared size. TAR also bounds total decompressed bytes to that size plus 16 MiB of metadata/padding. Duplicate entries, invalid paths, and file/parent conflicts are rejected. TAR links, sparse files, and special entries are unsupported. Container roots reject records; their data children advertise views through the regular format registry.
