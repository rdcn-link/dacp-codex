# Technical reference

[Back to project guide](../README_en.md) · [中文](reference.md)

This page covers format readers, metadata, operator contracts, and the Flight protocol. Start with the project guide if you are new to DACP.

## Formats and views

A format identifies the source representation; a view describes how it can be read. Clients use the advertised `views`, including for formats added later, rather than guessing capabilities from a filename. Format names are lowercase.

| Source | Format | Views |
|---|---|---|
| `.csv` | `csv` | `records`, `bytes` |
| `.jsonl`, `.ndjson` | `ndjson` | `records`, `bytes` |
| `.json` | `json` | `bytes` |
| `.zip`, `.ZIP` | `zip` | `bytes` |
| `.tar` | `tar` | `bytes` |
| `.gz` | `gzip` | `bytes` |
| `.tar.gz`, `.tgz` | `tar-gzip` | `bytes` |
| `.xls`, `.xlsx` | `excel` | `records`, `bytes` |
| DACP Parquet | `parquet` | `records`, `bytes` |
| SQL table | `sql` | `records`, `bytes` |
| Other files | `bin` | `bytes` |

The registry uses an explicit `format` override first, then compound container suffixes, registered signature checks, and case-insensitive extensions, with `bin` as fallback. Detection reads at most eight signature bytes; it does not validate the entire file. Invalid structured content fails when its records view is opened. A recognized file remains available unchanged through bytes. Parquet currently requires DACP Arrow schema footer metadata. Ordinary JSON has bytes only; publish newline-delimited `.json` files with `format: ndjson` to enable records. SQL uses format `sql`; its bytes view uses Arrow IPC.

### Browsing ZIP archives

Map a ZIP file using an ordinary file alias. The server publishes it as `kind=folder`, `format=zip`, with a `bytes` view returning the original archive. `list("/archive")` returns immediate children, while `list()` includes all archive entries; missing parent folders are inferred. Entry formats are selected by extension in listings and detected by existing readers when opened. ZIPs inside ZIPs remain `bin` data Items without recursive expansion. Modification time, size, and record count remain unknown for every folder.

```yaml
datasets:
  public:
    items:
      /archive: {file: /private/reports.ZIP}
```

```python
ds.list("/archive")
ds.list("/archive/reports")
with ds.open("/archive/reports/orders.csv") as stream:
    records = stream.as_records()
```

Listings read the ZIP directory without decompressing entries. Opening, inferring schema, or counting stages only the requested entry into a temporary server file, cleaned after completion, failure, or cancellation; the first response for a large entry therefore waits for decompression. Archives allow at most 10,000 entries, 512 MiB decompressed per entry, and 2 GiB total declared size. Absolute paths, traversal, duplicate entries, and file/parent conflicts are rejected. Entries never extract into the source directory or follow archived symlinks. Use `ls /archive` in `dacp-shell` to browse entries and `show /archive hex` to preview original archive bytes. Both SDKs open readable folders by advertised views; ordinary folders have no readable views.

### TAR, GZIP, and compressed TAR

Like `zip`, formats `tar`, `gzip`, and `tar-gzip` publish folders with an original-file bytes view. `.tar.gz` and `.tgz` are recognized as single compressed TAR containers exposing TAR entries directly, without an intermediate `.tar` child. Extensions are case-insensitive; explicit `format` overrides suffix detection.

Plain `.gz` exposes one decompressed data child. Public alias `/orders.csv.gz` produces `/orders.csv.gz/orders.csv`; aliases without `.gz` use `content`. Private filenames from GZIP headers are ignored, and child modification time and listing size remain unknown. Children offer records and bytes according to their content format. Concatenated GZIP members form a single decompressed stream.

```python
with Source.open("dacp://localhost:8815/data1") as ds:
    print(ds.list("/bundle_gzip"))
    with ds.open("/orders.csv.gz") as stream:
        with stream.as_bytes() as reader:
            print(reader.read(2))
    with ds.open("/orders.csv.gz/orders.csv") as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()
```

TAR listings scan sequentially; compressed TAR listings also decompress. Reading a selected entry scans the entire archive to validate its directory and checksums, staging only the requested file for existing readers. GZIP listings do not decompress the payload; reading stages its decompressed content. Temporary files are cleaned on completion, failure, or cancellation, and scans and decompression check cancellation. Indexes are not cached across requests.

As with ZIP, limits are 10,000 entries, 512 MiB decompressed per entry, and 2 GiB total declared size. TAR's decompressed stream allows an additional 16 MiB for headers, padding, and trailers. TAR accepts regular files and directories, rejects symbolic links, hard links, sparse files, devices, and other special entries, and checks headers and GZIP CRCs. Conventional leading `./` prefixes and root directory entries are normalized; other traversal, duplicate names, and file/parent conflicts are rejected. All folder properties remain unknown. Nested containers remain binary data without recursive expansion. BZ2, XZ, and Zstandard decompression are not implemented.

### Reader options and Excel

File mappings accept these private reader options:

```yaml
datasets:
  office:
    items:
      /orders:
        file: /private/sales.xlsx
        format: excel
        sheet: Orders
        header_row: 2
        schema:
          - {name: id, type: int64}
          - {name: amount, type: float64}
      /events:
        file: /private/events.json
        format: ndjson
```

`sheet` and `header_row` apply to Excel; the header row is one-based and defaults to 1. A single-sheet workbook selects its only sheet automatically; multiple sheets require an explicit selection. Publish multiple logical aliases to expose separate sheets. The original workbook is returned through bytes for each alias; records and counts refer only to its selected sheet. Sheet bindings and private filenames never appear in metadata or tickets.

Excel uses Apache POI 5.5.1 event readers for both XLS and XLSX. Empty headers become `column_N`, duplicate names receive deterministic `_2`, `_3` suffixes, and missing cells become null. Empty data rows are skipped; data beyond the header width fails. Cached formula values are read without recalculation; missing cached values become null when identifiable. Cell errors and encrypted or invalid workbooks fail with a DACP format error. Numeric cells are int64 when integral and in range, otherwise float64; booleans and strings retain their values. Excel numeric dates remain serial numbers, with no implicit locale or timezone conversion. Optional `schema` supports ordered `name`/`type` entries using `int64`, `float64`, `bool`, and `utf8`/`string`. Without it, sampling is bounded by `batch_size * infer_sample`; later incompatible values fail instead of changing the schema.

Listings do not parse CSV, NDJSON, or Excel records; their counts remain unknown unless populated at publication. Opening records infers a schema from a bounded sample. Outside optional startup counting, only explicit `count` scans these files; Parquet can obtain its count cheaply from the footer. Excel parsers have a queue of at most 16 rows and stop on close/cancellation; schemas, current batches, individual cells, and POI shared-string dictionaries use additional memory. Large shared-string tables can still require substantial memory. No full worksheet is materialized.

The server extension API is [`FormatHandler`](../dacp-server/src/main/scala/dacp/Formats.scala). Providers implement detection, optional cheap metadata, schema, record reading, explicit counting, and cleanup; file bytes use the shared reader. Add a provider JAR and register its public no-argument class in `META-INF/services/dacp.FormatHandler`, then restart. Format IDs and extension registrations must be unique; `sql` is reserved for the SQL adapter. The core server and SDKs need no format-specific changes. See [handler development](../docs/format-handlers.md).

## Item properties

Item metadata includes the following optional properties, with unknown values distinct from a known `0`:

| Property (Python / Scala) | Meaning |
|---|---|
| `last_modified_time_ms` / `lastModifiedTimeMs` | UTC Unix epoch milliseconds. Python's `last_modified_time` returns a timezone-aware UTC datetime; Scala's `lastModifiedTime` returns `Option[Instant]`. |
| `total_size_bytes` / `totalSizeBytes` | Total content bytes; file mappings use original file size. |
| `record_count` / `recordCount` | Exact logical record count. CSV excludes the header, NDJSON skips blank lines, and Excel counts data rows in the selected sheet. These files expose saved publication counts when enabled; otherwise their counts, and SQL counts, remain unknown during browsing. Use explicit `ds.count(path)`. Parquet uses footer metadata. |

Unknown values are `None` in Python and Scala (known Scala values use `Some`), with absent protobuf fields. SQL mappings do not estimate modification time or byte size. Non-record data such as PNG images has an unknown record count. All three folder properties are always unknown, including for empty folders; computing folder properties never visits child Items.

Properties are returned by `list()`, `list(path)`, and Item FlightInfo, and displayed by the interactive client's `ls`. Explicit CSV/NDJSON/Excel counts scan files and SQL counts execute queries; ordinary listing does not perform those counts. SQL counts respect the source query timeout. Folder metadata does not trigger descendant scans; size and modification time are read per request, while publication counts are retained until restart. Properties are observations rather than an atomic snapshot with subsequent reads, and can change when source data changes.

```python
with Source.open("dacp://localhost:8815/data1") as ds:
    for item in ds.list("/"):
        print(item.path, item.last_modified_time, item.total_size_bytes, item.record_count)
```

### Populating counts at publication

Set `count_file_records_on_startup: true` to count custom file mappings and initially discovered directory record files once while loading configuration and reuse those counts in ordinary browsing. It defaults to false. Counts are tied to file size and modification time; changes detected during or after counting leave the result unknown. Restarting recounts files. Explicit `count` still reads current content without changing saved metadata. SQL, archive children, and folders are excluded. Invalid record files fail startup when this option is enabled.

## Dataset construction

Built-in providers are `directory`, `sqldb`, and `custom`. Each Dataset selects one provider to interpret private `source` and `items` settings and expose a common data access interface. Directory providers discover relative paths dynamically, SQLDB providers use explicit public table aliases, and custom providers may combine sources. Omitting provider preserves existing custom configurations; legacy `datasets: {name: /directory}` shorthand is unsupported.

Directory Datasets do not follow symlinks. Folder metadata and immediate-child listings do not recursively scan ordinary child directories; `list()` discovers all files and archive entries recursively. Optional startup counting scans initial files; later files are discovered without automatic counting. Reader overrides match public paths without renaming files. SQL bindings or absolute file mappings in a directory Dataset are rejected; use custom for those combinations.

Dataset providers construct datasets, container readers browse archives, and format handlers interpret content. Future providers such as MongoDB can implement their own listing, metadata, and streaming without pretending to be files or SQL tables, or changing the protocol or SDKs. See the [provider development guide](dataset-providers.md).

## Task execution and operators

### DAG and operator contract

`input` is a single URI or an upstream `output` name; `inputs` is a list of such values. Upstream references also support `s1/result-1`. Tasks validate duplicate names, unknown dependencies, cycles, and target nodes before running serially in topological order. Execution defaults to the node hosting the first input URI, or can be set explicitly with `node: dacp://host:port`. The coordinating node resolves upstream references into concrete URIs and dispatches tasks through mTLS Flight actions; no central registry is required. Each subtask gets a new container with networking disabled, a read-only `/input` mount, and a writable `/output` mount.

`/input/manifest.json` contains `file/path/format`; `file` points to an Arrow IPC stream. The bytes view uses `data: binary`, with one raw byte chunk per row. `/input/params.json` contains the parameter object. Single inputs use a FIFO, with IPC EOS marking completion; container reads drive upstream consumption. Multiple inputs are fully materialized on disk before the container starts. Temporary input directories are removed on completion or failure, and failed output directories are also deleted. Outputs are registered only when the container exits with code 0. The maximum task timeout is 30 minutes.

The container entry point receives `operator /input /output /input/params.json`; the `DACP_OPERATOR` environment variable specifies the versioned operator name. Custom operators follow the same Arrow input contract. They produce an `/output/_output.json` manifest (`file/path/format`), where each `file` is a single-column binary Arrow IPC stream and `path` is the final filename. After successful exit, the executor decodes these streams, removes temporary IPC files, and registers the final directory. Once the Parquet footer is complete, built-in operators wrap files in this Arrow output contract with bounded memory. The built-in converter processes CSV/JSON Items individually into `part-N.parquet`, does not merge Items with different schemas, and rejects PNG and other formats.

### format_convert parameters

| Parameter | Default / supported values |
|---|---|
| `from` | `auto`; `csv` / `json` |
| `to` | `parquet` |
| `schema` | `infer` or an ordered column declaration, such as `{id: int64, amount: float64}` |
| `infer_sample` | 4; each chunk contains 1024 parsed records; the schema is fixed after sampling |
| `on_conflict` | `fail` only |
| `compression` | `zstd` / `snappy` / `none`; defaults to zstd |
| `row_group_size` | `128MB`; also accepts B / KB / GB or a byte count |

Explicit schemas support `int64`, `float64`, `bool`, and `utf8` (or `string`); column names and order must match. Empty CSV fields and JSON null values become null. JSON uses NDJSON, with one flat object per line. Conflicting types during sampling immediately produce `SCHEMA_CONFLICT`, and subsequent batches are checked against the fixed types. Explicit float64 permits valid integer representations, while explicit utf8 preserves text. There is no automatic type widening. Memory usage depends on sampled chunks, the current Arrow batch, parser buffers, and the Parquet row group, rather than total file size. An exceptionally large individual record must still fit in memory.

The converter uses Arrow Java to decode IPC and build vectors, and the native Apache Parquet Java writer with an Avro row bridge to produce Parquet; Arrow Java has no native Parquet writer. The output Arrow schema is written to both `dacp.arrow.schema` in the Parquet footer and the reserved dataset file `_schema.json`. Reserved files are not exposed as Items.

## Protocol and configuration

The sole metadata definition is [proto/dacp.proto](../proto/dacp.proto). Scala generates Java classes during the sbt build, and Python bindings are committed. After changing the protocol, regenerate them using the system protoc (3.21+):

```bash
protoc -I proto --python_out=dacp-client/dacp proto/dacp.proto
```

| Flight call | Response |
|---|---|
| GetFlightInfo `[datasets, dataset]` | `app_metadata` contains a protobuf `DataSet` with all logical Items (folder and data); no DoGet endpoint |
| GetFlightInfo `[datasets, dataset, list, /path]` | Returns only immediate children of a folder |
| GetFlightInfo `[datasets, dataset, count, /path]` | Explicit exact record count for a data Item (unknown for non-record data); no DoGet endpoint |
| GetFlightInfo `[datasets, dataset, /logical/item]` | Arrow schema and a FlightEndpoint containing a protobuf `Ticket` |
| DoGet `Ticket(dataset,item,view)` | `view=records` returns records; `view=bytes` returns a single binary column containing raw file bytes |

The records view supports CSV, NDJSON, Excel, SQL, and dacp-generated Parquet. Any file can use the bytes view. The SDK wraps binary batches in an incrementally readable `io.BufferedReader`. Tickets describe resources and views; they are not authentication tokens. Both GetFlightInfo and DoGet validate URI allowlists, normalized paths, and symbolic-link boundaries. Links outside allowed boundaries are excluded from listings.

Configuration in `examples/server.yaml`:

| Field | Meaning |
|---|---|
| `host` / `port` | Listening address / port |
| `public_host` | Hostname for external URIs; allowlists use this name and an explicit port |
| `datasets` | Dataset name → provider, source, and items; names occupy one URI path segment |
| `allowed_uri_prefixes` | Node-level URI prefix allowlist, matched at path segment boundaries; output names also require authorization |
| `tls_cert` / `tls_key` / `tls_ca` | PEM certificate, private key, and trusted client CA; mTLS is mandatory |
| `count_file_records_on_startup` | Populate directly published record file counts at startup; defaults to false and adds startup scanning cost |
| `batch_size` | Maximum rows per batch in file records views |
| `byte_chunk` | Maximum bytes per binary row in bytes views |
| `infer_sample` | Number of batches sampled for server-side records schema inference |
| `output_ttl_seconds` | Output lifetime starting at successful registration |
| `sql_sources` | Private JDBC sources (tables are not automatically published): url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
| `operator_library_url` / `operator_cache` / `operator_max_bytes` | Remote operator library URL / local immutable version cache / maximum archive size in bytes (default 8 GiB) |
| `operator_library` / `output_root` | Local operator library / temporary output root |
| `task_peers` | Node origins permitted for task dispatch; the local node is always allowed; no central discovery service |
| `task_input_prefixes` | Input URI prefixes execution nodes may pull; defaults to `allowed_uri_prefixes` when omitted |
| `max_concurrent_tasks` | Concurrency limit for local tasks and control requests (default 2); returns UNAVAILABLE immediately when full |

The prefix `dacp://localhost:8815/data1` does not match `data10`. In this version, allowlists apply to all connections verified by the node's CA; there are no permission groups based on certificate subjects. Task executors initiate mTLS using the node's configured certificates. Multiple nodes must trust the corresponding CAs.

Service errors use Flight/gRPC statuses and explicit dacp error names: missing resources return `NOT_FOUND`; expired outputs return `NOT_FOUND` with `EXPIRED` in the message; denied access returns `UNAUTHORIZED` with `FORBIDDEN`; schema conflicts return `INVALID_ARGUMENT` with `SCHEMA_CONFLICT`. Protobuf also defines error codes for protocol extensions. Any transport failure invalidates the current reader, and callers must reopen from the beginning; there are no retries within a stream. Existing DoGet tickets also fail after output expiry, and active streams check TTL at the next batch boundary.
