# Technical reference

[Back to project guide](../README.md) · [中文](reference_zh.md)

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
    provider: mixed
    items:
      /archive: {file: /private/reports.ZIP}
```

```python
ds.list("/archive")
ds.list("/archive/reports")
with ds.open("/archive/reports/orders.csv") as stream:
    records = stream.as_records()
```

Listings read the ZIP directory without decompressing entries. Opening, inferring schema, or counting stages only the requested entry into a temporary server file, cleaned after completion, failure, or cancellation; the first response for a large entry therefore waits for decompression. Archives allow at most 10,000 entries, 512 MiB decompressed per entry, and 2 GiB total declared size. Absolute paths, traversal, duplicate entries, and file/parent conflicts are rejected. Entries never extract into the source directory or follow archived symlinks. Use `ls /archive` in `dacp-shell` to browse entries and `cat /archive hex` to preview original archive bytes. Both SDKs open readable folders by advertised views; ordinary folders have no readable views.

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
    provider: mixed
    items:
      /orders:
        file: /private/sales.xlsx
        format: excel
        options: {sheet: Orders, header_row: 2}
        schema:
          - {name: id, type: int64}
          - {name: amount, type: float64}
      /events:
        file: /private/events.json
        format: ndjson
```

`options.sheet` and `options.header_row` are validated by the Excel handler; the header row is one-based and defaults to 1. A single-sheet workbook selects its only sheet automatically; multiple sheets require an explicit selection. Publish multiple logical aliases to expose separate sheets. The original workbook is returned through bytes for each alias; records and counts refer only to its selected sheet. Sheet bindings and private filenames never appear in metadata or tickets.

Excel uses Apache POI 5.5.1 event readers for both XLS and XLSX. Empty headers become `column_N`, duplicate names receive deterministic `_2`, `_3` suffixes, and missing cells become null. Empty data rows are skipped; data beyond the header width fails. Cached formula values are read without recalculation; missing cached values become null when identifiable. Cell errors and encrypted or invalid workbooks fail with a DACP format error. Numeric cells are int64 when integral and in range, otherwise float64; booleans and strings retain their values. Excel numeric dates remain serial numbers, with no implicit locale or timezone conversion. Optional `schema` supports ordered `name`/`type` entries using `int64`, `float64`, `bool`, and `utf8`/`string`. Without it, sampling is bounded by `batch_size * infer_sample`; later incompatible values fail instead of changing the schema.

Listings do not parse CSV, NDJSON, or Excel records; their counts remain unknown unless populated at publication. Opening records infers a schema from a bounded sample. Outside optional startup counting, only explicit `count` scans these files; Parquet can obtain its count cheaply from the footer. Excel parsers have a queue of at most 16 rows and stop on close/cancellation; schemas, current batches, individual cells, and POI shared-string dictionaries use additional memory. Large shared-string tables can still require substantial memory. No full worksheet is materialized.

The server extension API is [`FormatHandler`](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-core/src/main/scala/xstream/data/FormatHandler.scala). Providers implement detection, optional cheap metadata, schema, record reading, explicit counting, and cleanup; file bytes use the shared reader. Add a provider JAR and register its public no-argument class in `META-INF/services/xstream.data.FormatHandler`, then restart. Format IDs and extension registrations must be unique; `sql` is reserved for the SQL adapter. The core server and SDKs need no format-specific changes. See [handler development](format-handlers.md).

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

Set `count_file_records_on_startup: true` to count mixed file mappings and initially discovered directory record files once while loading configuration and reuse those counts in ordinary browsing. It defaults to false. Counts are tied to file size and modification time; changes detected during or after counting leave the result unknown. Restarting recounts files. Explicit `count` still reads current content without changing saved metadata. SQL, archive children, and folders are excluded. Invalid record files fail startup when this option is enabled.

`plugin_directory` optionally selects a directory containing plugin and dependency JARs. It is loaded before registry initialization and requires a restart after changes. Reader-specific parameters go under `options`; handlers validate them. See [plugins](plugins.md) for the standalone API and structured-file contracts.

## Dataset construction

Directory uses optional `reader_overrides`, SQLDB uses `tables` mappings from public paths to private tables, and mixed uses `items`. Directory and SQLDB reject `items`. Plugins default to `items` and may customize the mapping key through `DatasetProvider.itemSettingsKey`.

Built-in providers are `directory`, `sqldb`, and `mixed`. Each Dataset selects one provider to interpret private `source` and provider-specific mapping settings and expose a common data access interface. Directory providers discover relative paths dynamically, SQLDB providers use explicit public table aliases, and mixed providers may combine sources. Every Dataset must explicitly specify `provider`.

Directory Datasets do not follow symlinks. Folder metadata and immediate-child listings do not recursively scan ordinary child directories; `list()` discovers all files and archive entries recursively. Optional startup counting scans initial files; later files are discovered without automatic counting. Reader overrides match public paths without renaming files. SQL bindings or absolute file mappings in a directory Dataset are rejected; use mixed for those combinations.

Dataset providers construct datasets, container readers browse archives, and format handlers interpret content. Future providers such as MongoDB can implement their own listing, metadata, and streaming without pretending to be files or SQL tables, or changing the protocol or SDKs. See the [provider development guide](dataset-providers.md).

## Task execution and operators

Task inputs reference only upstream outputs as `task[:port]`, defaulting to `result`. External Items enter through `read_item@1.0` with `params.uri/view`. See [Job format](jobs.md). Outputs are named streams, never saved datasets. The shell supports `run OP --input source=/item` and `submit job.yaml`, and prints results incrementally. Ordinary jobs use TLS and bearer-authenticated Flight DoExchange; pipeline tasks run concurrently with bounded queues. Each output port has at most one downstream consumer; consume multiple inputs in lexical port order.

Operator launch arguments are `operator /input /input/params.json`. Inputs are Arrow IPC FIFOs described by `/input/manifest.json`. Stdout carries framed start/data/end events; diagnostics go to stderr. No output directory, output registration, TTL, or result-dataset URI is used. Parquet conversion uses private ephemeral scratch to complete the footer, then streams bytes.

See the [authoring contract](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md), [stream contracts](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams.md), and [DEBUG-only heredoc source](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-source.md) for source examples, ports, framing, runtime allowlists, cancellation, and limits.

Operators resolve from the local directory and installed image first, then completed cached downloads, then remote URLs in configured order. A manifest HTTP 404 tries the next library; other download or validation failures stop execution. Configure `remote_operator_library_urls: [http://library-a:8820, https://library-b:8820]`; an empty list uses only local operators.

Runtime images and operator code are separate content-addressed artifacts. Built-in launch plugins support complete containers, Java/Scala JARs, Python source, and wheels, all within isolated runtime containers. See [operator packaging](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging.md) for definitions, shared caching, executor registration, and current limitations.


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

The records view supports CSV, NDJSON, Excel, SQL, and dacp-generated Parquet. Any file can use the bytes view. The SDK wraps binary batches in an incrementally readable `io.BufferedReader`. Tickets describe resources and views; they are not authentication tokens. Both GetFlightInfo and DoGet validate published namespaces, normalized paths, and symbolic-link boundaries. Links outside allowed boundaries are excluded from listings.

Configuration in `examples/server.yaml`:

| Field | Meaning |
|---|---|
| `host` / `port` | Listening address / port |
| `public_host` | Hostname for external URIs |
| `public_port` | Advertised port, defaulting to the listening port; may differ from `port` behind a relay |
| `datasets` | Dataset name → provider, source, and items; names occupy one URI path segment |
| `tls_cert` / `tls_key` / `tls_ca` | PEM server certificate, private key and trusted CA; ordinary clients use TLS / tokens, collaboration uses mutual TLS |
| `count_file_records_on_startup` | Populate directly published record file counts at startup; defaults to false and adds startup scanning cost |
| `batch_size` | Maximum rows per batch in file records views |
| `byte_chunk` | Maximum bytes per binary row in bytes views |
| `infer_sample` | Number of batches sampled for server-side records schema inference |
| `sources` | Private JDBC sources (tables are not automatically published): url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
| `remote_operator_library_urls` / `operator_cache` / `operator_max_bytes` | Ordered remote operator library URLs / local immutable version cache / maximum size per artifact in bytes (default 8 GiB) |
| `local_operator_library` | Local operator library |
| `task_peers` | Node origins permitted for task dispatch; the local node is always allowed; no central discovery service |
| `task_input_prefixes` | Optional execution-node input URI prefix restriction; omitted or empty means no additional prefix restriction |
| `max_concurrent_tasks` | Concurrency limit for local tasks and control requests (default 2); returns UNAVAILABLE immediately when full |

Published node datasets are available to authenticated TLS / token clients without an additional data-read allowlist. Union collaboration uses node-issued service credentials and mutual TLS; peer streams must also match exact execution plans.

Service errors use Flight/gRPC statuses and explicit dacp error names: missing resources return `NOT_FOUND`; denied access returns `UNAUTHORIZED` with `FORBIDDEN`; schema conflicts return `INVALID_ARGUMENT` with `SCHEMA_CONFLICT`. Protobuf also defines error codes for protocol extensions. Any transport failure invalidates the current reader, and callers must reopen from the beginning; there are no retries within a stream.

Server `mode` defaults to `NORMAL`. Inline source requires `DEBUG` at both coordinator and worker; `source_runtimes` maps runtime references to administrator-selected `image` and `executor` settings.

## DataCatalog

`data_catalog` selects a catalog provider and passes private plugin settings through `source`; it cannot coexist with nonempty `datasets`. Omission selects the static catalog. See [dynamic catalogs](dataset-catalogs.md) for contracts, JDBC examples, discovery, and authorization. Standard Flight `ListFlights` returns public Dataset URIs and descriptions without additional data grants without Item contents.

## S3-compatible endpoint

Nodes enable a SigV4-authenticated, read-only HTTPS S3 endpoint by default: datasets are buckets, and Items advertising `bytes` are objects. Configure `s3.enabled` (default true), `s3.host` (default `0.0.0.0`), and `s3.port` (default actual Flight port plus 2000). Use path-style URLs and SigV4-signed requests. See the [S3-compatible service](s3-service.md) for supported operations, clients, and plugin examples.
