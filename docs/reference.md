# DACP service reference

[中文](reference_zh.md)

DACP publishes Datasets and provides remote data access and Job execution. XStream supplies dataset configuration, format readers and local execution.

- [Data model and Items](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-model.md)
- [File formats, archives and reader options](https://github.com/rdcn-link/xstream-codex/blob/main/docs/file-formats.md)
- [Dataset provider configuration and extensions](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-providers.md)
- [Job language](https://github.com/rdcn-link/xstream-codex/blob/main/docs/jobs.md) · [Submit remote Jobs](jobs.md)

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

Available views and formats come from the configured XStream plugins; consult Item metadata before opening a view. The SDK wraps binary batches in an incrementally readable `io.BufferedReader`. Tickets describe resources and views; they are not authentication tokens. Both GetFlightInfo and DoGet validate published namespaces, normalized paths, and symbolic-link boundaries. Links outside allowed boundaries are excluded from listings.

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
| `task_peers` | Node origins permitted for step dispatch; the local node is always allowed; no central discovery service |
| `task_input_prefixes` | Optional execution-node input URI prefix restriction; omitted or empty means no additional prefix restriction |
| `max_concurrent_tasks` | Concurrency limit for local steps and control requests (default 2); returns UNAVAILABLE immediately when full |

Published node metadata is anonymous over TLS. Reading data requires a reader or runner subject; execution and inference require runner. Union collaboration uses node-issued service credentials and mutual TLS; peer streams must also match exact execution plans.

Service errors use Flight/gRPC statuses and explicit dacp error names: missing resources return `NOT_FOUND`; denied access returns `UNAUTHORIZED` with `FORBIDDEN`; schema conflicts return `INVALID_ARGUMENT` with `SCHEMA_CONFLICT`. Protobuf also defines error codes for protocol extensions. Any transport failure invalidates the current reader, and callers must reopen from the beginning; there are no retries within a stream.

Server `mode` defaults to `NORMAL`. Inline source requires `DEBUG` at both coordinator and worker; `source_runtimes` maps runtime references to administrator-selected `image` and `executor` settings.

## DataCatalog

`data_catalog` selects a catalog provider and passes private plugin settings through `source`; it cannot coexist with nonempty `datasets`. Omission selects the static catalog. See [dynamic catalogs](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-catalogs.md) for contracts, JDBC examples, discovery, and authorization. Standard Flight `ListFlights` returns public Dataset URIs and descriptions without additional data grants without Item contents.

## S3-compatible endpoint

Nodes enable a SigV4-authenticated, read-only HTTPS S3 endpoint by default: datasets are buckets, and Items advertising `bytes` are objects. Configure `s3.enabled` (default true), `s3.host` (default `0.0.0.0`), and `s3.port` (default actual Flight port plus 2000). Use path-style URLs and SigV4-signed requests. See the [S3-compatible service](s3-service.md) for supported operations, clients, and plugin examples.

## Union physical planning

Union selects an optimizer with `planning.provider` (default `cost`), bounds candidate search with `planning.beam_width` (default `32`, range `1–256`), and passes provider-specific values through `planning.settings`. See [physical plan generation](physical-planning.md) for the configuration, constraints and estimates, and [optimizer extensions](developers/planning-optimizer.md) for plugin development.
