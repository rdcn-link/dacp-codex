# Dataset, Item, and stream access

The DACP access chain is `DataCatalog → PublishedDataset → DatasetAccess → ItemAccess → DataView[K,S] → DataStream[K]`. `ItemAccess` is an optional per-Item implementation style; plugins that manage several Items together may implement `DatasetAccess` directly.

The catalog determines which Datasets exist; the provider determines how to access them. An Item is public metadata, without local files, SQL connections, MongoDB collections, or S3 object credentials. Clients always use Dataset names, logical Item paths, and views.

| Layer | Responsibility and lifetime |
|---|---|
| `DataCatalog` | Dynamically lists and opens Datasets; `open` returns a caller-owned Dataset lease |
| `PublishedDataset` | Owns Dataset resources; `open(check)` creates request-scoped access |
| `DatasetAccess` | Recursive and immediate-child listing, metadata, counting, scanning, and reading; closed at request end |
| `ItemAccess` | Content handle for a plugin-private object or cursor; closed after each count, scan, or read |
| Typed view / stream | `records` emits `RecordBatch`; `bytes` emits `ByteChunk`. Arrow envelopes belong to the transport boundary |

## Implementing a backend per Item

A plugin may extend `ItemDatasetAccess`, implementing `list()`, `list(path)`, `describe(path)`, and `openItem(path)`. The returned `ItemAccess` implements `count()` and `view(kind)`. Return `None` for unsupported capabilities; a supported `DataView` opens a `DataStream` with a typed selection. Records use `Scan`, bytes use `AllBytes`. `BatchItemAccess` is an optional Arrow callback helper: implement `read` and override `readScan` for source pushdown, or use its bounded fallback. See [typed streams and scientific representations](data-streams.md).

`describe` must explicitly advertise the plugin's native `Item.views`, such as `Some(Seq("records"))` or `Some(Seq("bytes"))`. The type name does not need file-format registration. Return `None` for unknown counts; ordinary folders have no content views. Structure roots supporting original bytes may explicitly advertise `bytes`.

`ItemDatasetAccess` validates logical paths and declared views, opens the Item handle during consumption, and closes it on success, exceptions, and cancellation. `openItem` must release partially acquired resources if it fails before returning. `DataStream.consume` runs synchronously: call `consumer.start(descriptor)` once, even for an empty stream, and `consumer.accept(chunk)` for each borrowed block. Successful return means completion. Call `context.check()` before reading and emitting; close Arrow roots, input streams and cursors in `finally`. Consumers must copy blocks they retain after the callback. Do not close the supplied allocator or application-owned connection pools.

See [MessagePlugins.scala](../examples/plugins/src/main/scala/example/MessagePlugins.scala) for a complete compilable example: one native Dataset serves `/message` as bytes and `/summary` as records, without local files or JDBC. This is a small-payload demonstration; production objects require bounded chunks.

## Registration and configuration

Implement `DatasetProvider`, register it in `META-INF/services/xstream.data.DatasetProvider`, install the plugin and private dependency JARs in `plugin_directory`, and restart. For the example plugin:

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  messages:
    provider: example-message
    source:
      message: hello
```

A dynamic catalog can return a plugin-created `PublishedDataset` directly or create one through `CatalogContext.createDataset`; it need not use `PublishedItem`. `PublishedItem` and `FileDatasets.mixed` are construction helpers for the built-in file/JDBC mixed provider, rather than a universal backend model.

## MongoDB and S3 integration points

A MongoDB provider can publish collections as logical Items, create cursors in `openItem`, and emit records. It must define field, nested-document, and type mappings without silently losing structures. An S3 provider can map prefixes/keys to folders and Items, open object response bodies through handles, and emit chunked bytes. Supporting records additionally requires explicit parsers, schemas, and scan semantics. Plugins own authentication, pagination, timeouts, cancellation, and dependencies. Clients, tasks, and Union still read through the same `DatasetAccess` and Flight protocol.

These extension contracts and independent plugin examples are implemented; **production MongoDB and S3 connectors are not implemented yet**. File format detection, archives, and structured browsing belong to the file provider; SQL query optimization belongs to JDBC access. `DataView` discovery and consumption do not recognize storage types. Local execution and S3 consume typed blocks; network encoding belongs to `FlightDataStreams`.

Contract tests cover independent plugin Flight listing and records/bytes, native records scans and counts, and Item handle cleanup on success, failure, and cancellation. See [the plugin guide](plugins.md), [provider configuration](dataset-providers.md), and [dynamic catalogs](dataset-catalogs.md).

## Common JSONL bytes view

When a plugin declares `records` without `bytes`, DACP Server adds a bytes view at its shared Dataset access boundary. `list`, `list(path)`, and `describe` advertise both capabilities. Flight, S3, ordinary tasks, and Union node inputs use the same access object. Plugins keep implementing only their native records stream; no bytes branch is required. Hosts using DatasetAccess directly can reuse the `ItemByteViews.wrap` adapter.

The representation is UTF-8 without a BOM, one JSON object per record with field names as keys, and a trailing `\n` per line. An empty record stream produces zero bytes. For example:

```jsonl
{"station":"北京","temperature":20.5,"quality":null}
{"station":"A","temperature":18.0,"quality":"ok"}
```

Strings are escaped, integers and decimals remain numbers, nulls remain null, structs remain nested objects, and lists remain arrays without expanding them into rows. Binary fields become Base64 strings; Java temporal values become strings; non-finite floating-point values become `"NaN"`, `"Infinity"`, or `"-Infinity"`. Arrow maps retain their entries-array structure. Top-level dictionary encoding is decoded to values. Nested dictionaries, unsupported value types, and duplicate field names fail explicitly instead of undergoing lossy conversion. The rule only applies to scientific files when a plugin explicitly provides records; it does not automatically flatten file structures.

If the plugin declares bytes, its existing implementation always takes priority, including when that implementation fails. `format` and the Arrow schema retain the original record identity and structure; JSONL is not presented as original file bytes. Derived JSONL has unknown `totalSizeBytes` and retains the original record count. HEAD and catalog access do not scan records to determine size. S3 Range requests for unknown-size objects return 501.

Conversion uses bounded `byte_chunk` chunks that may split records or UTF-8 characters. Clients must decode the concatenated byte stream rather than parse chunks independently as JSON. Output is flushed after each input batch, honors backpressure and cancellation, and neither buffers the entire record set nor stages to disk. Source or conversion errors propagate; already emitted content is an incomplete result and must not be treated as success.
