# Dataset providers

A Dataset is a logical namespace. Providers decide how to construct it; clients continue to use public paths, `list`, `open`, and `count`. The implementation is in [DatasetPublication.scala](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-files/src/main/scala/dacp/DatasetPublication.scala).

## Built-in providers

| Provider | Required source | Mapping key and settings |
|---|---|---|
| `directory` | `root`: private filesystem directory | `reader_overrides`: optional reader settings keyed by logical path |
| `sqldb` | `connection`: private `sources` name | `tables`: public aliases mapped with `{table: PRIVATE_TABLE}` |
| `mixed` | No source settings | `items`: explicit `{file: ...}`, `{sql: ..., table: ...}`, or `{kind: folder}` |
| `mounted` | No source settings | `mounts`: a nested `dataset` and optional `root` per public prefix |

```yaml
sources:
  warehouse:
    provider: jdbc
    settings:
      url: jdbc:postgresql://localhost/warehouse
      user_env: WAREHOUSE_USER
      password_env: WAREHOUSE_PASSWORD
datasets:
  documents:
    provider: directory
    source:
      root: /home/test/dataset1
    reader_overrides:
      /events.json: {format: ndjson}
  sales:
    provider: sqldb
    source:
      connection: warehouse
    tables:
      /customers: {table: PRIVATE_CUSTOMERS}
  research:
    provider: mixed
    items:
      /measurements: {file: /private/results.csv}
      /customers: {sql: warehouse, table: PRIVATE_CUSTOMERS}
  workspace:
    provider: mounted
    mounts:
      /documents:
        dataset:
          provider: directory
          source: {root: /srv/documents}
        root: /
```

Directory roots must exist at startup. Paths are resolved against the server working directory. Root-relative public names reflect the filesystem; all symlinks are ignored, including links pointing inside the root. Unsupported filesystem entries and names that cannot form valid logical paths are skipped. Empty directories are folders. Supported containers expose children and original-file bytes, exactly as in mixed Datasets. Directory `reader_overrides` accept only `format`, `schema`, and handler-owned `options`; they cannot add sources or aliases.

Each directory request discovers the relevant paths anew. `list()` walks the root recursively, while `list(path)` reads immediate children of ordinary directories. Metadata for an ordinary folder does not visit descendants. Container directory enumeration retains its own format-specific scanning cost. No watcher or cross-request directory index is required.

`count_file_records_on_startup: true` captures counts for mixed file mappings and initially discovered directory record files. SQL tables, containers, and archive entries are excluded. Later additions are discovered without counting; changed files lose their saved counts when size or modification time differs. Explicit `count` is independent of saved counts. Startup counting is opt in and may be expensive.

SQLDB Datasets use one connection but still require public aliases: private table names are never automatically published. Every Dataset must explicitly specify `provider`. Each Dataset has exactly one provider. Mixed combines file/JDBC sources; mounted composes arbitrary providers through public Dataset contracts and preserves their capabilities and structure. Mount prefixes must be non-root and non-overlapping; child roots default to `/`. Composition depth is limited to 32.

Directory and SQLDB reject `items`; use their provider-specific keys.

## Extending construction

Implement `xstream.data.DatasetProvider` with a unique lowercase `id` and a public no-argument constructor. Register it in a JAR through:

```text
META-INF/services/xstream.data.DatasetProvider
```

The file contains the provider's fully qualified class name, one per line. Put the JAR and its private dependencies in the configured `plugin_directory` and restart. Compile against `dacp-plugin-api` with a `Provided` dependency, not against the server implementation. Duplicate or unknown provider IDs fail configuration loading. New providers own their private `source` and provider-specific mapping settings; the server does not need a new hardcoded Dataset kind.

Provider-wide parameters belong in `source`; nested YAML maps and lists retain their Java collection representation. The provider must validate required values, types, and unknown settings. Per-logical-path settings use `items` by default. Override `DatasetProvider.itemSettingsKey` to declare a different mapping key (lowercase letters, digits, and underscores, starting with a letter; `provider` and `source` are reserved). The parser passes that key's contents as `DatasetDefinition.items`; arbitrary dataset-level keys are rejected. See the [complete CSV provider example](developers/README.md#user-defined-dataset-providers) for implementation, registration, and parameter configuration.

The lifecycle is:

1. `create(definition, context)` validates private settings and constructs a `PublishedDataset` once per server configuration. It may initialize discovery or publication metadata. Keep credentials and physical identifiers out of logs and public errors; `DatasetDefinition.toString` redacts its settings.
2. `PublishedDataset.open(check)` creates a request-scoped `DatasetAccess`. The supplied callback aborts discovery when the request is cancelled.
3. `DatasetAccess` implements recursive listing, immediate-child listing, metadata, exact counting, and stream delivery. Its `view(path, kind)` returns an optional typed `DataView`; `open(selection, StreamContext)` creates a single-use `DataStream`. Check cancellation before reads/emissions and close resources on success, failure, and cancellation. See [typed data streams](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-streams.md).

Use `Item.views = Some(Seq("records", "bytes"))` to advertise native capabilities independently of file format handlers. Record metadata must include a compatible Arrow schema before the stream starts. The local bytes view emits `ByteChunk`; the network adapter uses a single-binary-column Arrow envelope. Ordinary folders have no views, and folder size, time, and record count remain unknown. Missing resources should throw `FileNotFoundException`; public format failures use a fixed `FormatFailure` message without private details. The server applies URI authorization and sanitizes failures; do not throw transport exceptions containing backend diagnostics.

Native providers can implement streams directly, so MongoDB need not be modeled as a file or JDBC connection. MongoDB is not implemented in this change. The test [TestDatasetProvider](../dacp-server/src/test/scala/dacp/TestDatasetProvider.scala) demonstrates ServiceLoader discovery and native streaming through the unchanged Flight protocol.

Dataset providers, [container readers](format-handlers.md#container-readers), and [format handlers](format-handlers.md) have separate responsibilities: constructing a namespace, browsing embedded containers, and interpreting data content. `mixed` and `sqldb` reuse `NamespaceAdapter`; the directory provider constructs scoped namespaces as filesystem paths are discovered. Task outputs are transient item streams and do not enter the dataset namespace.

`DatasetContext` supplies batch settings, generic source settings and the current PluginEnvironment, and `createDataset` for composing installed providers, without exposing server implementation classes. Native backends can implement access directly. The server closes each request access handle; implement `PublishedDataset.close` to release provider-level resources at shutdown. Use [PluginChecks](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-core/src/main/scala/xstream/data/PluginChecks.scala) in contributor tests, and see the [standalone example](../examples/plugins/README.md).

## Records scans

`DataViews.Records` accepts a shared `Scan` for filtering, projection and limits when opened. Providers can perform source execution while retaining necessary residual filtering. `BatchDatasetAccess.readScan` supplies a bounded default. See [records scan contracts](records-scans.md#plugin-implementation-contract).

## Dynamic Dataset identities

`DatasetProvider` handles one Dataset. To discover the server-level Dataset list from a database or application, use [DataCatalog](dataset-catalogs.md). Static `datasets` configuration uses the built-in `static` catalog; a configured dynamic catalog owns the list instead. Existing Item browsing and read contracts remain unchanged.

For per-Item object/cursor handles, use `ItemDatasetAccess` and `ItemAccess`; see [the access contract guide](dataset-access.md) ([中文](dataset-access_zh.md)). `PublishedItem` is specific to built-in file/JDBC publication, not a required representation for native backends.
