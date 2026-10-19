# Dataset providers

A Dataset is a logical namespace. Providers decide how to construct it; clients continue to use public paths, `list`, `open`, and `count`. The implementation is in [DatasetProviders.scala](../dacp-server/src/main/scala/dacp/DatasetProviders.scala).

## Built-in providers

| Provider | Required source | Mapping key and settings |
|---|---|---|
| `directory` | `root`: private filesystem directory | `reader_overrides`: optional reader settings keyed by logical path |
| `sqldb` | `connection`: private `sql_sources` name | `tables`: public aliases mapped with `{table: PRIVATE_TABLE}` |
| `mixed` | No source settings | `items`: explicit `{file: ...}`, `{sql: ..., table: ...}`, or `{kind: folder}` |

```yaml
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
```

Directory roots must exist at startup. Paths are resolved against the server working directory. Root-relative public names reflect the filesystem; all symlinks are ignored, including links pointing inside the root. Unsupported filesystem entries and names that cannot form valid logical paths are skipped. Empty directories are folders. Supported containers expose children and original-file bytes, exactly as in mixed Datasets. Directory `reader_overrides` accept only `format`, `schema`, and handler-owned `options`; they cannot add sources or aliases.

Each directory request discovers the relevant paths anew. `list()` walks the root recursively, while `list(path)` reads immediate children of ordinary directories. Metadata for an ordinary folder does not visit descendants. Container directory enumeration retains its own format-specific scanning cost. No watcher or cross-request directory index is required.

`count_file_records_on_startup: true` captures counts for mixed file mappings and initially discovered directory record files. SQL tables, containers, and archive entries are excluded. Later additions are discovered without counting; changed files lose their saved counts when size or modification time differs. Explicit `count` is independent of saved counts. Startup counting is opt in and may be expensive.

SQLDB Datasets use one connection but still require public aliases: private table names are never automatically published. Every Dataset must explicitly specify `provider`. Each Dataset has exactly one provider; mixed is the mode for combining independent sources.

Directory and SQLDB reject `items`; use their provider-specific keys.

## Extending construction

Implement `dacp.DatasetProvider` with a unique lowercase `id` and a public no-argument constructor. Register it in a JAR through:

```text
META-INF/services/dacp.DatasetProvider
```

The file contains the provider's fully qualified class name, one per line. Put the JAR and its private dependencies in the configured `plugin_directory` and restart. Compile against `dacp-plugin-api` with a `Provided` dependency, not against the server implementation. Duplicate or unknown provider IDs fail configuration loading. New providers own their private `source` and provider-specific mapping settings; the server does not need a new hardcoded Dataset kind.

Provider-wide parameters belong in `source`; nested YAML maps and lists retain their Java collection representation. The provider must validate required values, types, and unknown settings. Per-logical-path settings use `items` by default. Override `DatasetProvider.itemSettingsKey` to declare a different mapping key (lowercase letters, digits, and underscores, starting with a letter; `provider` and `source` are reserved). The parser passes that key's contents as `DatasetDefinition.items`; arbitrary dataset-level keys are rejected. See the [complete CSV provider example](developers/README_en.md#user-defined-dataset-providers) for implementation, registration, and parameter configuration.

The lifecycle is:

1. `create(definition, context)` validates private settings and constructs a `PublishedDataset` once per server configuration. It may initialize discovery or publication metadata. Keep credentials and physical identifiers out of logs and public errors; `DatasetDefinition.toString` redacts its settings.
2. `PublishedDataset.open(check)` creates a request-scoped `DatasetAccess`. The supplied callback aborts discovery when the request is cancelled.
3. `DatasetAccess` implements recursive listing, immediate-child listing, metadata, exact counting, and stream delivery. Its `stream` receives the allocator, listener, and readiness callback. Honor readiness/cancellation before reading or emitting batches and close resources on success, failure, and cancellation.

Use `Item.views = Some(Seq("records", "bytes"))` to advertise native capabilities independently of file format handlers. Record metadata must include a compatible Arrow schema before the stream starts. The bytes view uses the existing Arrow single-binary-column envelope. Ordinary folders have no views, and folder size, time, and record count remain unknown. Missing resources should throw `FileNotFoundException`; public format failures use a fixed `FormatFailure` message without private details. The server applies URI authorization and sanitizes failures; do not throw transport exceptions containing backend diagnostics.

Native providers can implement streams directly, so MongoDB need not be modeled as a file or JDBC connection. MongoDB is not implemented in this change. The test [TestDatasetProvider](../dacp-server/src/test/scala/dacp/TestDatasetProvider.scala) demonstrates ServiceLoader discovery and native streaming through the unchanged Flight protocol.

Dataset providers, [container readers](format-handlers.md#container-readers), and [format handlers](format-handlers.md) have separate responsibilities: constructing a namespace, browsing embedded containers, and interpreting data content. `mixed` and `sqldb` reuse `NamespaceAdapter`; the directory provider constructs scoped namespaces as filesystem paths are discovered. Task outputs are transient item streams and do not enter the dataset namespace.

`DatasetContext` supplies batch settings and optional namespace publication helpers (`publish`, `openNamespace`), without exposing server implementation classes. Native backends can implement access directly. The server closes each request access handle; implement `PublishedDataset.close` to release provider-level resources at shutdown. Use [PluginChecks](../dacp-plugin-api/src/main/scala/dacp/PluginChecks.scala) in contributor tests, and see the [standalone example](../examples/plugins/README.md).

## Records scans

`DatasetAccess.scan` accepts a shared `Scan` request for filtering, projection, and limits. Its default implementation streams records through the common evaluator. Providers may override it for source execution, retaining residual filtering where needed. See [records scan contracts](records-scans.md#plugin-implementation-contract).
