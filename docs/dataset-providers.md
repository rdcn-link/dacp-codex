# Dataset providers

A Dataset is a logical namespace. Providers decide how to construct it; clients continue to use public paths, `list`, `open`, and `count`. The implementation is in [DatasetProviders.scala](../dacp-server/src/main/scala/dacp/DatasetProviders.scala).

## Built-in providers

| Provider | Required source | Item settings |
|---|---|---|
| `directory` | `root`: private filesystem directory | Optional reader overrides keyed by relative logical path |
| `sqldb` | `connection`: private `sql_sources` name | Public aliases mapped with `{table: PRIVATE_TABLE}` |
| `custom` | No source settings | Explicit `{file: ...}`, `{sql: ..., table: ...}`, or `{kind: folder}` |

```yaml
datasets:
  documents:
    provider: directory
    source:
      root: /home/test/dataset1
    items:
      /events.json: {format: ndjson}
  sales:
    provider: sqldb
    source:
      connection: warehouse
    items:
      /customers: {table: PRIVATE_CUSTOMERS}
  research:
    provider: custom
    items:
      /measurements: {file: /private/results.csv}
      /customers: {sql: warehouse, table: PRIVATE_CUSTOMERS}
```

Directory roots must exist at startup. Paths are resolved against the server working directory. Root-relative public names reflect the filesystem; all symlinks are ignored, including links pointing inside the root. Unsupported filesystem entries and names that cannot form valid logical paths are skipped. Empty directories are folders. Supported containers expose children and original-file bytes, exactly as in custom Datasets. Directory `items` accept only `format`, `sheet`, `header_row`, and `schema`; they cannot add sources or aliases.

Each directory request discovers the relevant paths anew. `list()` walks the root recursively, while `list(path)` reads immediate children of ordinary directories. Metadata for an ordinary folder does not visit descendants. Container directory enumeration retains its own format-specific scanning cost. No watcher or cross-request directory index is required.

`count_file_records_on_startup: true` captures counts for custom file mappings and initially discovered directory record files. SQL tables, containers, and archive entries are excluded. Later additions are discovered without counting; changed files lose their saved counts when size or modification time differs. Explicit `count` is independent of saved counts. Startup counting is opt in and may be expensive.

SQLDB Datasets use one connection but still require public aliases: private table names are never automatically published. Existing configurations containing only `items` default to custom. `sql_datasets` and the old directory string shorthand remain unsupported. Each Dataset has exactly one provider; custom is the mode for combining independent sources.

## Extending construction

Implement `dacp.DatasetProvider` with a unique lowercase `id` and a public no-argument constructor. Register it in a JAR through:

```text
META-INF/services/dacp.DatasetProvider
```

The file contains the provider's fully qualified class name, one per line. Put the JAR and its dependencies on the server classpath (`lib/` in a staged installation) and restart. Duplicate or unknown provider IDs fail configuration loading. New providers own their private `source` and `items` settings; the server does not need a new hardcoded Dataset kind.

The lifecycle is:

1. `create(definition, config)` validates private settings and constructs a `PublishedDataset` once per Config instance. It may initialize discovery or publication metadata. Keep credentials and physical identifiers out of logs and public errors; `DatasetDefinition.toString` redacts its settings.
2. `PublishedDataset.open(check)` creates a request-scoped `DatasetAccess`. The supplied callback aborts discovery when the request is cancelled.
3. `DatasetAccess` implements recursive listing, immediate-child listing, metadata, exact counting, and stream delivery. Its `stream` receives the allocator, listener, and readiness callback. Honor readiness/cancellation before reading or emitting batches and close resources on success, failure, and cancellation.

Use `Item.views = Some(Seq("records", "bytes"))` to advertise native capabilities independently of file format handlers. Record metadata must include a compatible Arrow schema before the stream starts. The bytes view uses the existing Arrow single-binary-column envelope. Ordinary folders have no views, and folder size, time, and record count remain unknown. Missing resources should throw `FileNotFoundException`; public format failures use a fixed `FormatFailure` message without private details. The server applies URI authorization and sanitizes failures; do not throw transport exceptions containing backend diagnostics.

Native providers can implement streams directly, so MongoDB need not be modeled as a file or JDBC connection. MongoDB is not implemented in this change. The test [TestDatasetProvider](../dacp-server/src/test/scala/dacp/TestDatasetProvider.scala) demonstrates ServiceLoader discovery and native streaming through the unchanged Flight protocol.

Dataset providers, [container readers](format-handlers.md#container-readers), and [format handlers](format-handlers.md) have separate responsibilities: constructing a namespace, browsing embedded containers, and interpreting data content. `custom` and `sqldb` reuse `NamespaceAdapter`; the directory provider constructs scoped namespaces as filesystem paths are discovered. Task outputs keep their generated logical aliases and TTL lifecycle.
