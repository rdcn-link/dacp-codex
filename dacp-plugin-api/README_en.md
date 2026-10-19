# DACP plugin API

[中文](README.md)

Shared Scala/JVM contracts for Dataset catalogs, dataset providers, file handlers, structured-file browsing, container readers, and operator launch executors. This module depends on Arrow and Scala, and has no server, SQL driver, Docker, Excel, or HTTP implementation dependencies.

From the repository root:

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd examples/plugins && sbt package)
```

Contributed plugins compile against `io.github.rdcn-link:dacp-plugin-api_2.13:0.1.0-SNAPSHOT` as `Provided`. The server supplies the matching API at runtime. This is an unreleased API; update contributors and core together when contracts change.

- `DatasetCatalog`, `DatasetCatalogProvider`, `CatalogContext`, and `DatasetInfo`: discover Datasets dynamically and acquire independent access leases; see [dynamic catalogs](../docs/dataset-catalogs_en.md).
- `DatasetProvider`, `DatasetDefinition`, `DatasetContext`, `PublishedDataset`, and `DatasetAccess`: construct and access native namespaces.
- `FormatHandler`, `FileOptions`, and `Records`: interpret files and stream Arrow batches.
- `StructuredFileHandler`: expose file-internal logical objects directly as a namespace.
- `OperatorExecutor`, `OperatorProgram`, and `OperatorCommand`: validate operator artifacts and select container launch commands; see [operator packaging](../docs/operator-packaging.md).
- `ContainerReader`: enumerate archive entries and stage individual files for existing format readers.
- `Item`, `PublishedItem`, `LogicalPath`, `PluginSchemas`, and `FormatFailure`: common public/private contracts.
- `PluginChecks.listing`: reusable metadata checks for contributor tests, without reading or counting content.

See the [plugin guide](../docs/plugins.md) and the [standalone example](../examples/plugins/README_en.md) for configuration, registration, lifecycle, and tests. Compile with the same Scala version as the server. Keep third-party libraries out of the core; package required dependencies alongside the plugin without copying server-provided APIs.

Records query contracts include `Scan`, typed `Literal`/`Predicate` trees, schema-bound validation, and `ScanStreams` for bounded filtering/projection with early termination. Providers and file handlers can override `scan` for source optimizations while preserving the common semantics. See [records queries](../docs/records-scans.md).

Operator `OperatorProgram.configuration` carries plugin-owned `executor_config`; `inputs` and `outputs` expose the supported representations per port. Plugins validate settings; core does not interpret executor-specific parameters. See [native commands](../docs/native-commands_en.md).

`OperatorCommand.executableScratch` requests a bounded 512 MiB executable tmpfs at `/operator-work` for private native artifact copies; ordinary `/tmp` remains non-executable. The server manages its resource lifecycle.
