# DACP plugin API

Shared Scala/JVM contracts for dataset providers, file handlers, structured-file browsing, and container readers. This module depends on Arrow and Scala, and has no server, SQL driver, Docker, Excel, or HTTP implementation dependencies.

From the repository root:

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd examples/plugins && sbt package)
```

Contributed plugins compile against `io.github.rdcn-link:dacp-plugin-api_2.13:0.1.0-SNAPSHOT` as `Provided`. The server supplies the matching API at runtime. This is an unreleased API; update contributors and core together when contracts change.

- `DatasetProvider`, `DatasetDefinition`, `DatasetContext`, `PublishedDataset`, and `DatasetAccess`: construct and access native namespaces.
- `FormatHandler`, `FileOptions`, and `Records`: interpret files and stream Arrow batches.
- `StructuredFileHandler`: expose file-internal logical objects directly as a namespace.
- `ContainerReader`: enumerate archive entries and stage individual files for existing format readers.
- `Item`, `PublishedItem`, `LogicalPath`, `PluginSchemas`, and `FormatFailure`: common public/private contracts.
- `PluginChecks.listing`: reusable metadata checks for contributor tests, without reading or counting content.

See the [plugin guide](../docs/plugins.md) and the [standalone example](../examples/plugins/README.md) for configuration, registration, lifecycle, and tests. Compile with the same Scala version as the server. Keep third-party libraries out of the core; package required dependencies alongside the plugin without copying server-provided APIs.
