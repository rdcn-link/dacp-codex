# Developer guide

[中文](README.md) · [Documentation](../README_en.md) · [User guide](../users/README_en.md)

For contributors implementing operators, dataset providers, formats, executors, SDKs, or the server. For connecting to a node, browsing data, configuring built-in providers, and running jobs, start with the user guide. Commands below run from the project root unless stated otherwise.

For catalog-level extensions and application startup, see [DatasetCatalog](../dataset-catalogs_en.md) and the [embedded server](../embedded-server_en.md).

## Architecture and extension points

Core owns contracts, discovery and lifecycle, transport, authorization, namespace validation, cancellation, and resource management. Plugins own domain behavior, private settings and dependencies. Implementations of existing extension points must not require core registry or client changes. Built-ins use the same registration contracts as external plugins.

| Module | Responsibility |
|---|---|
| `dacp-plugin-api` | Public provider, format, scan and executor contracts |
| `dacp-server` | Flight service, namespace, coordination and resource lifecycle |
| `dacp-client`, `dacp-client-scala` | Public client APIs and shell |
| `dacp-operator-runtime` | JVM operator runtime and built-in implementations |
| `plugins/dacp-plugin-operator-executors` | Container launchers and native-command bridge |
| `dacp-operator-repository` | Immutable publication, public search and downloads |
| `proto/dacp.proto` | Shared wire messages |

## Write and publish operators

- [Authoring guide](../operator-authoring_en.md): complete Scala/Python examples, builds and uploads.
- [Native commands](../native-commands_en.md): stdin/stdout adapter, executable artifacts and plugin configuration.
- [Packaging and executors](../operator-packaging_en.md): shared dependencies, immutable artifacts, repository schema and registration.
- [Stream contract](../operator-streams_en.md): named ports, frames, backpressure, cancellation and current scheduling limits.
- [DEBUG source](../operator-source_en.md): Python/Scala compilation and launch contracts; raw Java source is unsupported.

Operator outputs are transient streams. There is no result-dataset registration, saved-result mode or output TTL. Preserve scientific structures; bytes and records are implemented, while new array/image representations need a shared contract first. General pipeline scheduling and fan-out are not implemented.

## Extend datasets and formats

See [plugin contribution requirements](../plugins_en.md), [dataset provider contracts](../dataset-providers_en.md), [format and container handlers](../format-handlers_en.md), and [records query semantics](../records-scans_en.md). The example below illustrates provider implementation, configuration ownership and service registration.

### User-defined Dataset providers

A user-defined provider is a server-side Scala/JVM plugin. `provider` selects the plugin; `source` supplies private parameters for this Dataset instance. Multiple Datasets can use the same provider with different parameters. The plugin validates its settings; clients see public paths and data.

This complete provider example publishes one CSV file at the fixed public path `/records`:

```scala
package example

import dacp._
import java.nio.file.Path

class CsvFileProvider extends DatasetProvider {
  val id = "csv-file"

  def create(d: DatasetDefinition, context: DatasetContext): PublishedDataset = {
    require(d.source.keySet.subsetOf(Set("file")),
      "unknown csv-file parameter")
    require(d.source.contains("file"), "csv-file requires source.file")
    require(d.items.isEmpty,
      "csv-file does not accept item mappings")
    val file = Path.of(d.source("file").toString)
    context.createDataset(DatasetDefinition("mixed", items = Map(
      "/records" -> Map[String,Object]("file" -> file.toString, "format" -> "csv")
    )))
  }
}
```

Compile the code into a JAR against the standalone `dacp-plugin-api`, using the matching API and Scala versions and a `Provided` dependency. Include this service registration file inside the JAR:

```text
META-INF/services/dacp.DatasetProvider
```

Its contents are the implementation class name:

```text
example.CsvFileProvider
```

Place the plugin and its required dependency JARs in `plugin_directory` (without duplicating the API or dependencies supplied by the server), then restart with this configuration fragment merged with listener, TLS, and URI allowlist settings:

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  measurements:
    provider: csv-file
    source:
      file: /private/measurements.csv
```

Clients read `/records` without receiving `source.file`. Plugins needing per-item mappings receive them through `items` by default, or can override `DatasetProvider.itemSettingsKey` to declare their own configuration key; parsed mappings are always available as `definition.items`. `source` can contain nested plugin-specific parameters; the plugin must validate types, required values, and unknown parameters.

Directory providers use `reader_overrides`, SQLDB uses `tables`, and mixed uses `items`. Directory and SQLDB reject `items`. Native backends such as a future MongoDB provider can implement `PublishedDataset` and `DatasetAccess` directly for listing, metadata, counting, and streaming without mapping to files or SQL. See the [provider development guide](../../docs/dataset-providers_en.md) for interfaces, cancellation, and resource cleanup requirements. MongoDB is not implemented yet.

### File and structured-content plugins

File plugins implement `FormatHandler` and register in `META-INF/services/dacp.FormatHandler`. Each handler validates its private `options` parameters; for example, Excel uses `options: {sheet: Orders, header_row: 2}`. Common `format` and `schema` remain at the top level of reader settings.

To browse logical objects inside a file, implement `StructuredFileHandler` and register through the same format service file. Its `openStructure` returns `DatasetAccess` and can stream internal objects directly without extracting temporary files. The file root is a folder with original bytes; children explicitly advertise `records` or `bytes`. Archive readers can implement `ContainerReader` and register in `META-INF/services/dacp.ContainerReader`, without modifying core format registries.

The standalone API lives in [dacp-plugin-api](../../dacp-plugin-api). Runnable contributions and build/install instructions are in [examples/plugins](../../examples/plugins/README_en.md). `PluginChecks.listing` provides reusable metadata contract checks. The server closes request-scoped access handles and releases provider resources on shutdown. Concrete NetCDF/FITS parsers and new array/image views are not implemented yet; contributors can represent internal file objects through the current interfaces, without silently flattening arrays into records.

## Development and validation

DACP prefers plugins for extension. New dataset backends, file formats, and processing operators should implement public extension contracts and be installed as plugins, without changes to core dispatch. Built-in and community implementations should use the same contracts; plugins own their parameter validation and third-party dependencies, while clients access data through common paths and capabilities. See the [plugin contribution guide](../../docs/plugins_en.md) for contribution guidance, available mechanisms, and scientific-data extension boundaries.

Run from the project root:

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
```

After starting the server and setting the certificate environment variables, run the SDK and security checks:

```bash
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
```

Use `bash scripts/test-collaboration.sh` for cross-node container integration; it requires Docker. Tests cover namespaces, format readers, archive browsing, SQL streams, schema validation, authorization, stream lifetimes, and task collaboration.

The protocol is defined in [proto/dacp.proto](../../proto/dacp.proto). Source code lives in [dacp-server](../../dacp-server), [dacp-client](../../dacp-client) for Python, and [dacp-client-scala](../../dacp-client-scala) for Scala.


## Additional contract and integration checks

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-repository && sbt test)
```

After staging the server, building the referenced runtimes and preparing example certificates, run `.venv/bin/python scripts/test-native-execution.py` with Docker access for native streaming, partial-failure and cancellation checks. `scripts/test-source-submission.py` requires the DEBUG server from `examples/source-debug.yaml`; it checks Python/Scala heredocs. See each script and the linked guides for prerequisites and test ports.

The [technical reference](../reference_en.md) documents transport and configuration. Keep tests, examples, and Chinese/English documentation synchronized. This project is unreleased: replace superseded interfaces directly, without compatibility aliases or migration fallbacks.

## Operator review evidence

Publish optional source archives, repository revisions, license declarations/text, build instructions and SBOMs. Public pages show downloads and missing information. See [source and review information](../operator-provenance_en.md).

## Union development

The [Union protocol](../union_en.md) defines certificate identity, compute authorization, and direct streams. `dacp-union-protocol` provides shared transport helpers; `dacp-server/UnionWorker` reuses plugin data access and operator execution; `dacp-union-server` handles member polling, subjob preparation, and result forwarding. Run `cd dacp-union-server && sbt test` for two-node contracts over real mTLS. Domain analysis operators live in `examples/union/operators`, without core registry changes.

Ordinary Flight requests, Union node subtasks, and global jobs share persistent execution records. For lifecycle, atomic writes, and tests, see [execution history](../execution-history_en.md).

## Bilingual documentation checks

Run `python3 scripts/check-doc-translations.py` from the repository root. Published Markdown uses Chinese in `README.md`/`name.md` and English in `README_en.md`/`name_en.md`. Update complete translations, commands, configuration and links together. The check validates pairing, basic language presence and local document/heading links; it does not replace semantic translation review. Agent instructions and generated build output are excluded.
