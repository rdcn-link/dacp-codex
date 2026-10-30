# DACP bundled plugins

[中文](README_zh.md)

All bundled extension implementations live here. Plugins depend on `dacp-plugin-api`, not `dacp-server`, and register through `META-INF/services/` inside their JARs. They use the same discovery mechanism as contributed plugins; core registries do not enumerate built-in classes.

| Module | Implementations and registration IDs | Implementation-specific dependencies |
|---|---|---|
| `xstream-data-files` | `DatasetProvider`: `directory`, `sqldb`, `mixed`; file/JDBC reading and mixed namespaces | H2 and PostgreSQL JDBC drivers |
| `xstream-data-formats-basic` | `FormatHandler`: `csv`, `ndjson`, `json`, `bin` | Apache Commons CSV |
| `xstream-data-parquet` | `FormatHandler`: `parquet`, including scan pushdown | Parquet and Hadoop |
| `xstream-data-excel` | `FormatHandler`: `excel` | Apache POI |
| `xstream-data-archives` | `ContainerReader`: `zip`, `gzip`, `tar`, `tar-gzip` | Apache Commons Compress |
| `xstream-data-catalog-static` | `DataCatalogProvider`: `static` | No additional dependencies |
| `dacp-plugin-operator-executors` | `OperatorExecutor`: `container`, `jvm`, `python`, `command` | No additional dependencies |
| `xstream-data-mounted` | `DatasetProvider`: `mounted`; generic namespace composition | No additional dependencies |
| `dacp-plugin-execution-file` | `ExecutionStoreProvider`, `MaterializationStoreProvider`: `file`; atomic execution metadata and output materialization with quotas, expiry, and integrity validation | No additional dependencies |

File, JDBC, and mixed providers share alias and directory implementation in one Dataset plugin. Format readers are packaged independently; the Dataset plugin does not depend on POI, Parquet, or archive implementation classes. JSON provides original bytes by default; NDJSON provides records. Operator programs and shared runtimes remain in `operators/`, `examples/operators/`,; they execute in containers rather than loading into the server as plugins.

## Build and run

From the repository root:

```bash
(cd xstream-data/xstream-data-files && sbt package)
(cd xstream-data/xstream-data-excel && sbt package)
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-server && sbt test 'Universal / stage')
```

Server compilation depends only on the public API and core modules such as the protocol. The standard distribution includes these plugins and their dependencies through runtime dependencies. Both `sbt run` and packaged launchers provide bundled capabilities without copying built-ins into the installation directory. Server integration tests verify real Flight access, aliases, records/bytes, scans, cleanup, and cancellation.

Third-party plugins compile against `dacp-plugin-api` as `Provided`. Put plugin JARs and private dependencies not already supplied by the server directly into the installation directory, then restart. For example:

```yaml
plugin_directory: ../.dacp-plugins
datasets:
  files:
    provider: directory
    source: {root: /srv/data}
```

`plugins/` is a version-controlled source directory; `.dacp-plugins/` is the Git-ignored local installation directory. The server does not recursively scan the source tree. Do not install duplicate copies of plugin IDs already bundled with the distribution. Catalogs, backends, formats, archives, and launchers register in `xstream.data.DataCatalogProvider`, `xstream.data.DatasetProvider`, `xstream.data.FormatHandler`, `xstream.data.ContainerReader`, and `dacp.OperatorExecutor` respectively.

## Architecture and extensions

`dacp-plugin-api` supplies contracts, generic Arrow scans and record utilities, discovery, and resource utilities. `dacp-server` retains Flight, authorization, and request/task lifecycles. Core `sources` entries contain only provider/settings; the Dataset plugin validates JDBC settings and implements connections, queries and type mapping. Production MongoDB, S3, NetCDF, and FITS implementations are not provided.

Adding a plugin for an existing extension point requires only registration, installation, and configuration, without core or client changes. See [the plugin guide](../docs/plugins.md), [Dataset/Item access](../docs/dataset-access.md), [independent contributor examples](../examples/plugins/README.md), and [executor documentation](dacp-plugin-operator-executors/README.md).

[Output materialization configuration and plugin contracts](../docs/materialization.md).
