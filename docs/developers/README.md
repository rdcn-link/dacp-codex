# Developer guide

[中文](README_zh.md) · [Documentation](../README.md) · [User guide](../users/README.md)

The built-in `read_item@1.0` handles transport only; Dataset/Item plugins own backends and formats. See [Jobs](../jobs.md) for stream dependency contracts.

For contributors implementing operators, dataset providers, formats, executors, SDKs, or the server. For connecting to a node, browsing data, configuring built-in providers, and running jobs, start with the user guide. Commands below run from the project root unless stated otherwise.

For catalog-level extensions and application startup, see [DataCatalog](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-catalogs.md) and the [embedded server](../embedded-server.md).

## Architecture and extension points

Core owns contracts, discovery and lifecycle, transport, authorization, namespace validation, cancellation, and resource management. Plugins own domain behavior, private settings and dependencies. Implementations of existing extension points must not require core registry or client changes. Built-ins use the same registration contracts as external plugins.

| Module | Responsibility |
|---|---|
| `dacp-server` | Flight service, namespace, coordination and resource lifecycle |
| `dacp-client`, `dacp-client-scala` | Public client APIs and shell |
| `proto/dacp.proto` | Shared wire messages |

## Write and publish operators

- [Authoring guide](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md): complete Scala/Python examples, builds and uploads.
- [Native commands](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands.md): stdin/stdout adapter, executable artifacts and plugin configuration.
- [Packaging and executors](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging.md): shared dependencies, immutable artifacts, repository schema and registration.
- [Stream contract](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams.md): named ports, frames, backpressure, cancellation and current scheduling limits.
- [DEBUG source](https://github.com/rdcn-link/dacp-codex/blob/main/docs/operator-source.md): Python/Scala compilation and launch contracts; raw Java source is unsupported.

Operator outputs are transient streams. There is no result-dataset registration, saved-result mode or output TTL. Bytes, records, arrays and sequences share Arrow streaming contracts; scientific dimensions remain explicit. General pipeline scheduling and fan-out are not implemented.

## Integrate XStream data extensions

To add Dataset providers, format or container handlers, or records queries, see [XStream extension development](http://xstream.conoai.cn/docs.html) for implementation and registration. DACP administrators install plugins and dependencies in `plugin_directory` and select providers in node configuration; see [plugin integration](../plugins.md).

## Development and validation

DACP prefers plugins for extension. New dataset backends, file formats, and processing operators should implement public extension contracts and be installed as plugins, without changes to core dispatch. Built-in and community implementations should use the same contracts; plugins own their parameter validation and third-party dependencies, while clients access data through common paths and capabilities. See the [plugin contribution guide](../plugins.md) for contribution guidance, available mechanisms, and scientific-data extension boundaries.

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

Use `bash scripts/test-collaboration.sh` for cross-node container integration; it requires Docker. Tests cover namespaces, format readers, archive browsing, SQL streams, schema validation, authorization, stream lifetimes, and step collaboration.

The protocol is defined in [proto/dacp.proto](../../proto/dacp.proto). Source code lives in [dacp-server](../../dacp-server), [dacp-client](../../dacp-client) for Python, and [dacp-client-scala](../../dacp-client-scala) for Scala.


## Additional contract and integration checks

```bash
(cd /path/to/xstream-codex/plugins/xstream-plugin-operator-executors && sbt test)
(cd /path/to/xstream-operator-repository && sbt test)
```

After staging the server, building the referenced runtimes and preparing example certificates, run `.venv/bin/python scripts/test-native-execution.py` with Docker access for native streaming, partial-failure and cancellation checks. `scripts/test-source-submission.py` requires the DEBUG server from `examples/source-debug.yaml`; it checks Python/Scala heredocs. See each script and the linked guides for prerequisites and test ports.

The [technical reference](../reference.md) documents transport and configuration. Keep tests, examples, and Chinese/English documentation synchronized. Public contracts, callers and examples must use consistent names and field definitions.

## Operator review evidence

Publish optional source archives, repository revisions, license declarations/text, build instructions and SBOMs. Public pages show downloads and missing information. See [source and review information](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-provenance.md).

## Union development

The [Union protocol](../union.md) defines certificate identity, compute authorization, and direct streams. `dacp-union-protocol` provides shared transport helpers; `dacp-server/UnionWorker` reuses plugin data access and operator execution; `dacp-union-server` handles member polling, subjob preparation, and result forwarding. Run `cd dacp-union-server && sbt test` for two-node contracts over real mTLS. Domain analysis operators live in `examples/union/operators`, without core registry changes.

Ordinary Flight requests, Union node subtasks, and global jobs share persistent execution records. For lifecycle, atomic writes, and tests, see [execution history](../execution-history.md).


Union provides standard DACP discovery, Items, Jobs and S3. For public short names, placement and management access, see [Union proxy service](../union-proxy.md).

Source-only operator publication: [submission, isolated builds, review and trust scores](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission.md). Repository administrators assign multiple categories; developers choose a registered runtime.
