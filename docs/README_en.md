# DACP documentation

[中文](README.md) · [Project home](../README_en.md)

For job format, stream dependencies and control dependencies, see [Jobs](jobs_en.md).

Protocol overview: [DACP (Data Access and Collaboration Protocol)](protocol_en.md).

Dynamic catalogs and application embedding: [DatasetCatalog](dataset-catalogs_en.md), [embedded server](embedded-server_en.md).

## Read by role

| Entry point | Audience | Reading path |
|---|---|---|
| [User guide](users/README_en.md) | Data consumers, analysts, and application users of deployed services | Connect → browse and query → execute jobs → dashboard and history |
| [Administrator guide](administrators/README_en.md) | Operators deploying nodes, Union servers, or repositories | Environment and certificates → data publication → operators and authorization → monitoring, storage, and shutdown |
| [Developer guide](developers/README_en.md) | Operator authors, application integrators, plugin and protocol developers | Module architecture → extension contracts → implementation and registration → tests |

## Tool and module manuals

| Tool | Documentation |
|---|---|
| dacp-shell, Python SDK | [User guide](users/README_en.md), [query semantics](records-scans_en.md) |
| Scala SDK | [Scala client](../dacp-client-scala/README_en.md) |
| DACP Server | [Administrator guide](administrators/README_en.md), [configuration and protocol reference](reference_en.md) |
| Union Server and web dashboard | [Union manual](../dacp-union-server/README_en.md), [collaboration protocol](union_en.md), [two-node example](../examples/union/README_en.md) |
| Operator repository CLI and website | [Repository manual](../dacp-operator-repository/README_en.md) |
| Operator runtime and executors | [Runtime](../dacp-operator-runtime/README_en.md), [executors](../plugins/dacp-plugin-operator-executors/README_en.md) |
| Plugin API and Union transport utilities | [Plugin API](../dacp-plugin-api/README_en.md), [Union protocol module](../dacp-union-protocol/README_en.md) |

## Topic references

| Topic | Documentation |
|---|---|
| Writing, publishing, and example code | [Scala / Java / Python operators](operator-authoring_en.md), [native commands](native-commands_en.md) |
| DEBUG source submission | [Source runtimes and heredocs](operator-source_en.md) |
| Runtimes, artifacts, caches, and frame protocol | [Operator packaging](operator-packaging_en.md), [stream contract](operator-streams_en.md) |
| Source, licenses, build instructions, and SBOM | [Operator review material](operator-provenance_en.md) |
| Execution records, history queries, and restart semantics | [Execution history](execution-history_en.md) |
| Plugin discovery and contribution requirements | [Plugin architecture](plugins_en.md) |
| Dataset backends, file formats, and structured browsing | [Dataset providers](dataset-providers_en.md), [format handlers](format-handlers_en.md) |
| Filters, projections, SQL shell, and optimization contracts | [Records queries](records-scans_en.md) |
| Data model, read capabilities, and node settings | [Technical reference](reference_en.md) |

## Runnable examples

- [Jobs between ordinary nodes](../examples/multi-node/README_en.md) and [Union analysis across centers](../examples/union/README_en.md).
- [Scala operators](../examples/operators/scala/README_en.md), [Python operators](../examples/operators/python/README_en.md), [native executable operators](../examples/operators/native/README_en.md).
- [Plugin project](../examples/plugins/README_en.md) and [SQL publication](../examples/sql/README_en.md).

Commands run from the project root unless a document specifies otherwise. Relative paths in service configurations resolve against the service process working directory. Example certificates, hosts, ports, and paths are for local demonstrations; configure deployment values for your environment. Each `README.md` is Chinese, with its English translation in `README_en.md`.

[User access logs](access-logs_en.md): JSON Lines files, size rotation, fields and collection.

[Dataset directories](dataset-directory_en.md): public node descriptions, per-member browsing on the Union website and shell `datasets`.
[Shared-process measurements](operator-fusion-benchmark_en.md): strategy comparisons with one million and four million rows.

[Node web dashboard](node-web_en.md): default HTTP service, configuration, and G6 execution DAGs.

Official website builds, deployment and software download publication: [dacp-website](../dacp-website/README_en.md). Website content is English-only.

[S3-compatible reads](s3-service_en.md): dataset buckets and Item objects exposed at node startup, with anonymous read-only downloads.

Union provides standard DACP discovery, Items, Jobs and S3. For public short names, placement and management access, see [Union proxy service](union-proxy_en.md).
