# DACP documentation

[中文](README_zh.md) · [Project home](../README.md)

For job format and stream dependencies, see [Jobs](jobs.md).

Protocol overview: [DACP (Data Access and Collaboration Protocol)](protocol.md).

Dynamic catalogs and application embedding: [DataCatalog](dataset-catalogs.md), [embedded server](embedded-server.md).

The [collaboration operator catalog](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-catalog.md) documents the configured collaboration operators, ports, parameters, input/output and Shell / Job examples.

## Read by role

| Entry point | Audience | Reading path |
|---|---|---|
| [User guide](users/README.md) | Data consumers, analysts, and application users of deployed services | Connect → browse and query → execute jobs → dashboard and history |
| [Administrator guide](administrators/README.md) | Operators deploying nodes, Union servers, or repositories | Environment and certificates → data publication → operators and authorization → monitoring, storage, and shutdown |
| [Developer guide](developers/README.md) | Operator authors, application integrators, plugin and protocol developers | Module architecture → extension contracts → implementation and registration → tests |

## Tool and module manuals

| Tool | Documentation |
|---|---|
| dacp-shell, Python SDK | [User guide](users/README.md), [query semantics](records-scans.md) |
| DACP proxy server | [Single-upstream TCP relay](../dacp-proxy-server/README.md) |
| Scala SDK | [Scala client](../dacp-client-scala/README.md) |
| DACP Server | [Administrator guide](administrators/README.md), [configuration and protocol reference](reference.md) |
| Union Server and web dashboard | [Union manual](../dacp-union-server/README.md), [collaboration protocol](union.md), [two-node example](../examples/union/README.md) |
| Operator repository CLI and website | [Repository manual](https://github.com/rdcn-link/xstream-operator-repository/blob/main/README.md) |
| Plugin API and Union transport utilities | [Plugin API](../dacp-plugin-api/README.md), [Union protocol module](../dacp-union-protocol/README.md) |

## Topic references

| Topic | Documentation |
|---|---|
| Writing, publishing, and example code | [Scala / Java / Python operators](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md), [native commands](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands.md) |
| DEBUG source submission | [Source runtimes and heredocs](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-source.md) |
| Runtimes, artifacts, caches, and frame protocol | [Operator packaging](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging.md), [stream contract](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams.md) |
| Source, licenses, build instructions, and SBOM | [Operator review material](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-provenance.md) |
| Execution records, history queries, and restart semantics | [Execution history](execution-history.md) |
| Plugin discovery and contribution requirements | [Plugin architecture](plugins.md) |
| Dataset backends, file formats, and structured browsing | [Dataset providers](dataset-providers.md), [format handlers](format-handlers.md) |
| Filters, projections, SQL shell, and optimization contracts | [Records queries](records-scans.md) |
| Data model, read capabilities, and node settings | [Technical reference](reference.md) |

## Runnable examples

- [Jobs between ordinary nodes](../examples/multi-node/README.md) and [Union analysis across centers](../examples/union/README.md).
- [Scala operators](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/scala/README.md), [Python operators](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/python/README.md), [native executable operators](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/native/README.md).
- [Plugin project](../examples/plugins/README.md) and [SQL publication](../examples/sql/README.md).

Commands run from the project root unless a document specifies otherwise. Relative paths in service configurations resolve against the service process working directory. Example certificates, hosts, ports, and paths are for local demonstrations; configure deployment values for your environment. Documentation defaults to English in `README.md` and unsuffixed topic files; complete Chinese translations use `README_zh.md` and `*_zh.md`.

[User access logs](access-logs.md): JSON Lines files, size rotation, fields and collection.

[Dataset directories](dataset-directory.md): public node descriptions, per-member browsing on the Union website and shell `datasets`.
[Shared-process measurements](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion-benchmark.md): strategy comparisons with one million and four million rows.

[Node web dashboard](node-web.md): default HTTP service, configuration, and G6 execution DAGs.

Official website builds, deployment and software download publication: [dacp-website](https://github.com/rdcn-link/dacp-website). Website content is English-only.

[S3-compatible reads](s3-service.md): dataset buckets and Item objects exposed at node startup, with SigV4-authenticated read-only downloads.

Union provides standard DACP discovery, Items, Jobs and S3. For public short names, placement and management access, see [Union proxy service](union-proxy.md).

[Authentication and access control](security.md): Flight tokens, Union service credentials, S3 SigV4, revocation and audit. [Structure inference](https://github.com/rdcn-link/xstream-codex/blob/main/docs/structure-inference.md): design-time operator output structures and bounded samples; execution does not call schema inference.

[Streaming versus per-step materialization benchmark](materialization-benchmark.md): 500,000 weather observations, three measured runs and actual persisted bytes.

[Measured local versus cross-node streaming job performance](local-vs-union-benchmark.md).

Project presentation material: [English meeting poster and A4 PDF](../assets/posters/README.md).

[Online Job Designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README.md): design local and cross-node jobs in the browser and export YAML for Shell / SDK submission.

- [Operator parameter types and designer forms](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-parameters.md)

Source-only operator publication: [submission, isolated builds, review and trust scores](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission.md). Repository administrators assign multiple categories; developers choose a registered runtime.

Project boundaries: [DACP services powered by XStream](ecosystem.md). The visual designer, SDK/engine and operator repository are independently maintained projects.
