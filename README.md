# DACP

<img src="assets/dacp-logo.png" alt="DACP logo: connected nodes form the C in Collaborative" width="240">

[中文](README_zh.md)

DACP provides data access and collaborative computation across nodes. Servers publish files, SQL tables, and structured-file contents as Datasets and Items. Clients browse data, read record or byte streams, and submit tasks composed of operators. Computation runs near the data and returns live output streams.

DACP is powered by the embedded **XStream streaming engine**: typed data views, stream-connected operators, cancellation and execution policies are shared with standalone XStream. DACP adds authenticated data services and Union planning across independent nodes. See [project responsibilities](docs/ecosystem.md).

For the data model, transport, trust, and collaboration contracts of DACP (Data Access and Collaboration Protocol), see the [protocol overview](docs/protocol.md).

[DataCatalog plugins](docs/dataset-catalogs.md) can supply Dataset identities dynamically; Scala/JVM applications can host nodes through the [DacpServer API](docs/embedded-server.md).

Nodes also expose an [S3-compatible read-only service](docs/s3-service.md) by default, allowing S3 clients to list dataset buckets and download Item byte streams.

Independent official website project: [dacp-website](https://github.com/rdcn-link/dacp-website), providing an English project overview, online documentation by role, and software downloads.

Job definitions, node subjobs, execution policies and variable files use YAML. Every Job requires a readable `name`, separate from its system-generated execution ID. Node and Union dashboards share job names and execution fields. In the shell, submit them together with `submit workflow.yaml --policy policy.yaml --vars variables.yaml`. See [writing jobs](docs/jobs.md).

## Read by role

| Reader | Entry point | Main topics |
|---|---|---|
| User | [User guide](docs/users/README.md) | dacp-shell, Python / Scala SDKs, queries, jobs, result streams, and dashboards |
| Administrator | [Administrator guide](docs/administrators/README.md) | Deployment, data publication, certificates and authorization, Union membership, operator repositories, logs, and history |
| Developer | [Developer guide](docs/developers/README.md) | Operators, plugin registration, SDK integration, protocols, source builds, and tests |

[Documentation index](docs/README.md) · [中文文档](docs/README.md)

## Components and tools

| Component | Purpose |
|---|---|
| `dacp-server` | Publishes data, handles data access requests, and executes local and distributed tasks |
| [dacp-proxy-server](dacp-proxy-server/README.md) | Expose one private DACP service through a relay preserving end-to-end TLS and authentication |
| `dacp-shell` | Interactive browsing, SQL queries, operator execution, and job-file submission; installed with the Python SDK |
| `dacp-client` / `dacp-client-scala` | Python / Scala applications access data, execute Jobs, and submit Union jobs |
| [dacp-union-server](dacp-union-server/README.md) | Manages one Union, connects configured nodes, polls health and capacity, schedules distributed DAGs, and relays final result streams |
| [Union web dashboard](dacp-union-server/README.md#execution-and-interfaces) | Management view of nodes, task scripts, zoomable DAGs, traffic, and execution history |
| `dacp-plugin-api` / `dacp-union-protocol` | Public plugin contracts and shared Union transport utilities |
| [plugins](plugins/README.md) | Bundled Dataset, file-format, archive, catalog, and operator launch plugins |

Servers use JDK 17 and Scala 2.13; the Python SDK requires Python 3.10+. Browsing data needs no Docker. Operator execution requires Linux and an accessible Docker Engine. Builds use sbt 1.10.x.

## Get started

- Existing server: start with [connecting and browsing](docs/users/README.md#browsing-and-reading).
- Local demonstration: follow [single-node deployment](docs/administrators/README.md#single-node-deployment).
- Distributed demonstration: run the [two-node Union example](examples/union/README.md).
- Custom computation: read [writing and publishing operators](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md).

Ordinary data access and execution use TLS and tokens; Union collaboration uses mutual TLS and node-issued service credentials. S3 uses independent SigV4 authentication. Repository downloads are public, publication requires a token, and internal dashboards are publicly read-only. See [authentication and access control](docs/security.md). Operators use isolated runtime environments; eligible node subjobs share one process. See [in-process operator fusion](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion.md). Outputs are not registered as Datasets. Servers persist execution metadata, while result contents are delivered through single-consumption streams. Selected boundaries may be materialized and replayed through explicit execution policies.

[Online Job Designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README.md): drag-and-drop composition, service-oriented catalogs, live operator repositories, schema previews, variables, chat proposals and YAML import / export.

Source-only operator publication: [submission, isolated builds, review and trust scores](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission.md). Repository administrators assign multiple categories; developers choose a registered runtime.

The independent [XStream engine](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-engine/README.md) provides the `xstream-runner` local launcher and is embedded by DACP Server. [XStream Designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README.md) browses local and configured remote catalogs lazily.

[xstream-data: typed data streams and scientific format plugins](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README.md).

## XStream integration

XStream data, engine and designer live in the separate [xstream-codex repository](https://github.com/rdcn-link/xstream-codex). DACP depends on versioned XStream artifacts rather than sibling source directories. For an unreleased development checkout, initialize local artifacts using `scripts/bootstrap-local.sh /path/to/dacp-codex` from the XStream checkout before building DACP. See [build boundaries](https://github.com/rdcn-link/xstream-codex/blob/main/docs/repositories.md).

## Independent related projects

| Project | Integration |
|---|---|
| [xstream-engine](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-engine/README.md) | Standalone and embeddable streaming Job engine, including the xstream-runner program |
| [xstream-designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README.md) | Browser visual editing, catalog browsing, schema previews and YAML import / export |
| [xstream-operator-repository](https://github.com/rdcn-link/xstream-operator-repository/blob/main/README.md) | HTTP/HTTPS repository with public search, version browsing, and downloads; source-only submissions are built and published after administrator review |
| [xstream-operator-sdk](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-operator-sdk/README.md) | Lightweight metadata contracts (`contract`), JVM templates (`jvm`) and Python templates (`python`) |

Documentation ownership and website publishing: [maintenance policy](docs/documentation.md).
