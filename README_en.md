# DACP

<img src="assets/dacp-logo.png" alt="DACP logo: connected nodes form the C in Collaborative" width="240">

[中文](README.md)

DACP uses Apache Arrow Flight for data access and collaborative computation across nodes. Servers publish files, SQL tables, and structured-file contents as Datasets and Items. Clients browse data, read record or byte streams, and submit tasks composed of operators. Computation runs near the data and returns live output streams.

For the data model, transport, trust, and collaboration contracts of DACP (Data Access and Collaboration Protocol), see the [protocol overview](docs/protocol_en.md).

[DatasetCatalog plugins](docs/dataset-catalogs_en.md) can supply Dataset identities dynamically; Scala/JVM applications can host nodes through the [DacpServer API](docs/embedded-server_en.md).

## Read by role

| Reader | Entry point | Main topics |
|---|---|---|
| User | [User guide](docs/users/README_en.md) | dacp-shell, Python / Scala SDKs, queries, jobs, result streams, and dashboards |
| Administrator | [Administrator guide](docs/administrators/README_en.md) | Deployment, data publication, certificates and authorization, Union membership, operator repositories, logs, and history |
| Developer | [Developer guide](docs/developers/README_en.md) | Operators, plugin registration, SDK integration, protocols, source builds, and tests |

[Documentation index](docs/README_en.md) · [中文文档](docs/README.md)

## Components and tools

| Component | Purpose |
|---|---|
| `dacp-server` | Publishes data, handles Flight requests, and executes local and distributed tasks |
| `dacp-shell` | Interactive browsing, SQL queries, operator execution, and job-file submission; installed with the Python SDK |
| `dacp-client` / `dacp-client-scala` | Python / Scala applications access data, execute Jobs, and submit Union jobs |
| [dacp-union-server](dacp-union-server/README_en.md) | Manages one Union, invites nodes, polls health and capacity, schedules distributed DAGs, and relays final result streams |
| [Union web dashboard](dacp-union-server/README_en.md#http-operations-dashboard) | Public read-only view of nodes, task scripts, zoomable DAGs, traffic, and execution history |
| [dacp-operator-repository](dacp-operator-repository/README_en.md) | HTTP/HTTPS repository with public search, version browsing, and downloads; administrators publish through the CLI |
| `dacp-operator-runtime` / `plugins/dacp-plugin-operator-executors` | Shared operator environments, JVM / Python / container launchers, and native-command stream bridging |
| `dacp-plugin-api` / `dacp-union-protocol` | Public plugin contracts and shared Union Flight transport utilities |
| [plugins](plugins/README_en.md) | Bundled Dataset, file-format, archive, catalog, and operator launch plugins |

Servers use JDK 17 and Scala 2.13; the Python SDK requires Python 3.10+. Browsing data needs no Docker. Operator execution requires Linux and an accessible Docker Engine. Builds use sbt 1.10.x.

## Get started

- Existing server: start with [connecting and browsing](docs/users/README_en.md#browsing-and-reading).
- Local demonstration: follow [single-node deployment](docs/administrators/README_en.md#single-node-deployment).
- Distributed demonstration: run the [two-node Union example](examples/union/README_en.md).
- Custom computation: read [writing and publishing operators](docs/operator-authoring_en.md).

Data access and task execution use Flight mTLS; the operator repository and read-only dashboard use HTTP/HTTPS. Union CA trust, client allowlists, and node compute approval are configured separately. Each operator runs in its own container. Outputs are not registered as Datasets. Servers persist execution metadata, while result contents are delivered through single-consumption streams.
