# DACP protocol

[中文](protocol.md) · [Documentation](README_en.md)

DACP (Data Access and Collaboration Protocol) defines data access and computation collaboration between clients, data nodes, and coordination nodes. This implementation builds on Apache Arrow Flight: Flight supplies transport, while DACP specifies naming, metadata, read semantics, operator tasks, identity, authorization, and collaboration. DACP is this project's protocol design, rather than a separate Apache Arrow standard.

## Data and computation model

Servers organize data into Datasets and describe accessible objects as Items. Clients inspect metadata and advertised capabilities before selecting a view. `bytes` exposes byte content; `records` exposes batches with an Arrow schema. Format and view describe storage representation and access capability separately; clients must not infer supported reads solely from filename extensions. Plugins should explicitly describe scientific groups, tables, and multidimensional variables rather than silently flattening them into records.

A job contains tasks. A task's `id` identifies dependency references; its `title` describes its purpose to users. Operators consume and produce streams through named ports. Inputs may reference data items or upstream outputs. Results are not automatically published as Datasets. Ordinary node jobs and Union jobs have separate submission interfaces and execution constraints; their scripts are not interchangeable.

## Transport responsibilities

| Purpose | Current transport |
|---|---|
| Dataset discovery | Flight `ListFlights` returns public Dataset URIs and descriptions without additional data grants; plugins can supply a dynamic catalog |
| Browsing, metadata, and reading | Arrow Flight; see the technical reference and protobuf definitions for messages |
| Ordinary node job execution | Flight `DoExchange` carrying control messages and result streams |
| Union invitations, approval, members, status, and job control | Flight `DoAction` with the `dacp.union.` action prefix and JSON messages |
| Union final results and inter-node stream reads | Flight `DoGet` with the corresponding tickets |
| Operator repository search, versions, and artifact downloads | HTTP/HTTPS |
| Public read-only Union dashboard | HTTP for status and history; it does not authorize computation submission |

The shared Union transport module limits JSON messages to 1 MiB. For exact actions, tickets, and fields, see the [Union protocol](union_en.md), [technical reference](reference_en.md), `proto/dacp.proto`, and `dacp-union-protocol`. The operator process port framing protocol is an executor interface, described in the [operator stream contract](operator-streams_en.md), rather than the client Flight interface.

## Identity, trust, and authorization

Flight uses mTLS. Clients verify the server's CA chain and connection identity; servers verify client certificates. A server calling another node also acts as a client. A valid certificate establishes verifiable identity, not automatic permission to read data or execute tasks.

Union identifies callers using the SHA-256 fingerprint of the TLS-verified leaf certificate and can pin remote certificates. Access to Union follows its authorization configuration. Each node independently approves Union invitations and local computation permissions, which can restrict data scope, operators, and resources. Self-declared identity in a JSON request cannot override certificate identity or authorization.

## Cross-center collaboration

One Union Server maintains one Union. Data nodes run independently. Union invites nodes, which join after approval. Union actively polls their status and schedules steps according to declared dependencies.

For example, a task on A produces `oa`, and B combines `oa` with local data to produce `ob`. Union schedules A and B; the intermediate stream travels directly between nodes, and Union relays the final `ob` stream to the client. The client need not connect directly to B. Participating nodes still require certificate verification and the corresponding business authorization.

The implementation supports explicitly declared collaboration DAGs. It does not promise arbitrary general pipeline optimization, result replay, or recovery of interrupted streams. The dashboard and execution records expose status, traffic, and task titles.

## Streaming and persistence

Callers should consume results incrementally and close streams promptly. Cancellation, timeout, or downstream failure must release upstream readers and execution resources. Receiving partial data does not establish job success; clients must also check the final stream status.

Nodes and Union persist execution metadata; Union also records job scripts and step states. Result contents are delivered as live streams rather than historical output files. Execution records cannot restore result streams after restart. See [execution history](execution-history_en.md) for storage, queries, and restart behavior.

## Implementation and extensions

Core owns public contracts, transport, authorization, namespaces, and resource management. Plugins extend data backends, formats, structured browsing, and executors. Changes to representations or wire contracts must update shared interfaces, servers, Python/Scala clients, tests, and bilingual documentation together. See the [plugin architecture](plugins_en.md) for implemented and planned extension boundaries.
