# DACP protocol

[中文](protocol_zh.md) · [Documentation](README.md)

DACP (Data Access and Collaboration Protocol) defines data access and computation collaboration between clients, data nodes, and coordination nodes. This implementation builds on Apache Arrow Flight: Flight supplies transport, while DACP specifies naming, metadata, read semantics, operator tasks, identity, authorization, and collaboration. DACP is this project's protocol design, rather than a separate Apache Arrow standard.

## Data and computation model

Servers organize data into Datasets and describe accessible objects as Items. Clients inspect metadata and advertised capabilities before selecting a view. `bytes` exposes byte content; `records` exposes batches with an Arrow schema. Format and view describe storage representation and access capability separately; clients must not infer supported reads solely from filename extensions. Plugins should explicitly describe scientific groups, tables, and multidimensional variables rather than silently flattening them into records.

A job contains tasks. A task's `id` identifies dependency references; its `title` describes its purpose to users. Operators consume and produce streams through named ports. Inputs reference only upstream outputs; read_item source operators read external Items. Results are not automatically published as Datasets. Nodes and Union both accept standard Job submission. Union uses its public URIs and optional logical node IDs, and retains dedicated management APIs; see [Union proxy service](union-proxy.md).

## Transport responsibilities

| Purpose | Current transport |
|---|---|
| Dataset discovery | Flight `dacp.catalog` returns completeness, public Dataset URIs and descriptions without additional data grants; plugins can supply a dynamic catalog |
| Browsing, metadata, and reading | Arrow Flight; see the technical reference and protobuf definitions for messages |
| S3-compatible object reads | Separate HTTPS endpoint, SigV4-authenticated and read-only; datasets map to buckets and bytes Items to objects; see the [S3 service](s3-service.md) |
| Ordinary node job execution | Flight `DoExchange` carrying control messages and result streams |
| Union membership validation, status and job control | Flight `DoAction` with the `dacp.union.` action prefix and JSON messages |
| Union final results and inter-node stream reads | Flight `DoGet` with the corresponding tickets |
| Operator repository search, versions, and artifact downloads | HTTP/HTTPS |
| Read-only Union management dashboard | HTTP for status and history; it does not authorize computation submission |

The shared Union transport module limits JSON messages to 1 MiB. For exact actions, tickets, and fields, see the [Union protocol](union.md), [technical reference](reference.md), `proto/dacp.proto`, and `dacp-union-protocol`. The operator process port framing protocol is an executor interface, described in the [operator stream contract](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams.md), rather than the client Flight interface.

## Identity, trust, and authorization

Ordinary Flight clients use TLS and bearer tokens. Union members use mutual TLS and node-issued service credentials. Peer streams are authorized by exact execution plans, not merely CA trust. S3 has independent SigV4 keys. Servers enforce authorization regardless of SDK type. See [security](security.md).

## Cross-center collaboration

One Union Server maintains one Union. Data nodes run independently. Union connects members configured with node-issued credentials. Union actively polls their status and schedules steps according to declared dependencies.

For example, a task on A produces `oa`, and B combines `oa` with local data to produce `ob`. Union schedules A and B; the intermediate stream travels directly between nodes, and Union relays the final `ob` stream to the client. The client need not connect directly to B. Participating nodes still require certificate verification and the corresponding business authorization.

The implementation supports explicitly declared collaboration DAGs. It supports configured materialization boundaries and replay; automatic recovery requires an execution policy and retry-aware clients. It does not promise arbitrary general pipeline optimization. The dashboard and execution records expose status, traffic, and task titles.

## Streaming and persistence

Callers should consume results incrementally and close streams promptly. Cancellation, timeout, or downstream failure must release upstream readers and execution resources. Receiving partial data does not establish job success; clients must also check the final stream status.

Nodes and Union persist execution metadata; Union also records job scripts and step states. Result contents are delivered as live streams rather than historical output files. Execution records cannot restore result streams after restart. See [execution history](execution-history.md) for storage, queries, and restart behavior.

## Implementation and extensions

Core owns public contracts, transport, authorization, namespaces, and resource management. Plugins extend data backends, formats, structured browsing, and executors. Changes to representations or wire contracts must update shared interfaces, servers, Python/Scala clients, tests, and bilingual documentation together. See the [plugin architecture](plugins.md) for implemented and planned extension boundaries.

### Single-node job execution policies

Flight `DoExchange` accepts `execution_policy_yaml` (YAML bytes), `retry_aware` (boolean), and `replay_from` (original execution ID) alongside job YAML in `OperatorRequest`. For replay, leave `job_yaml` empty; the server restores the original job and policy and checks the submitting identity and valid materialized outputs. Worker-step requests reject these job-level fields. Result streams begin with `execution` metadata exposing the execution ID. Before retrying, `attempt_failed` lets the client callback clear provisional results before receiving fresh `start/data/end` events. A `complete` event must still be followed by successful Flight end-of-stream. Disconnecting or closing the stream cancels execution. See [execution policies](materialization.md).
