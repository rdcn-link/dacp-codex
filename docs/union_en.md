# Single-Union collaboration protocol

Each `dacp-union-server` (X) manages one persistent Union. Independent DACP nodes respond to X's invitation and participate only after local administrator approval. X actively polls members; nodes neither register nor send heartbeats. See the module README for supported commands and implementation limits.

## Identity and authorization

Union uses Arrow Flight mTLS throughout. Nodes reuse their existing Flight data ports; X has its own Flight listener. The operator repository retains HTTP. Identity is the SHA-256 fingerprint of the TLS-verified leaf certificate DER, never an identity claimed in a request. Certificate rotation requires invitation and approval again. X's client identity allowlist limits job submission; jobs and results belong to their submitter. Node approval binds the Union ID and X's identity to compute input prefixes, exact operators, concurrency, and expiry. This does not grant original-data download permission. Nodes can revoke approval, cancelling affected executions. Approval uses local administration rather than a remote approval endpoint.

Invitations, approvals, rejection, withdrawal, and removal persist locally; individual executions are transient sessions with separately persisted execution metadata. Poll failures produce unknown status without automatic member removal. Restarting X or a node does not recover execution; membership persists and explicitly materialized outputs may be replayed through a new execution.

## Execution and streams

Clients submit standard Jobs with public Union input URIs, optional logical task.node and explicit outputs. Administrators can also use specialized asynchronous submission with internal placements. See [Union proxy service](union-proxy_en.md). X reuses DAG validation and groups tasks into node subjobs. Union accepts versioned operators only, without inline source. Inputs reference data Items (uri/view) or upstream task/port pairs; foreign member inputs stream over Flight. Nodes read local inputs directly through DatasetAccess, without downloading through X or Flight. Each output connection has one consumer.

X prepares every subjob before starting any. Nodes check capacity and authorization and reject plans they cannot execute. A's output binds the execution/task/port to B's certificate identity; B pulls it over mTLS. Local connections use bounded queues; cross-node streams use Flight `DoGet` with backpressure. OperatorFrames are reused: start/data/end is followed by complete only after successful execution; failures terminate with a Flight error. After complete, consumers must still verify successful Flight end-of-stream. Outputs create no Dataset and stream directly by default; explicit policy can materialize selected outputs on the producing node for replay.

Final results travel from a node through X's bounded queue to the client. Status is independent and does not wait for a slow result reader. Connections, execution, and waiting for results have timeouts. Explicit cancellation, execution failure, or interruption of final result consumption cancels the entire Job. Clients can connect to results after submission but before timeout; disconnecting a status subscription alone does not cancel execution.

## Observability

X periodically polls member capacity and health. During execution it queries subjob status and produces Flight events. Events have monotonically increasing sequence numbers and a bounded in-memory window. A stale cursor receives an explicit gap plus current snapshot; snapshots are also available from the status endpoint. Events include Task states and per-port cumulative payload_bytes, records where applicable, remote_payload_bytes, and last_activity_ms. Byte counters measure data frame payloads, excluding TLS/gRPC overhead. Sending and receiving are counted separately and must not be summed as unique traffic. Unknown totals do not produce percentages; rates can be calculated from successive counters.

The implementation does not promise automatic running-job recovery, internal checkpoints, fan-out, arbitrary scheduling optimization, scientific array representations, or cross-node snapshot consistency.

## Execution policy, output materialization, and replay

Union accepts a separate `execution_policy` JSON alongside `job_yaml`. Streaming remains the default. Selected connected outputs are saved on their producing node through `MaterializationStoreProvider`; all selected ports commit together after the task succeeds and inputs are verified. `materialization.allowed: false` prohibits engine output materialization and rejects conflicting defaults or task overrides.

The owner can explicitly request `replay_job` after the source execution terminates. This creates a new execution, reuses complete, unexpired selected outputs, prunes unnecessary upstream work, and reruns downstream tasks. Final outputs can also be replayed. Original and effective policies, source links, and replay state are retained in history. Nodes must enable a storage plugin and revalidate authorization. Read [execution policy, storage configuration, client examples, and plugin contracts](materialization_en.md).

This is task-boundary replay, not internal operator checkpoints, byte-offset resume, or restoration of running processes. Expired, incomplete, or corrupt artifacts cannot successfully replay. Standard `Job.run` is also supported at Union, sharing its scheduling and execution policy engine.

Top-level `max_retries` (0–10, default 0) bounds execution retries. Results remain incremental; consumers must provide a retry callback to discard prior partial output. Materialization is optional. `timeout_seconds` covers resource waits and all attempts and cannot exceed the server limit. See [execution policy](materialization_en.md).

## Administration website

U can expose a read-only operations dashboard on a separate HTTP listener without changing the Flight collaboration protocol above. The website uses U's cached state; observation neither triggers node RPCs nor claims results. Management views expose execution metadata across submitters. Loopback access is local; external binding requires a token file and login or Bearer authentication. See the [Union server](../dacp-union-server/README_en.md#management-api-and-dashboard) for configuration and read-only HTTP interfaces.

A task `id` is its unique dependency identifier within the Job, used by input `task` references, final outputs, scheduling, status, and traffic correlation; it is required. Optional `title` describes its purpose, supports Unicode, and must contain 1–256 characters without control characters. Titles need not be unique and do not resolve dependencies. Dashboard cards and DAGs show the title with the ID; execution history retains titles.

```yaml
tasks:
  - id: daily_temperature
    title: Aggregate product quantities
    node: dacp://localhost:18861
    operator: daily_temperature_stats@1.0
    inputs:
      source: {uri: "dacp://localhost:18861/climate/observations", view: records}
```

The DAG uses locally bundled AntV G6 5.1.1 without an external CDN. Task cards show `title`, falling back to the ID, together with operator, state and input/output payload. Edges show ports and payload. Click a card or edge to inspect the full title, task ID, server address, cross-node payload, rate between observations and record counts where applicable below the graph. Sending and receiving count separately; missing observations and terminal rates display `—`. Final-output edges use source output-port counters, not confirmed client-received bytes.

Color, symbols and borders distinguish steps: solid green ✓ for completed, thick blue ▶ for running, dashed gray ○ for not yet executed, and red ! for failed, cancelled, interrupted or expired. Unknown execution state uses an amber border and `?` label rather than implying the step has not executed. A legend and explicit state text remain visible.

G6 supports dependency and swimlane layouts, each with horizontal (left to right) or vertical (top to bottom) flow. Swimlanes align dependency stages in server rows or columns. Layout controls are independent; refresh preserves both choices. The default combined layout is: both server groups and their internal tasks are automatically arranged left to right by dependency. Each server region has a contrasting background color and border and a heading with its address and task count. Local inputs and tasks stay within the server region, cross-region edges represent data flow, and final outputs occupy a separate Union → client region. Double-click group backgrounds to collapse or expand them; “Expand all” restores the complete view.

The toolbar offers zoom in/out, fit canvas and 100% display, with 15%–250% zoom. Drag to pan and scroll to zoom around the pointer. State refreshes update content while preserving layout, zoom, pan and collapsed groups. Switching jobs lays out and fits the new graph. Canvas information is also available in the task and port tables below for reading and assistive technology access.
