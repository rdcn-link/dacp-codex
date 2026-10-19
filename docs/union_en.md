# Single-Union collaboration protocol

Each `dacp-union-server` (X) manages one persistent Union. Independent DACP nodes respond to X's invitation and participate only after local administrator approval. X actively polls members; nodes neither register nor send heartbeats. See the module README for supported commands and implementation limits.

## Identity and authorization

Union uses Arrow Flight mTLS throughout. Nodes reuse their existing Flight data ports; X has its own Flight listener. The operator repository retains HTTP. Identity is the SHA-256 fingerprint of the TLS-verified leaf certificate DER, never an identity claimed in a request. Certificate rotation requires invitation and approval again. X's client identity allowlist limits job submission; jobs and results belong to their submitter. Node approval binds the Union ID and X's identity to compute input prefixes, exact operators, concurrency, and expiry. This does not grant original-data download permission. Nodes can revoke approval, cancelling affected executions. Approval uses local administration rather than a remote approval endpoint.

Invitations, approvals, rejection, withdrawal, and removal persist locally; individual executions are transient sessions with separately persisted execution metadata. Poll failures produce unknown status without automatic member removal. Restarting X or a node does not recover execution; membership persists and previous outputs cannot be replayed.

## Execution and streams

Clients submit Jobs with explicit task.node and outputs. X reuses DAG validation and groups tasks into node subjobs. The first implementation accepts versioned operators only, without inline source. Inputs reference local data Items (uri/view) or upstream task/port pairs. Nodes read local inputs directly through DatasetAccess, without downloading through X or Flight. Each output connection has one consumer.

X prepares every subjob before starting any. Nodes check capacity and authorization and reject plans they cannot execute. A's output binds the execution/task/port to B's certificate identity; B pulls it over mTLS. Local connections use bounded queues; cross-node streams use Flight `DoGet` with backpressure. OperatorFrames are reused: start/data/end is followed by complete only after successful execution; failures terminate with a Flight error. After complete, consumers must still verify successful Flight end-of-stream. Outputs create no Dataset, persist no files, and cannot be replayed.

Final results travel from a node through X's bounded queue to the client. Status is independent and does not wait for a slow result reader. Connections, execution, and waiting for results have timeouts. Explicit cancellation, execution failure, or interruption of final result consumption cancels the entire Job. Clients can connect to results after submission but before timeout; disconnecting a status subscription alone does not cancel execution.

## Observability

X periodically polls member capacity and health. During execution it queries subjob status and produces Flight events. Events have monotonically increasing sequence numbers and a bounded in-memory window. A stale cursor receives an explicit gap plus current snapshot; snapshots are also available from the status endpoint. Events include Task states and per-port cumulative payload_bytes, records where applicable, remote_payload_bytes, and last_activity_ms. Byte counters measure data frame payloads, excluding TLS/gRPC overhead. Sending and receiving are counted separately and must not be summed as unique traffic. Unknown totals do not produce percentages; rates can be calculated from successive counters.

The implementation does not promise automatic retries, persisted job recovery, checkpoints, output replay, fan-out, arbitrary scheduling optimization, scientific array representations, or cross-node snapshot consistency.

## Administration website

U can expose a read-only operations dashboard on a separate HTTP listener without changing the Flight collaboration protocol above. The website uses U's cached state; observation neither triggers node RPCs nor claims results. The public read-only view exposes execution metadata across submitters without an access token; configure the listener address for remote viewing. See the [Union server](../dacp-union-server/README_en.md#http-operations-dashboard) for configuration and read-only HTTP interfaces.

A task `id` is its unique dependency identifier within the Job, used by input `task` references, final outputs, scheduling, status, and traffic correlation; it is required. Optional `title` describes its purpose, supports Unicode, and must contain 1–256 characters without control characters. Titles need not be unique and do not resolve dependencies. Dashboard cards and DAGs show the title with the ID; execution history retains titles.

```yaml
tasks:
  - id: sum_sales
    title: Aggregate product quantities
    node: dacp://localhost:18861
    operator: quantity_sum@1.0
    inputs:
      source: {uri: "dacp://localhost:18861/data/sales", view: bytes}
```

DAG task nodes show `title`, falling back to the ID; hover for the task ID. Nodes include state, execution node, input/output payload and cross-node payload. Edges show ports, payload, rate between observations, and record counts where applicable. Sending and receiving count separately; missing observations and terminal rates display `—`. Final-output edges use source output-port counters, not confirmed client-received bytes.

The DAG distinguishes steps using color, symbols, and borders: solid green ✓ for completed, thick blue ▶ for running, dashed gray ○ for not yet executed, and red ! for failed, cancelled, interrupted, or expired. A legend and explicit state text remain visible.

Unknown execution state uses an amber border and `?` label rather than implying the step has not executed.

The DAG groups execution nodes into separate regions labelled with server addresses and task counts. Local inputs and tasks appear inside their server region; cross-region edges show data movement. Final outputs appear in a separate Union → client region.

The layout uses vertical server swimlanes with node addresses at the top. Dependency levels flow downward, and cross-server edges point downward across lanes. Parallel steps stay in their server lane; edge labels show ports and traffic.

The DAG toolbar offers zoom in/out, fit width, and 100% display, with 25%–300% zoom. Drag to pan or hold Ctrl / Command while scrolling to zoom around the pointer; touch devices use native scrolling. Automatic refresh preserves the current job’s zoom and pan, while changing jobs restores fit width.
