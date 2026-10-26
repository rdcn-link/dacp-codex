# Single-Union collaboration protocol

Each Union maintains one persistent identity and reads its independent members from startup configuration. It polls member health and schedules jobs; nodes do not register or send heartbeats.

## Identity and authorization

Ordinary clients use TLS and bearer tokens. Union-to-member calls use mutual TLS plus node-issued service credentials bound to the Union ID and certificate fingerprint. Trusting a CA alone grants no collaboration permissions. Members trust all operations authorized through their Union service credential. Peer data streams are limited to exact execution plans. Local credential revocation or expiry stops active collaborative work. See [authentication and access control](security_en.md) for credential administration and different ordinary-token revocation semantics.

Membership comes only from configuration, without remote invitation or administration APIs. Union identity, member pins, public namespace routes and execution history persist. Unavailable members do not prevent startup and are retried by polling. Restart does not recover running executions; committed materializations support explicit replay.

## Execution and streams

Clients submit standard Jobs with public Union input URIs, optional logical task.node and explicit outputs. Asynchronous submission uses the same public namespace. See [Union proxy service](union-proxy_en.md). X reuses DAG validation and groups tasks into node subjobs. Union accepts versioned operators only, without inline source. Inputs use upstream `task[:port]` references; external Items enter through `read_item@1.0` with `params.uri/view`. Foreign member streams use Flight. Nodes read local Items through DatasetAccess, without downloading through X or Flight. Each output connection has one consumer.

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

U can expose a read-only operations dashboard on a separate HTTP listener without changing the Flight collaboration protocol above. The operations page uses U's cached state without triggering node RPCs or claiming results. The nodes page reads a member's live catalog when its card is expanded. The public read-only dashboard exposes execution metadata across submitters without login or tokens. See the [Union server](../dacp-union-server/README_en.md#execution-and-interfaces) for configuration and read-only HTTP interfaces.

A task `id` is its unique dependency identifier within the Job, used by input `task[:port]` references, final outputs, scheduling, status, and traffic correlation; it is required. Optional `title` describes its purpose, supports Unicode, and must contain 1–256 characters without control characters. Titles need not be unique and do not resolve dependencies. Dashboard cards and DAGs show the title with the ID; execution history retains titles.

```yaml
job: daily-temperature
tasks:
  - id: read_observations
    operator: read_item@1.0
    inputs: {}
    params:
      uri: dacp://localhost:18861/climate/observations
      view: records
  - id: daily_temperature
    title: Aggregate product quantities
    node: dacp://localhost:18861
    operator: daily_temperature_stats@1.0
    inputs:
      source: read_observations
outputs:
  result: daily_temperature
```

The DAG uses locally bundled AntV G6 5.1.1 without an external CDN. Task cards show `title`, falling back to the ID, together with operator, state and input/output payload. Edges show ports and payload. Click a card or edge to inspect the full title, task ID, server address, cross-node payload, rate between observations and record counts where applicable below the graph. Sending and receiving count separately; missing observations and terminal rates display `—`. Final-output edges use source output-port counters, not confirmed client-received bytes.

Color, symbols and borders distinguish steps: solid green ✓ for completed, thick blue ▶ for running, dashed gray ○ for not yet executed, and red ! for failed, cancelled, interrupted or expired. Unknown execution state uses an amber border and `?` label rather than implying the step has not executed. A legend and explicit state text remain visible.

G6 supports dependency and swimlane layouts, each with horizontal (left to right) or vertical (top to bottom) flow. Swimlanes align dependency stages in server rows or columns. Layout controls are independent; refresh preserves both choices. The default combined layout is: both server groups and their internal tasks are automatically arranged left to right by dependency. Each server region has a contrasting background color and border and a heading with its address and task count. Local inputs and tasks stay within the server region, cross-region edges represent data flow, and final outputs occupy a separate Union → client region. Double-click group backgrounds to collapse or expand them; “Expand all” restores the complete view.

The toolbar offers zoom in/out, fit canvas and 100% display, with 15%–250% zoom. Drag to pan and scroll to zoom around the pointer. State refreshes update content while preserving layout, zoom, pan and collapsed groups. Switching jobs lays out and fits the new graph. Canvas information is also available in the task and port tables below for reading and assistive technology access.

The website separates the operations dashboard `/`, graphical member nodes `/nodes`, and data catalog `/datasets`. Node cards load local dataset names inline; the catalog merges live member directories using Union-assigned public names and Union URIs, with explicit incomplete-directory warnings. Only name mappings are persisted, not catalog lists. Policies and attempts are separate; expandable attempt timelines label missing historical information explicitly.

The nodes page uses a dark topology view with health, capacity and directed data flows. Traffic counts receiver-side `remote_payload_bytes` once, never adding send and receive totals. Its scope is inter-member task edges from the latest attempt of each of the most recent 100 jobs, including persisted history. It excludes protocol overhead, other applications and final output returned to clients. Rates require successive fresh observations for the same attempt and input port; retry resets, stale samples or missing observations show unknown. Transfer animation appears only for observed positive rates. Clicking a topology node expands its local datasets below without consuming result streams.

Node cards show environment CPU, DACP process CPU, environment memory, JVM heap, available processors and thread count. Readings come from JVM management APIs and may reflect container limits. Environment memory is not DACP-exclusive; heap is not total process memory. Unsupported metrics show unknown. Participation counts deduplicate jobs with observed member execution states in this Union’s retained history; steps and retries do not add counts. The topology counts task data payload only, excluding heartbeats, status queries, catalog access and Flight message envelopes. Dashed Union membership links indicate membership rather than data transfer and display no message byte counts.

Node topology edge labels display cumulative traffic and observed rates directly; unknown rates show `—`. Hover over an edge or label for endpoints and task payload statistics. Traffic is not duplicated in a separate list below the topology.

The topology uses an orbital background, distinct member card colors, resource utilization bars and directed links. Only links with a positive observed rate animate; the system reduced-motion preference disables animation.

Web interfaces use English throughout the Union job, node and data catalog pages and the DACP node dashboard. User-provided task titles, scripts, dataset names and descriptions are displayed in their original language.

Click a task, input/output node, edge or server group in the job DAG to highlight the selection and show its details. Click empty canvas space to clear it. Automatic updates and layout changes preserve selections that still exist; changing jobs clears the selection. Node dashboards share this interaction.

### Shared execution

Nodes plan shared execution from runtime compatibility and connected ports, without invoking structure inference or reading input contents. Actual stream headers supply metadata during execution.
