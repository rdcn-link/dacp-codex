# Physical plan generation and optimization

Union chooses where a submitted Job executes. XStream runs each member's local sub-job. Open **Physical plan → Complete plan** in the Union dashboard to inspect the planning summary, estimated transfers, runtime candidates and placement reasons. Execution information shows the units actually launched. Designer edits Jobs; it does not select distributed execution plans.

```mermaid
flowchart LR
    J[Submitted logical plan] --> N[Resolve member namespaces]
    N --> M[Collect bounded metadata]
    M --> C[Expand candidate placements]
    C --> V[Check hard constraints]
    V --> E[Estimate whole-plan cost]
    E --> B[Keep best bounded candidates]
    B --> P[Select physical plan]
    P --> R[Member validates and launches units]
```

## Planning snapshot

The snapshot contains approved members, capacity, installed executor IDs, reported memory, reserved-step count, Item size where available, and installed operator runtime/dependency metadata. Both Union-context Items and full member DACP URIs contribute source sizes. A read is pinned to its data owner when that owner is an approved member. An external URI remains a remote read; the external server does not automatically become an execution member.

Metadata requests use existing authenticated member connections, certificate pins and bounded RPC timeouts. Installed operator profiles read local manifests/cache only: they do not invoke operator code, download artifacts or load runtime images. Missing profiles do not claim runtime compatibility. Missing sizes use conservative estimates and produce planning warnings. Metadata can require a format's normal bounded schema inference, but planning does not request row counts or scan entire inputs. No authorization is granted by a size or profile lookup; dataset publication checks and member admission still apply before execution.

## Candidate selection

The planner expands legal targets in dependency order and retains at most `beam_width` partial candidates after each step. This is bounded beam search, not exhaustive enumeration or a guarantee of a globally optimal plan. Each candidate is evaluated as a graph, so later joins and pinned downstream steps can change the preferred location of earlier computation. Equal scores prefer fewer cross-node bytes, then fewer processes, then deterministic node ordering.

Hard checks include approved membership, source ownership, explicit placement, per-member and Union task capacity, known executor support, and estimated memory when both an estimate and free-memory observation exist. Runtime/environment compatibility determines fusion candidates. The worker repeats authorization, environment and resource checks when preparing execution; estimates are not resource reservations. Existing work contributes a conservative queue penalty rather than excluding every currently busy node.

## Execution units and transport

Matching installed runtime images, dependency environments and executor configuration can identify a **fusion candidate**. The estimate only groups a whole member partition when graph contraction is acyclic and no materialization/replay or selected read-output boundary prevents fusion. The executor must accept the group at member preparation. The dashboard distinguishes these candidates from actual shared processes; preparation can produce separate units instead.

Edges identify `arrow_in_process_candidate`, same-node `arrow_ipc`, or cross-node `arrow_flight`. Stream transfer can overlap producer computation; the cost model follows dependency critical paths instead of summing all branches. The estimate includes startup, computation, link latency/bandwidth and a queue penalty. It is a heuristic, not a full scheduler simulation. Output size defaults to the sum of upstream output estimates; this does not assert that an arbitrary operator preserves size.

Default costs are deliberately conservative assumptions, not benchmark results: unknown source size 1 MiB, computation 100 ms per non-reader step, startup 500 ms, cross-node throughput 100 MB/s with 2 ms latency, local throughput 1 GB/s with 0.1 ms latency. `item_source` is a host stream source with no operator-process startup cost. Source byte size is only a proxy for output size when SQL, selection or a different representation changes the stream.

## Configuration

Set these in the **Union** startup YAML, not the Job:

```yaml
planning:
  provider: cost
  beam_width: 32
  settings:
    unknown_bytes: 1048576
    compute_ms: 100
    startup_ms: 500
    network_bytes_per_second: 100000000
    network_latency_ms: 2
    local_bytes_per_second: 1000000000
    local_latency_ms: 0.1
```

The beam width is 1–256. Cost values must be finite and nonnegative; bandwidths must be positive. Unknown settings are rejected. A larger beam considers more alternatives at greater planning cost. Plugin configuration is controlled by the administrator; Jobs cannot load optimization code.

The `planning` field in the Job dashboard/history records provider, search width, candidate evaluations, selected score, estimated completion time, cross-node bytes, process count, per-step estimates, execution-unit candidates, edges and warnings. Estimates are explicitly labeled and remain distinct from actual traffic and execution units. Historical replay retains its saved placements and materialization boundaries; it does not silently substitute a newly optimized plan.

## Implemented scope

The planner selects placement using data locality, bounded whole-graph cost comparison and potential process fusion. It preserves the submitted step graph and explicit execution policy. It does not automatically move filters, rewrite joins, infer semantic properties from operator names, introduce materialization or replan a running Job. Those changes require explicit semantic contracts and further validation. Installed optimization plugins can supply workload-specific size/time estimates, link costs, extra placement candidates and scoring without changing the logical graph.

See [optimizer extensions](developers/planning-optimizer.md) and [FAQ](faq.md#16-how-can-i-tune-or-extend-physical-plan-optimization).
