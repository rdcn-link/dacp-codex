# Extend physical planning

Implement `dacp.union.PhysicalPlanOptimizer`, published by `dacp-union-protocol`. The built-in `cost` provider and external providers use the same `PluginEnvironment` discovery and lifecycle. See [physical planning](../physical-planning.md) for the model and limitations.

## Contracts

| Method | Purpose | Limits |
|---|---|---|
| `validate(settings)` | Validate plugin-owned startup parameters | Reject unknown/invalid values |
| `estimate(input, step, node, inputBytes, settings)` | Optional output size, compute/startup time and memory estimate | Finite nonnegative values; no source reads |
| `link(input, from, to, settings)` | Optional bandwidth and latency estimate | Positive finite bandwidth; nonnegative latency |
| `candidates(input, settings)` | Additional complete step-to-node assignments | At most 256; exact step IDs, approved node IDs |
| `score(plan, settings)` | Objective, minimized by the search | Finite nonnegative score |

Returning `None` from an estimate/link hook uses the baseline model. `PlanningInput` contains immutable steps, source sizes, operator references, serialized parameters, per-node installed profiles and targets. A profile's fusion key is advisory and remains subject to executor verification. Target IDs are certificate identities, not display names. Do not log full step parameters if they contain sensitive values.

Callbacks can be called repeatedly for partial and complete candidates. They must be deterministic, side-effect free, fast, bounded and thread-safe; never execute operators or fetch data during scoring. Resource acquisition belongs in `initialize`, and release in `close`. A plugin exception or invalid returned value rejects planning rather than silently changing policy. Server cancellation is checked throughout candidate search. Plugin code is trusted in-process code and must cooperate; it is not sandboxed.

## Install a provider

Compile with Scala 2.13 against `io.github.rdcn-link:dacp-union-protocol_2.13:0.1.0-SNAPSHOT` using `Provided` scope. Obtain the host-matching SDK from the local build until published artifacts are available. Do not bundle duplicate public APIs. Put the provider's class name in:

```text
META-INF/services/dacp.union.PhysicalPlanOptimizer
```

Place the JAR and its private dependencies in the Union's `plugin_directory`, select the provider and restart the Union:

```yaml
plugin_directory: /opt/dacp/union-plugins
planning:
  provider: transfer-aware
  beam_width: 32
  settings:
    transfer_penalty_ms_per_mb: 10
```

[Runnable Scala example](../../examples/planning-optimizer/README.md) supplies this `transfer-aware` provider. It adds a configurable transfer penalty to estimated completion time. It can be extended with measured selectivity or domain-specific compute costs through `estimate`. Installed built-in `cost` settings are documented in the user guide; external settings belong entirely to the provider.

```mermaid
flowchart LR
    S[Immutable planning snapshot] --> P[Plugin estimates / candidates]
    P --> V[Union constraint validation]
    V --> C[Core critical-path estimate]
    C --> R[Plugin score]
    R --> B[Bounded candidate selection]
```

## Safety and verification

Union validates all candidates, including plugin-supplied assignments, against ownership, explicit placements, capacity and known executor/memory constraints. Plugins cannot add members, change credentials, alter runtime profiles, mutate the logical graph or bypass publication policy. Environment compatibility is checked again on the member. Configuration belongs to the administrator, not to a submitted Job.

Test at least: a known workload chooses the expected member; missing statistics stay conservative; invalid assignments/numbers fail; forbidden origins/roles remain denied; fusion boundaries remain separate; cancellation propagates; and repeated identical snapshots produce identical selections. The core contract suite is `GraphPlacementSpec`; integration coverage is in `UnionSpec`.
