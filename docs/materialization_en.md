# Execution policy, output materialization, and replay

[中文](materialization.md)

Execution policies apply to both single-node DACP Server jobs and collaborative Union workflows submitted through Python or Scala clients. The business task file defines operators, inputs and outputs, dependencies, and placement. A separate execution-policy JSON controls materialization of **connected output ports**.

Direct streaming without output retention remains the default. `materialize` saves original OperatorFrames on the producing node while forwarding them, preserving bytes or records schemas and batch boundaries. It creates no Dataset, does not route intermediate data through Union, and does not checkpoint internal operator state.

## Administrators: enable node storage

Add this to DACP nodes that need materialization or replay:

```yaml
union:
  state_directory: .union/a
  execution_timeout_seconds: 1800
  retention_seconds: 300
  materialization_store:
    provider: file
    settings:
      directory: .materialized/a
      max_bytes: 1073741824
      max_task_bytes: 268435456
      retention_seconds: 86400
```

`directory` is private node configuration, not a submitter-selected path. The file plugin ships with the standard distribution in `plugins/dacp-plugin-execution-file`. Without `materialization_store`, the node rejects tasks requiring materialization or replay. Union prepares every node before starting any step, so unsupported storage causes preparation failure rather than silent fallback.

The file plugin requires `directory`; the other three settings default to the values above and must be positive integers. `max_bytes` limits managed data across the directory; `max_task_bytes` limits all selected outputs of one task, including frame length prefixes and manifest overhead. Exceeding quota fails execution and removes uncommitted data. Materialization retention is independent of in-memory `union.retention_seconds`. Expired artifacts cannot be acquired by new executions; files remain until active leases close. Only one writer service may open a storage directory. Startup removes uncommitted temporary directories.

All selected ports of a task commit atomically only after the operator succeeds, its inputs are fully consumed, and the selected outputs are complete. Uncommitted data cannot be replayed; cancellation, failure, and quota errors discard it. A downstream failure does not delete previously committed upstream outputs. The file plugin syncs files, publishes a manifest atomically, and validates lengths and SHA-256 to reject truncated or corrupt replay data.

## Users: separate policy JSON

For example, `execution-policy.json`:

```json
{
  "max_retries": 0,
  "materialization": {
    "allowed": true,
    "default": "stream",
    "tasks": {
      "ta": {
        "outputs": {
          "result": "materialize"
        }
      }
    }
  }
}
```

- `allowed` defaults to `true`: permission alone does not enable materialization.
- `default` defaults to `stream`; use `materialize` to select all connected outputs.
- `tasks.<task id>.outputs.<port>` overrides the default with `stream` or `materialize`. The task ID identifies a step, not a server.
- Omitting policy means direct streaming. The engine never enables materialization based on risk estimates.
- Unknown fields, incorrect types, missing tasks, unconnected ports, invalid modes, and conflicts are rejected at submission. The engine does not create consumers for otherwise unconnected operator ports.

To prohibit engine retention of result streams throughout the workflow:

```json
{"materialization":{"allowed":false,"default":"stream"}}
```

Any materializing default or task override then produces an error, even if that override would not ultimately execute. This does not prohibit operator file writes, format-processing temporary files, logs, or execution metadata. Policy cannot expand a node's operator, input-prefix, capacity, or identity authorization.

Python:

```python
import json
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('workflow.yaml').read_text(),
                      execution_policy=json.loads(Path('execution-policy.json').read_text())) as execution:
        job_id = execution.job_id  # Retain for subsequent replay
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

Scala:

```scala
val policy = dacp.union.Json.parse(java.nio.file.Files.readString(
  java.nio.file.Path.of("execution-policy.json")))
scala.util.Using.resource(union.submit(workflowYaml, policy)) { execution =>
  println(execution.id)
  scala.util.Using.resource(execution.results())(_.foreach(println))
}
```

Status and history retain `execution_policy` (submitted values) and `effective_execution_policy` (connected-port settings after applying defaults and overrides). Node `materialized_tasks` describe source executions and commit status; new execution history links to `replay_of` and `materialized_sources`. Subjobs using materialization disable shared-process step fusion to preserve explicit commit boundaries.

## When replay occurs

Replay occurs only on an **explicit client request**. The source execution must have terminated: succeeded, failed, cancelled, expired, or marked interrupted after restart. Active workflows cannot be replayed, and automatic retry occurs only when `max_retries` is explicitly positive.

```python
with union.replay(job_id, tasks=['ta']) as replay:
    print(replay.job_id)  # A new execution ID
    for event in replay.results():
        if event['type'] == 'data':
            print(event.get('batch', event['data']))
```

`execution.replay(tasks=['ta'])` is also available. Scala equivalents are `union.replay(jobId, Some(Seq("ta")))` and `execution.replay(Some(Seq("ta")))`.

- **Downstream failure**: reuse committed `ta` outputs without rerunning its operator; rerun downstream steps that are still needed. The client must consume a new result stream.
- **Retrieve successful results again**: if every connected output of the final-output steps was materialized, replay those steps directly. Their raw inputs and upstream operators are unnecessary; pruned steps are recorded as `skipped`.
- Without `tasks`, inspect policy-eligible steps whose connected outputs all require materialization, select those with complete, unexpired artifacts, then prune the required workflow from its final outputs. With explicit `tasks`, every requested step must be available or the request fails. At least one replayable step is required.
- Skipping an operator requires **all of its connected outputs** to be materialized. Partial-output materialization can save data but cannot skip a multi-output step. Replay reads ports concurrently with backpressure to avoid waits caused by port ordering.
- Replay uses the original task file and policy; operators, parameters, and inputs cannot be changed through this operation. Nodes revalidate task definitions against stored manifests. The client must still own the source execution, the Union must remain approved, and operator/input authorization is rechecked.
- New replay executions work after Union/node restart while source history and node storage remain available. This is neither automatic restoration of an old process or execution plan nor byte-offset resume.

Expired, incomplete, corrupt, offline, or unauthorized sources produce errors rather than false replay success. Steps not selected for reuse may reread current raw data. This feature provides neither cross-node snapshot consistency nor exactly-once execution of operators with side effects.

## Automatic retry count and overall timeout

The top-level `max_retries` is an integer from 0 to 10, defaulting to `0`; `2` permits two retries after the initial attempt. `timeout_seconds` is an integer from 1 to 86400 covering the entire job, resource waits, and all retries. If omitted, the server's `execution_timeout_seconds` applies; a submitted value cannot exceed that limit. Under Union, individual nodes may impose a smaller local limit.

```json
{"max_retries": 2, "timeout_seconds": 120, "materialization": {"allowed": false}}
```

Automatic retry supports entirely streaming jobs, including jobs that prohibit materialization. Results are delivered incrementally; **each attempt's results remain provisional until successful completion**. Before retrying, the server sends `attempt_failed`. Python callers must supply an `on_retry` callback and Scala callers `Some(callback)` to discard the preceding attempt's partial results before accepting the next attempt. Consumers that do not declare retry awareness cannot claim results for retry-enabled jobs. Discard provisional results after terminal failure, timeout, or cancellation too. Confirm success only after `complete` and a normal Flight stream ending.

```python
pending = bytearray()  # Small-result example; large results need a rollback-capable receiver.

def reset_attempt(event):
    pending.clear()
    print("Retry:", event["retry_count"])

try:
    with execution.results(on_retry=reset_attempt) as results:
        for event in results:
            if event["type"] == "data":
                pending.extend(event["data"])
except BaseException:
    pending.clear()
    raise
```

Every retry has a separate `attempt_id` under the same public execution ID. Reusable committed outputs with every connected port available are reused; other necessary steps execute again. A step with only some output ports stored must execute again. Failed attempt workers and streams stop before notifying the client and starting another attempt. Explicit cancellation, authorization/argument errors, and overall timeout do not retry; Union preparation failures do not retry either. Operator side effects may occur repeatedly: retries do not guarantee exactly-once behavior. Explicit replay creates a new execution, preserves the policy, and starts a fresh retry budget.

## Single-node multi-step jobs

DACP Server single-node jobs support the same policy JSON, stream notifications, and storage plugin without configuring Union. Administrators add these top-level server settings:

```yaml
execution_timeout_seconds: 1800
materialization_store:
  provider: file
  settings:
    directory: .materialized/local
    max_bytes: 1073741824
    max_task_bytes: 268435456
    retention_seconds: 86400
```

These settings serve single-node execution; `union.materialization_store` serves that node's Union subjobs. Both settings may reference the same store definition, whose lifecycle is managed centrally by the server. Without storage, streaming jobs and automatic retries work, but materialization and replay do not. Materialization commit boundaries use separate step execution; explicit single-node policies disable shared-process fusion. Submit cross-node scheduling policies to Union.

```python
from dacp import Job

policy = {
    "max_retries": 2,
    "timeout_seconds": 120,
    "materialization": {"default": "materialize"}
}
# Declare final outputs explicitly in script's outputs; normal input/output formats apply.
with Job.run("dacp://localhost:8815", script,
             execution_policy=policy, on_retry=reset_attempt) as results:
    for event in results:
        if event["type"] == "data":
            pending.extend(event["data"])
    execution_id = results.execution_id

# After the original execution ends, reuse valid complete boundaries and its saved script/policy.
with Job.replay("dacp://localhost:8815", execution_id,
                on_retry=reset_attempt) as results:
    for event in results:
        pass  # Receive a new result; never concatenate it with an earlier result.
```

Only the original submitter can replay; original execution history and unexpired materialized files must remain available. Failed jobs can replay from committed boundaries too. Closing the result stream (`close()`, leaving its context, or Ctrl-C in the shell) cancels execution, steps, and further retries. A callback exception also closes the stream and cancels execution.

Scala uses `Job.run(node, script, executionPolicy = policy, onRetry = Some(reset))` and `Job.replay(node, executionId, onRetry = Some(reset))`. The result stream and `OutputEvent.executionId` expose the execution ID.

The shell supports:

```text
submit workflow.yaml --policy policy.json
run identity@1.1 --input source=/weather/observations --policy policy.json
replay EXECUTION_ID
```

The shell displays the execution ID, explicitly invalidates preceding attempt output before retry, and resets its display state. Previously printed terminal text remains as attempt history; do not treat the entire terminal transcript as a final result file. To materialize final output, declare its ports in the task file's `outputs` and use `submit`.

## Developers: Flight protocol and storage plugins

Use the existing Arrow Flight mTLS transport; there is no HTTP write interface. All Action names use the `dacp.union.` prefix:

| Action | Request JSON | Response |
|---|---|---|
| `submit` | `{"job_yaml":"...","execution_policy":{...}}` | New execution snapshot; policy is optional |
| `replay_job` | `{"job_id":"source ID","tasks":["ta"]}` | New execution snapshot; `tasks` is optional |
| Node `materialized_status` | `{"union_id":"...","execution_id":"...","task":"ta"}` | Commit status, expiry, and stored-port integrity metadata; approved schedulers only |

Results still use the existing `results` Flight stream, requiring `start/data/end/complete` and successful Flight termination. Ordinary result streams remain single-claim; use `replay_job` for a new execution instead of claiming them twice.

Each node `prepare` task adds `materialize: ["result"]` and optional `replay_from: "source execution ID"`. Preparation checks every replay output and acquires leases. Replayed data binds the new execution ID to the intended consumer certificate; disk paths are never exposed directly to clients.

Public contracts live in `dacp-plugin-api`: `MaterializationStoreProvider`, `MaterializationStore`, `MaterializationWriter`, `MaterializedTask`, and `MaterializedChunks`. Register an external provider class through `META-INF/services/dacp.MaterializationStoreProvider`, install its JAR and dependencies in the node `plugin_directory`, and configure `provider` to its `id`. No core registry edits are needed.

Plugins own private configuration, quotas, expiration, atomic task commit, durability, leases, and read integrity. Core owns authorization, policy validation, frame protocol, cancellation, and execution resources. Opaque chunks are at most 9 MiB; plugins must not buffer a whole task. Closing an uncommitted writer must abort it, `get` may expose committed artifacts only, and expired artifacts cannot receive new leases. Cleanup must preserve existing leases. New implementations should test discovery, commit/abort, restart reads, quota, expiration/leases, truncation/corruption, and close cleanup. See `MaterializationSpec` and cross-node `UnionSpec`.

Single-node Flight DoExchange extends `OperatorRequest` with `execution_policy_json` (JSON bytes), `retry_aware` (boolean), and `replay_from` (original execution ID; `job_yaml` must then be empty). The result stream begins with an `execution` metadata frame containing `execution_id`; subsequent output frames include the same ID. Both transports send `attempt_failed` metadata with `attempt_id`, `next_attempt_id`, `retry_count`, and `message` before a new attempt. Union results tickets declare `retry_aware: true`.
