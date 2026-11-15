# Submit Jobs with execution policies

[中文](execution-policy_zh.md)

A node embeds XStream; Union coordinates execution across approved members. Supply execution policy separately from the Job:

```yaml
max_retries: 1
timeout_seconds: 1800
resources:
  tmp_size_mb: 512
  memory_mb: 1024
```

`max_retries` is the number of additional execution attempts, default zero. Every failed attempt stops all participants and discards staged Sink results before retrying. Without explicit checkpoints all steps rerun; valid committed checkpoints replace covered upstream work. Cancellation, timeout, authorization/configuration errors, cleanup failures and failures after commit starts do not trigger automatic retries. All attempts share the execution budget; Union environment preparation has its own deadline.

Use Shell `submit workflow.yaml --policy execution-policy.yaml` or the SDK. The client sends policy contents, not a server filesystem path. Node/Union startup configuration bounds execution time, container temporary memory and concurrency. The [XStream execution policy reference](https://github.com/rdcn-link/xstream-codex/blob/main/docs/execution-policy.md) defines fields and resource limits.

## Select output checkpoints

```yaml
max_retries: 2
checkpoints:
  sort_step: [result]
```

This policy fully saves `sort_step.result` before its downstream reader starts. If downstream processing fails, the same execution can reuse that output after all old workers stop and Sink discard succeeds. Uncovered branches rerun. Only steps with all connected outputs saved can be skipped. The default remains streaming; omit `checkpoints` or use `{}`. There is no automatic boundary selection or cross-execution replay.

Administrators configure `checkpoint_store` on each producing node (or a Union serving as a member):

```yaml
checkpoint_store:
  provider: file
  settings:
    directory: /var/lib/dacp/checkpoints
    max_bytes: 1073741824
    max_checkpoint_bytes: 268435456
```

The file provider ships in XStream's execution-file plugin. Jobs cannot select host paths. Storage quota and integrity failures are explicit. Union pins checkpoints to their producing member, preserves them between internal attempts, and invokes the certificate-bound `release_checkpoints` operation on terminal completion. Cleanup is scoped by scheduler identity, Union ID and Job execution ID. No arbitrary artifact-read API is exposed. Workers also reclaim abandoned terminal runs after the configured execution, preparation and retention grace periods; host restart clears all old checkpoint files. Checkpoints do not restore lost nodes or processes. See the [XStream checkpoint contract](https://github.com/rdcn-link/xstream-codex/blob/main/docs/execution-policy.md#explicit-output-checkpoints).

## Consume provisional results safely

```scala
import java.io.ByteArrayOutputStream
import scala.util.Using
import dacp.client.{Job, TlsConfig}
import dacp.union.Json

val provisional = new ByteArrayOutputStream()
val policy = Json.obj().put("max_retries", 1)
Using.resource(Job.run(node, workflowYaml, tls,
  executionPolicy = policy,
  onRetry = Some(_ => provisional.reset()))) { stream =>
  stream.foreach { event =>
    if (event.kind == "data") provisional.write(event.data)
  }
}
val finalBytes = provisional.toByteArray // Use only after successful complete consumption.
```

This example assumes a single bytes output and a small result. `node` is a DACP origin, `workflowYaml` is the Job text and `tls` is a configured `TlsConfig` validating the server identity and supplying an authorized token. For large output use a temporary file. Records require Arrow decoding rather than byte concatenation. Union's Scala `execution.results(onRetry = Some(...))` and Python `execution.results(on_retry=...)` follow the same discard rule. Automatic retry requires an explicitly retry-aware receiver.

## Sink coordination and history

An SDK Sink stages its result, then waits for the whole-Job decision. Union waits for all worker subjobs to report `ready_to_commit` and for all result streams to finish, then calls each member's certificate-bound internal `complete_execution`. Nested Union subjobs defer this decision to the parent. The existing cancellation operation discards staging and confirms stopped workers before a retry. These are internal service operations, not new public administration endpoints.

History records the submitted/effective policy, attempt identities and states. `commit_started` marks the publication boundary. Once publication begins, failures stop without automatic retry. Several Sink commits are not a distributed atomic transaction; inspect uncertain outcomes and use idempotent external publication. Temporary format staging, logs, final application results and execution history have separate lifecycles.

To rerun manually, submit the saved Job and resolved variables as a new Job. Stable input versions, pinned operator versions and controlled randomness are necessary for reproducibility.

## Execution-unit mode

Node startup configuration accepts `operator_fusion: isolated`, `fused` or `auto` (default). Union member nodes apply this mode to compatible local sub-Jobs. `auto` uses a conservative structural cost estimate, not measured latency or a guarantee of better performance. Compatibility, Sink, checkpoint and retry boundaries remain enforced. This is not a per-Job policy field. See the [fusion reference](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion.md).
