# DACP Union scheduling server

[中文](README.md)

`dacp-union-server` is an independent Scala module. One process maintains one persistent Union, invites independently running DACP nodes, and actively polls status and capacity. X can schedule computation only after local node approval. Nodes do not register with X or send heartbeats.

Client C submits Jobs to X over Arrow Flight mTLS, subscribes to status events, and incrementally reads final output. B pulls intermediate streams directly from A; final streams return through X to C. Outputs create neither Datasets nor saved files.

## Build and run

Requires JDK 17, sbt, and the nodes' existing operator runtime environments. From this module directory:

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

Example configuration; paths are relative to the process working directory:

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
client_identities: ["<64 lowercase hexadecimal characters: SHA-256 of client leaf certificate DER>"]
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
```

Every endpoint requires a client certificate. TLS verifies the certificate chain and hostname; application identity is the verified certificate fingerprint. Private keys use unencrypted PKCS#8 PEM. Certificates used by X and nodes need both `serverAuth` and `clientAuth`; C needs `clientAuth`. Flight hostnames must match certificate SANs. `client_identities` is an exact allowlist: sharing a CA does not grant submission access. A Job is visible only to its submitter. Node connections additionally pin the invited node certificate fingerprint. Certificate rotation requires invitation and approval again; use a new state directory when rotating X's certificate.

## Membership management

These are local X administrator commands. They share configuration and state with the running service:

```bash
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server remove ../.union-demo/x.yaml NODE_ID
```

`invite` creates a pending invitation at the node and returns its certificate fingerprint and data origin. `members` actively queries approval and runtime status; network failures produce `unknown` without removing the node. `remove` first asks the node to revoke membership, then deletes X's local record; a network failure retains the record and reports an error. Removal or revocation cancels affected running jobs. Each node retains at most 128 invitations.

Add this to the DACP node YAML, reusing its own TLS configuration:

```yaml
union:
  state_directory: ../.union-demo/a
  execution_timeout_seconds: 120
  retention_seconds: 300
```

From `dacp-server`, the node administrator runs:

```bash
target/universal/stage/bin/dacp-server union list ../.union-demo/a
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union reject ../.union-demo/a UNION_ID
target/universal/stage/bin/dacp-server union leave ../.union-demo/a UNION_ID
```

The approval policy must provide every field:

```yaml
scheduler_identity: "<X certificate fingerprint>"
input_prefixes: [dacp://localhost:18861/data]
operators: [quantity_sum@1.0]
max_concurrent_tasks: 4
expires_at_ms: 4102444800000
```

Approval binds the Union ID to X's actual certificate identity and limits compute input prefixes, exact operator versions, concurrency, and expiry. Compute permission does not change ordinary Flight download permission. Only pending invitations can be approved; rejected or revoked invitations can be invited again. State files use file locks and atomic replacement, allowing administration alongside the service.

## Jobs and clients

The [complete two-node example](../examples/union/README_en.md) includes operators, configuration, approval, and a client: A aggregates sales; B combines them with local prices. Tasks require explicit `node`, versioned operators, and final `outputs`. Inputs reference Items on the executing node or upstream Task/Port pairs.

Python:

```python
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('examples/union/job.yaml').read_text()) as execution:
        print(execution.job_id)
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

`execution.status()` queries status; `execution.cancel()` cancels the entire job. `execution.events(after=0)` returns an independent Flight event iterator that can run in another thread. Sequence numbers increase monotonically; the latest 128 events are retained within a 4 MiB budget. A stale cursor receives `gap` with the current snapshot. Explicitly close iterators on early exit; leaving the execution context cancels unfinished jobs and closes connections. Closing only the status subscription does not cancel execution. Results are single consumption streams; a second claim returns `ALREADY_EXISTS`.

The Scala client `dacp.client.Union` provides the same `submit`, `status`, `cancel`, `events`, and `results` interfaces. Close executions and iterators with `Using.resource`:

```scala
val union = new dacp.client.Union("dacp://localhost:19860",
  java.nio.file.Path.of("examples/certs/client.crt"),
  java.nio.file.Path.of("examples/certs/client.key"),
  java.nio.file.Path.of("examples/certs/ca.crt"))
scala.util.Using.resource(union.submit(java.nio.file.Files.readString(
  java.nio.file.Path.of("examples/union/job.yaml")))) { execution =>
  scala.util.Using.resource(execution.results())(_.foreach(println))
}
union.close()
```

## Execution semantics and limitations

X validates the DAG and groups tasks by explicit node. Insufficient capacity produces `waiting_resources`; every node is prepared before any starts. A capacity race during preparation rejects the job and cleans up prepared nodes without automatic retry. Union and ordinary Flight tasks share the node execution capacity. Each node must reserve all its subjob tasks simultaneously. A job allows at most 1024 input and connected output ports in total. Each task allows at most 32 inputs and 32 connected outputs. A job allows at most 64 tasks, X allows 64 active jobs, and each node and X retain at most 4096 execution records.

States are `queued`, `waiting_resources`, `preparing`, `running`, `succeeded`, `failed`, `cancelled`, and `expired`. Data channels use two-frame queues and Flight backpressure. Consumers verify upstream success through `complete`, rather than accepting data `end` alone. Final-result disconnection, cancellation, failure, authorization revocation, or timeout cleans up the global job. Execution timeout also applies while waiting for a client to claim results.

Snapshots include node Task states and per-input/output cumulative `payload_bytes`, `remote_payload_bytes`, and `last_activity_ms`; records streams also report `records`. Counters measure payloads, excluding Flight/gRPC/TLS overhead. Sending and receiving are counted separately. The status channel is independent of output queues.

Membership and execution records persist; running sessions and results are in memory. Restart does not recover jobs; nodes clean up by their own timeout if X crashes. Inline source submission, fan-out, automatic retries, checkpoints, result replay, and automatic placement optimization are unavailable. Existing Flight `Job.run` retains its own execution path; Union uses separate interfaces. X uses 128 Flight executor threads: leave room for control requests alongside long connections and set concurrency according to resources.

## Developer interfaces

All Action types have the `dacp.union.` prefix. Requests and single responses are JSON limited to 1 MiB. Addresses use `dacp://host:port`; nodes need no separate Union port. The operator repository retains HTTP.

| Interface | Type | JSON request or Ticket |
| --- | --- | --- |
| Submit | `DoAction submit` | `{"job_yaml":"..."}` |
| Status / cancel | `DoAction job_status` / `cancel_job` | `{"job_id":"ID"}` |
| Member status | `DoAction members` | `{}` |
| Results | `DoGet` | `{"type":"dacp.union.results","job_id":"ID"}` |
| Events | `DoGet` | `{"type":"dacp.union.events","job_id":"ID","after":0}` |

Node `DoAction` operations are `invite`, `membership`, `withdraw`, `node_status` (`union_id`), `prepare` (`union_id`, `execution_id`, `tasks`), and `start`, `execution_status`, `cancel_execution` (`execution_id`). Node output Tickets are `{"type":"dacp.union.output","execution_id":"ID","task":"ta","port":"result"}`.

Results and intermediate streams have Arrow schema `data: binary`, one row per batch containing a complete OperatorFrame. Events use `event: binary`, one JSON event per batch. Successful results include `start/data/end/complete` and require successful Flight end-of-stream. Failures use Flight status codes. Previous HTTP interfaces have been removed.

Node protocol, mTLS, and JSON helpers live in `dacp-union-protocol`. Node execution lives in `dacp-server`'s `UnionWorker`, using existing `DatasetAccess`, `TaskExecutor`, and operator contracts; domain analysis belongs in example operators. The [protocol contract](../docs/union_en.md) and `UnionSpec` cover authorization, direct transport, capacity waiting, failures, and cancellation.

## HTTP operations dashboard

Add `web` to U's configuration to start the website with `serve`. Without this section, no HTTP listener is opened:

```yaml
web:
  host: 127.0.0.1
  port: 19880
```

Open `http://127.0.0.1:19880/` in a browser. The two-node example's `prepare.py` generates this configuration. The website refreshes every 2 seconds and shows member approval, health and observation time, node capacity and Union quotas, available executors, job filters, task dependencies, elapsed time, and per-port byte and record counters. Select a job for details. Rates use successive node observations; stale observations are explicitly marked. The list displays the latest 100 jobs; counters cover all currently retained jobs. In-memory sessions follow `retention_seconds`; execution metadata persists indefinitely and remains available after restart through the paginated execution-history view.

This is a public read-only view across all submitters' execution metadata, including nodes, operators, and input Item URIs. It neither reads result contents nor claims result streams. Invitation, approval, submission, and cancellation are unavailable on the website. Node communication, client submission, and data streams continue to use Flight; the operator repository continues to use HTTP.

The dashboard is publicly readable without a token. To allow remote browsers:

```yaml
web:
  host: 0.0.0.0
  port: 19880
```

Read-only HTTP interfaces: `GET /api/overview`, `GET /api/jobs/ID`, and `GET /api/history?offset=0&limit=100`. Pages and APIs publicly expose node status, task scripts, DAGs, and execution history. State responses disable caching and cross-origin access. Task submission, computation, and data access still require Flight certificates and authorization. Stopping U also releases its HTTP listener.

All global jobs persist execution records by default, and nodes also record local subtasks. For directories, query commands, history interfaces, and interruption semantics, see [execution history](../docs/execution-history_en.md).
