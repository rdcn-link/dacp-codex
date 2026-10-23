# DACP Union service

[中文](README.md)

`dacp-union-server` is a Scala coordination service. Each process maintains one Union. Members run independently; Union invites and polls them and dispatches computation after local approval. Nodes neither register nor send heartbeats.

Union exposes ordinary DACP Flight services: online catalogs, Item metadata and records/bytes streams, record scans and Job execution. It also exposes read-only S3 access. Stable short public dataset IDs hide member addresses. Dedicated Union management APIs and the HTTP operations dashboard retain internal topology. See [Union proxy service](../docs/union-proxy_en.md) for naming, forwarding, placement and access boundaries.

## Build and start

Requires JDK 17 and sbt. Run from this module:

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

Configuration paths are relative to the process working directory:

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
client_identities: ["<SHA-256 of client leaf certificate DER: 64 lowercase hexadecimal characters>"]
administrator_identities: ["<administrator certificate fingerprint>"]
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
web:
  host: 127.0.0.1
  port: 19880
s3:
  enabled: true
  host: 0.0.0.0
  port: 20860
```

Flight uses mTLS with hostname verification. Union and members also act as clients, requiring `serverAuth` and `clientAuth`; private keys use unencrypted PKCS#8 PEM. CA signatures do not grant computation. Client and administrator identity lists are separate; only a job's owner can read its results or cancel it. Member connections pin certificates. Rotating node certificates requires invitation and approval again; rotating Union certificates requires a new state directory.

## Invitation and approval

These local Union administration commands can share state with the running service:

```bash
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server remove ../.union-demo/x.yaml NODE_ID
```

Enable `union` on a node, then obtain node administrator approval:

```yaml
union:
  state_directory: ../.union-demo/a
  execution_timeout_seconds: 120
  retention_seconds: 300
```

Run from `dacp-server`:

```bash
target/universal/stage/bin/dacp-server union list ../.union-demo/a
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union leave ../.union-demo/a UNION_ID
```

Example approval policy:

```yaml
scheduler_identity: "<Union certificate fingerprint>"
input_prefixes: [dacp://localhost:18861/climate]
operators: [daily_temperature_stats@1.0]
max_concurrent_tasks: 4
expires_at_ms: 4102444800000
```

Approval binds the Union ID, certificate identity, input prefixes, operator versions, quota and expiry. Removal requests remote withdrawal before deleting the local record; network failures report errors and preserve it. Revocation or removal stops affected jobs. Temporary outages report unknown status without automatic member removal. Catalogs are fetched online; short-ID routes persist. Complete successful listings remove missing routes, without accumulating tombstones, while the counter never moves backwards.

## Clients and jobs

Ordinary clients connect through `Source.datasets`, `Source.open`, `Job.run` and the shell, using public URIs and either implicit placement or logical node IDs. Multiple inputs are processed at one member, with cross-node inputs streamed. Final streams return through Union. See [proxy service and job examples](../docs/union-proxy_en.md).

Dedicated asynchronous interfaces support independent status, cancellation and event subscriptions:

```python
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('job.yaml').read_text()) as execution:
        print(execution.job_id)
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

`execution.status()`, `cancel()` and `events(after=0)` query, cancel and subscribe. Scala `dacp.client.Union` provides corresponding methods. Close executions and iterators to release resources. Results can be claimed once; disconnecting final results cancels unfinished jobs, while closing status subscriptions does not. Ordinary clients use the public namespace; administrators can submit explicit internal placements through the asynchronous API. See the [complete two-node example](../examples/union/README_en.md).

## Policy, materialization and execution semantics

Separate policy JSON controls `materialization`, `max_retries` (0–10) and `timeout_seconds`. Outputs stream by default. Explicit materialization stores output on its producing member for replay in a new execution. Automatic retries require clients to recognize failed attempts and discard previous partial output; final-output materialization is optional. Total timeout includes queuing and every attempt. See [execution policy](../docs/materialization_en.md) for configuration, calls and plugin contracts.

The engine prepares all members before starting; insufficient capacity waits. Intermediate streams travel directly between nodes, using bounded queues and Flight backpressure. `end` alone is insufficient: consumers verify `complete` and successful Flight end-of-stream. Cancellation, failure, revocation and timeout clean up the job. Union can participate upstream through the ordinary worker contract without a dedicated nesting protocol; paths detect cycles and enforce depth limits.

Each job allows 64 steps and 1024 connected ports; each step allows 32 inputs and 32 connected outputs. Union allows 64 active jobs and 4096 in-memory sessions; members reserve all subjob slots simultaneously. Events retain at most 128 entries within 4 MiB; stale cursors receive gap. History metadata persists, but running jobs do not recover automatically after restart. Materialized outputs support explicit replay. Output fan-out, inline source and internal operator checkpoints are unsupported. Backups should include members, routes, approvals and execution history.

## Management API and dashboard

Management Flight Actions use the `dacp.union.` prefix and a 1 MiB JSON limit:

| Action | Request |
| --- | --- |
| submit | `{"job_yaml":"...","execution_policy":{}}`, policy optional |
| replay_job | `{"job_id":"ID","tasks":["ta"]}`, tasks optional |
| job_status / cancel_job | `{"job_id":"ID"}` |
| members | `{}`, administrator |
| invite_member | `{"url":"dacp://node:18861"}`, administrator |
| remove_member | `{"identity":"fingerprint"}`, administrator |

Result Tickets use `{"type":"dacp.union.results","job_id":"ID"}`; event Tickets use `{"type":"dacp.union.events","job_id":"ID","after":0}`. Events require administrator identity. Ordinary DACP jobs use `DoExchange`, catalogs use `dacp.catalog`, and logical nodes use `dacp.nodes`.

The HTTP dashboard shows real members, scripts, states, traffic, DAGs and paginated history without claiming results. It refreshes every two seconds. Locally bundled AntV G6 supports horizontal/vertical dependency and swimlane layouts, zoom, pan and group collapse. `GET /api/overview`, `/api/jobs/ID` and `/api/history?offset=0&limit=100` are read-only management APIs. Omitting web disables its listener. Loopback binding is local; external binding requires token_file. See [management boundaries](../docs/union-proxy_en.md#management-boundaries) for token and login configuration. S3 is a separate public read-only data interface.

## Development and validation

`dacp-union-protocol` supplies transport, mTLS and JSON contracts; `UnionWorker` reuses plugin data access and executors. `ProxyCatalog` forwards through `DatasetAccess` without format/backend branches. Domain operators use the existing plugin and repository mechanisms.

```bash
# This module
sbt test
# Repository root; requires Playwright and Chromium
python scripts/test-union-dashboard.py
```

Web resources live in `src/main/resources/web/`. `dag.js` manages graphs and viewports; `app.js` handles read-only requests. Canvas/DOM text rendering prevents metadata execution. Local `@antv/g6@5.1.1` retains its MIT license; upgrades synchronize versioned filenames, HTML, asset routing and tests. Normal builds download no frontend dependencies. See [Union protocol](../docs/union_en.md) and [execution history](../docs/execution-history_en.md) for details and storage paths.
