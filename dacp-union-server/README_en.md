# DACP Union server

[中文](README.md)

Scala coordination and proxy service maintaining one Union. Members run independently. Union reads its member configuration, polls health and resources, and schedules streaming jobs. Members do not register or send heartbeats.

Union exposes standard DACP catalogs, Item access and Job execution through public dataset names that hide internal addresses. Intermediate task streams travel directly between members; final results return through Union. Its internal HTTP dashboard shows members, traffic, execution policy, attempts, DAGs and persistent history without authentication.

## Build and configuration

Requires JDK 17 and sbt. Run in this module:

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

Paths are relative to the process working directory:

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
credential_directory: ../.union-demo/x/credentials
members:
  - url: dacp://localhost:18861
    credential_file: ../.union-demo/a-union.token
  - url: dacp://localhost:18862
    credential_file: ../.union-demo/b-union.token
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
web: {host: 127.0.0.1, port: 19880}
s3: {enabled: true, host: 127.0.0.1, port: 21860}
```

## Authentication and membership

Follow [authentication and access control](../docs/security_en.md) for local credential issuance. Each member issues a Union service credential bound to its stable Union ID and certificate fingerprint. Put the secret in that member's `credential_file`. Members are configured only in this file; there are no remote invitation, approval or member-management APIs. Unavailable members are polled again and do not block startup. Credential revocation stops active collaboration work. Member certificate changes require an explicit local review of the saved pin in `state_directory/member-identities` and newly bound credentials when applicable. Keep Union state to preserve its identity.

Ordinary clients use TLS and a bearer token, without requiring a client certificate. Create one locally:

```bash
target/universal/stage/bin/dacp-union-server credentials ../.union-demo/x/credentials create --subject alice
```

Store the returned token in a protected file and use `dacp-shell dacp://localhost:19860 --ca ../examples/certs/ca.crt --token-file /protected/alice.token`. See [client examples](../docs/union-proxy_en.md). Jobs use the public namespace or logical node IDs. Owner checks protect status, cancellation, replay and result/event streams.

## Execution and interfaces

Jobs connect steps through `inputs`; control dependencies are unsupported and output-free operators are allowed. Every operator implements [structure inference](../docs/structure-inference_en.md). [Execution policy](../docs/materialization_en.md) controls materialization, replay, retry count and total timeout. Automatic retries require clients to discard a failed attempt's partial output; final materialization is optional. Streaming uses bounded queues and backpressure. Jobs interrupted by restart are not automatically resumed; committed materializations can be replayed.

Flight Actions with prefix `dacp.union.` include `submit`, `replay_job`, `job_status` and `cancel_job`. Result and event tickets carry `job_id`; events also carry `after`. Ordinary discovery uses `dacp.catalog`, and logical nodes use `dacp.nodes`. Requests are limited to 1 MiB. The HTTP dashboard is an internal read-only interface, not a credential administration service. S3 is independently authenticated with SigV4; see [S3](../docs/s3-service_en.md).

Each job supports 64 steps and 1024 connected ports, with 32 inputs/connected outputs per step. Union permits 64 active jobs and 4096 in-memory sessions. Events retain up to 128 entries within 4 MiB; stale cursors receive a gap notification. Back up configuration, credentials, Union identity, member pins, namespace routes, execution history and any materialization stores. Protect credentials separately from public data.

## Development

The ordinary and collaboration SDKs share transport, certificate and stream implementations in `dacp-union-protocol`; service-side authorization is independent of SDK choice. Dataset proxying uses plugin contracts without backend-specific core branches. Run `sbt test` here and the browser checks in `scripts/test-union-dashboard.py` from the repository root.
