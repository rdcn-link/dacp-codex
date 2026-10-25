# Authentication and access control

This document describes implemented DACP authentication, authorization boundaries, credential management, revocation and audit. The following sections cover node, Union, S3 and operator repository configuration and operations.

## Service boundaries

| Service | Authentication and access scope |
| --- | --- |
| Ordinary node and Union Flight services | TLS and bearer tokens; server-side authorization |
| Union collaboration calls to members | Mutual TLS and a node-issued Union service credential |
| Task streams between nodes | Mutual TLS and execution plans bound to tasks, ports and consumers |
| S3 service | Independent access keys; HTTPS, SigV4 header authentication and presigned GET |
| Node and Union dashboards | Public read-only access inside the internal network; no login or access control |
| Operator repository | Public browsing and downloads; token-protected publication API, no publication UI |

Authorization takes place on the server. Ordinary and collaboration SDKs are separate products sharing Flight, certificate and stream protocol implementations. Choosing an SDK does not grant permissions.

## Ordinary clients

Clients use tokens; individual client certificates are not required. Clients perform standard TLS CA and hostname verification, without manual certificate pinning or additional identity confirmation.

Tokens belong to a stable subject, which can hold multiple tokens. Jobs, status, results, cancellation and replay are authorized by job ownership. Token replacement or rotation must preserve the subject and historical job ownership.

Administrators create, list, revoke and rotate credentials using local server tooling. Tokens contain at least 256 bits of randomness, are shown only when created, and are stored as digests. Changes reload automatically. The server checks credentials in memory at the start of each RPC. Revoking or expiring an ordinary token leaves admitted RPCs and result streams running; subsequent RPCs on the same connection must pass authentication again.

## Union and member nodes

At startup, Union reads the member list for its single Union from configuration. There is no remote member management API. A node administrator issues a service credential bound to the Union identity and client certificate fingerprint. Union connects using the configured member address and credential file, without invitation, approval or additional operator allowlists.

Accepting the credential grants trust in Union collaboration operations. Nodes can independently serve other clients they permit. Union maintains a stable identity and records member identities. An unavailable member does not prevent startup; polling retries later.

Each collaboration call checks the service credential. Unlike ordinary tokens, revocation or expiration of a Union service credential must stop the associated active collaboration tasks and streams.

Members of the same Union need no additional peer grants. However, certificates issued by the same CA alone do not grant arbitrary access to another member's data. Execution plans must constrain the execution instance, task, output port and consumer. Union service secrets must never be forwarded to downstream nodes.

## S3

S3 and Flight are independent services and do not require a unified user or credential system. S3 has no anonymous reading mode: listings, metadata and content requests all require authentication.

S3 supports SigV4 header authentication and presigned GET URLs for temporary object sharing. Key revocation rejects subsequent requests, including new Range requests; admitted downloads continue. A presigned URL is a temporary credential and must not be logged or shared publicly.

## Dashboards and operator repository

Dashboards are internal, read-only services. Deployment networking controls their exposure. This design does not provide public internet login protection.

Repository browsing and downloading require no authentication. Publication uses a token-bearing service API, has no web publication interface, and cannot overwrite existing versions. Source code, licenses and build materials help users assess operators but do not guarantee safety.

## Audit and credential storage

Access logs should record the subject, operation, resource, outcome, duration, byte counts and associated job. Credential and member lifecycle events should also be recorded. Never log raw tokens, private keys, S3 secrets or complete signed URLs.

Credential files should be accessible only to the service account. Ordinary tokens are stored as digests. S3 signature verification requires recoverable key material, so it would be incorrect to describe all credentials as digest-only.

## Implementation and verification status

Implemented features include local credential management and hot reload, ordinary Flight token authentication, certificate-bound Union service credentials, job ownership isolation, exact peer plan authorization, separate collaboration SDKs, S3 SigV4 and presigned GET. Automated tests cover admitted streams continuing after ordinary token revocation while subsequent calls fail, active collaboration stopping after service credential revocation, ownership surviving token rotation, and audit records for rejected requests. Live integration verifies authentication on three member nodes, Union collaboration calls, and boto3 signed listing, metadata, content, Range and presigned GET requests.

Related documentation: [access logs](access-logs_en.md), [data access](dataset-access_en.md), [operator repository](../dacp-operator-repository/README_en.md). Ordinary client examples require a token file; Union member configuration requires a node-issued service credential file.

## Local credential operations

Set node `credential_directory: /protected/node-credentials` and configure a separate directory for Union. By default, a node uses `.dacp-security/PORT` under its process working directory, and Union uses `state_directory/credentials`. Directory permissions are 700 and credential files are 600.

Run these commands in the corresponding module. For Union, replace the executable with `dacp-union-server` and use its own credential directory:

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials create --subject alice --expires-in 86400
target/universal/stage/bin/dacp-server credentials /protected/node-credentials list
target/universal/stage/bin/dacp-server credentials /protected/node-credentials rotate CREDENTIAL_ID
target/universal/stage/bin/dacp-server credentials /protected/node-credentials revoke CREDENTIAL_ID
```

Creation and rotation return JSON containing the one-time `token`. Save its value as a text file readable only by the service account. Listing never returns secrets. Python uses `token=` or `DACP_TOKEN_FILE`; Scala uses `TlsConfig(tokenFile=...)`; the shell uses `--token-file`. Supply the CA through `--ca`, SDK parameters or `DACP_CA`. Do not use the complete JSON response as a token file.

First run `dacp-union-server identity CONFIG` to obtain the stable `union_id` and `scheduler_identity`. Then issue a credential locally at each member:

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials create --subject union-service --kind union --union-id UNION_ID --fingerprint SCHEDULER_IDENTITY
```

Save the returned `token` in that member's Union `credential_file`. Subsequent calls read replacement file contents. Unreachable members, invalid credentials and temporarily unreadable credential files produce unavailable status and are retried by polling without preventing Union startup.

Create S3 keys with `--kind s3`. The returned `id` is the Access Key ID and `secret_key` is the Secret Access Key; neither acts as a Flight token. See [S3](s3-service_en.md) for presigning commands and boto3 examples.

The authentication layer also writes `authentication-audit.jsonl`, recording RPC methods, subjects, status and duration, including authentication and authorization rejections. `credential-audit.jsonl` records credential management events. Each rotates to its corresponding `.previous.jsonl` after exceeding 1 MiB. Node access logs and execution history supply business payload counts and resource details.
