# Authentication and access control

This document describes implemented DACP authentication, authorization boundaries, credential management, revocation and audit. The following sections cover node, Union, S3 and operator repository configuration and operations.

## Service boundaries

| Service | Authentication and access scope |
| --- | --- |
| Ordinary node and Union Flight services | Public metadata over TLS; bearer tokens and subject roles for data and execution |
| Union collaboration calls to members | Mutual TLS and a node-issued Union service credential |
| Task streams between nodes | Mutual TLS and execution plans bound to tasks, ports and consumers |
| S3 service | Shared DACP subjects and read roles; separate access keys, HTTPS, SigV4 and presigned GET |
| Node and Union dashboards | Public read-only access inside the internal network; no login or access control |
| Operator repository | Public approved operators and runtimes; per-submitter Tokens for sources; authenticated administrator review |

Authorization takes place on the server. Ordinary and collaboration SDKs are separate products sharing Flight, certificate and stream protocol implementations. Choosing an SDK does not grant permissions.

## Ordinary clients

Catalogs, Dataset Item lists, Folder child lists and Item metadata (Views, Schema, size, modification time and already available record counts) are anonymous over TLS. Metadata access never requests an explicit count scan. Data streams, queries, computed counts and Job operations require a token. Individual client certificates are not required. Anonymous clients can connect using only the server address: when no CA, token or client certificate is configured, the SDK keeps TLS encryption but skips server certificate verification. An explicit CA enables certificate verification. Authenticated connections retain certificate verification; Union mutual TLS and certificate bindings are unchanged.

Tokens belong to a stable subject, which can hold multiple tokens. Jobs, status, results, cancellation and replay are authorized by job ownership. Token replacement or rotation must preserve the subject and historical job ownership.

Administrators create, list, revoke and rotate credentials using local server tooling. Tokens contain at least 256 bits of randomness, are shown only when created, and are stored as digests. Changes reload automatically. The server checks credentials in memory at the start of each RPC. Revoking or expiring an ordinary token leaves admitted RPCs and result streams running; subsequent RPCs on the same connection must pass authentication again.

## Subject roles and authorization

Roles belong to a stable subject, not to individual tokens. A subject may hold several independently revocable tokens. Assign roles locally with `set-roles`; the command replaces the complete role set. `none` removes all roles. Issuing a token does not grant any role implicitly.

| Role | Permissions |
| --- | --- |
| Anonymous | Public catalog, Item and Folder listings, and Item metadata |
| `reader` | Public metadata plus `data:read`: DACP streams, allowed queries, slices, computed counts and S3 reads |
| `runner` | Reader permissions plus `job:execute` and `job:manage:self`: execution, inference, samples and management/results of owned Jobs |
| Union service | Separate certificate-bound service credential; member collaboration operations, subject to node resource limits |
| Execution consumer | Exact execution-plan authorization for the designated intermediate output; not a permanent user role |

Permissions apply to the whole service; Dataset/Item ACLs and custom roles are not implemented. Runner includes data reading because operator execution can return input data. The service checks permissions and execution ownership, independent of SDK choice. A reader cannot submit Jobs or invoke inference. A token without assigned roles can only use public metadata. An invalid, revoked, expired or S3 credential is rejected even on a metadata request; omit credentials for anonymous access. A client certificate alone grants no data or Job permissions.

The service reads immutable in-memory credential and subject snapshots for each new call. Local changes reload within approximately 250 ms. Already admitted ordinary calls and streams retain their permission snapshot; role changes, token revocation and expiry affect subsequent calls. Union service revocation still cancels associated active collaboration. A malformed credential file rejects its credentials. A malformed subject file clears shared user roles, denying new DACP data/execution and S3 requests while leaving Union service authentication unchanged.

Subjects are stored in `subjects.json` in the credential directory, independently of `credentials.json`:

```json
{
  "analyst": {"roles": ["runner"]},
  "data_client": {"roles": ["reader"]}
}
```

The CLI validates roles and atomically writes protected files. It provides no remote administration interface. Internal node/Union dashboards remain read-only, unfiltered by user identity and protected by deployment networking, not these roles. Their pages and HTTP APIs must remain on the internal network; do not expose them through a public proxy. S3 shares the instance’s subjects and roles, using separate signing keys. Operator-repository accounts remain independent.

## Union and member nodes

At startup, Union reads the member list for its single Union from configuration. There is no remote member management API. A node administrator issues a service credential bound to the Union identity and client certificate fingerprint. Union connects using the configured member address and credential file, without invitation, approval or additional operator allowlists.

Accepting the credential grants trust in Union collaboration operations. Nodes can independently serve other clients they permit. Union maintains a stable identity and records member identities. An unavailable member does not prevent startup; polling retries later.

Each collaboration call checks the service credential. Unlike ordinary tokens, revocation or expiration of a Union service credential must stop the associated active collaboration tasks and streams.

Members of the same Union need no additional peer grants. However, certificates issued by the same CA alone do not grant arbitrary access to another member's data. Execution plans must constrain the execution instance, task, output port and consumer. Union service secrets must never be forwarded to downstream nodes.

## S3

DACP and its S3 endpoint share the instance’s `subjects.json` and roles. A subject may hold both DACP tokens and S3 access keys. S3 credentials receive `data:read` from reader or runner; an S3 key never authenticates a DACP call or grants Job execution. S3 has no anonymous mode: listings, metadata and content require both valid SigV4 authentication and a reader or runner role. DACP metadata remains anonymous. Node and Union instances each maintain their own subject directory; this is not a global account service.

S3 supports SigV4 header authentication and presigned GET URLs for temporary object sharing. Key revocation rejects subsequent requests, including new Range requests; admitted downloads continue. Role changes apply to each new S3 request, including presigned GET and Range requests. Removing all subject roles blocks new data reads through both protocols without rotating credentials. Already admitted downloads continue. Tokens and S3 keys can be revoked or rotated independently; role assignments survive rotation. Local presigning also requires a current read role. A presigned URL is a temporary credential and must not be logged or shared publicly.

## Dashboards and operator repository

Dashboards are internal, read-only services. Deployment networking controls their exposure. This design does not provide public internet login protection.

Approved operator browsing and downloads are public. Developers submit source through per-submitter Token APIs; private source and pending artifacts are not public. Administrators sign in separately, manage Tokens and environments, inspect automatic scan/build results, assign multiple categories and a final public trust score, and approve publication. Versions cannot be overwritten. Detailed scan reports remain private; scores are review assessments, not safety guarantees. See [source submission and review](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission.md).

## Audit and credential storage

Access logs should record the subject, operation, resource, outcome, duration, byte counts and associated job. Credential and member lifecycle events should also be recorded. Never log raw tokens, private keys, S3 secrets or complete signed URLs.

Credential files should be accessible only to the service account. Ordinary tokens are stored as digests. S3 signature verification requires recoverable key material, so it would be incorrect to describe all credentials as digest-only.

## Implementation and verification status

Implemented features include local credential management and hot reload, anonymous metadata, subject roles and ordinary Flight token authentication, certificate-bound Union service credentials, job ownership isolation, exact peer plan authorization, separate collaboration SDKs, S3 SigV4 and presigned GET. Automated tests cover admitted streams continuing after ordinary token revocation while subsequent calls fail, active collaboration stopping after service credential revocation, ownership surviving token rotation, and audit records for rejected requests. Live integration verifies authentication on three member nodes, Union collaboration calls, and boto3 signed listing, metadata, content, Range and presigned GET requests.

Related documentation: [access logs](access-logs.md), [data access](dataset-access.md), [operator repository](https://github.com/rdcn-link/xstream-operator-repository/blob/main/README.md). Data-read and execution examples require a token file with the appropriate subject role; Union member configuration requires a node-issued service credential file.

## Local credential operations

Set node `credential_directory: /protected/node-credentials` and configure a separate directory for Union. By default, a node uses `.dacp-security/PORT` under its process working directory, and Union uses `state_directory/credentials`. Directory permissions are 700 and credential files are 600.

Local tools use these subcommands. Servers themselves only start services; they do not expose administration commands:

| Tool | Subcommands |
| --- | --- |
| `dacp-admin` | Common account, credential and execution-history commands |
| `union-admin` | Identical common commands, plus `identity CONFIG` and `members CONFIG` |

Common commands share one implementation: arguments, defaults, JSON output and errors are identical. Pass the target service's credential directory. `create-account` creates an account and issues both a DACP token and S3 keys; its default role is reader. Use runner for Job execution. `create-token` issues an additional token for an existing account. `grant-union` issues a member's certificate-bound Union service credential, without an ordinary account or S3 keys. Superseded nested commands are removed.

```bash
target/universal/stage/bin/dacp-admin create-account /protected/node-credentials --subject alice --role runner --expires-in 86400
target/universal/stage/bin/dacp-admin create-account /protected/node-credentials --subject data_client
target/universal/stage/bin/dacp-admin create-token /protected/node-credentials --subject alice
target/universal/stage/bin/dacp-admin list-subjects /protected/node-credentials
target/universal/stage/bin/dacp-admin set-roles /protected/node-credentials alice reader
target/universal/stage/bin/dacp-admin list-credentials /protected/node-credentials
target/universal/stage/bin/dacp-admin rotate-credential /protected/node-credentials CREDENTIAL_ID
target/universal/stage/bin/dacp-admin revoke-credential /protected/node-credentials CREDENTIAL_ID
```

Use `set-roles DIRECTORY SUBJECT none` to remove account permissions for subsequent DACP and S3 requests; admitted streams continue. Credentials can be revoked or rotated independently.

Account creation returns `subject`, `roles`, `token_id`, `token`, `access_key_id`, `secret_key` and `expires_at_ms`. Secrets are returned only on issuance; listing hides them. Protect the response and save the `token` value separately in a mode-600 text file for `--token-file`; do not use the complete JSON as a token file. Duplicate account creation fails without changing existing permissions.

First run `union-admin identity CONFIG` to obtain the stable `union_id` and `scheduler_identity`. Then issue a credential locally at each member:

```bash
target/universal/stage/bin/dacp-admin grant-union /protected/node-credentials --subject union-service --union-id UNION_ID --fingerprint SCHEDULER_IDENTITY
```

Save the returned `token` in that member's Union `credential_file`. Subsequent calls read replacement file contents. Unreachable members, invalid credentials and temporarily unreadable credential files produce unavailable status and are retried by polling without preventing Union startup.

No separate S3 issuance is required. `access_key_id` and `secret_key` sign S3 requests and cannot authenticate DACP calls. See [S3](s3-service.md) for presigning and boto3 examples.

The authentication layer also writes `authentication-audit.jsonl`, recording RPC methods, subjects, status and duration, including authentication and authorization rejections. `credential-audit.jsonl` records credential management events. Each rotates to its corresponding `.previous.jsonl` after exceeding 1 MiB. Node access logs and execution history supply business payload counts and resource details.

`list-history DIRECTORY [OFFSET LIMIT]` and `get-history DIRECTORY RECORD_ID` read local history; see [execution history](execution-history.md). `identity CONFIG` reads or initializes Union identity. `members CONFIG` diagnoses members online using configured credentials; other administration commands operate locally.
