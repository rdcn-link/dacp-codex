# User access logs

DACP nodes write completed Flight requests as JSON Lines for line-oriented collection. Logs are independent of execution history and do not retain stream contents. The default directory is `.dacp-access/<port>/` relative to the server working directory, with a 64 MiB file limit.

```yaml
access_log:
  directory: /var/log/dacp/access
  max_bytes: 67108864
```

`max_bytes` must be a positive integer. If the next record would exceed the limit of a nonempty file, a new file is opened before appending; one JSON record never spans files. An oversized record occupies its own file. Names are `access-<timestamp>-<UUID>.jsonl`; restarts never overwrite old logs. Old files are retained without automatic deletion; deployment tooling owns collection and retention. Request threads enqueue records in a bounded queue of at most 1024 entries, then return. A dedicated writer appends them serially without making each request wait for disk synchronization. The writer synchronizes every second or after 256 records, and before rotation and orderly shutdown. A full queue applies backpressure rather than silently dropping records. Disk write failures are logged server-side and surfaced on subsequent appends and shutdown. Abnormal termination may lose records not yet written or synchronized; access logs are not per-request transaction commits.

Fields include request ID, start/end timestamps, TLS-verified identity fingerprint, operation, logical Dataset/Item/view where applicable, status, elapsed time, sent batch rows and bytes. Bytes views count raw binary cell payloads; records bytes sum Arrow vector buffers and are not network wire traffic. Step exchanges count outer frames. Requests without a started stream have zero traffic.

Statuses include `completed`, `failed`, `denied`, and `cancelled`. Discovery, metadata, data streams, step exchanges and Union actions are logged. TLS handshake failures happen before Flight request dispatch and require transport/proxy logging. Entries omit raw tickets, operator source, parameters, data content, credentials, physical backend addresses, and raw exception messages.

The configured directory must be writable. Startup initializes logging; orderly shutdown drains the queue, synchronizes it, and releases file handles. Use [execution history](execution-history.md) for step states and events; request completion does not establish business-job success.

## Business operation names

`operation` uses business names rather than transport methods. Records are written when requests finish; a start timestamp does not mean a record was emitted on arrival. The separate transport security audit, `authentication-audit.jsonl`, still records RPC methods.

| Operation | Meaning |
| --- | --- |
| `catalog.list` | List datasets |
| `catalog.describe` | Catalog snapshot |
| `item.list` | List items |
| `item.describe` | Item metadata |
| `item.count` | Record count |
| `item.read` | Read an Item view |
| `node.list` | List available nodes |
| `job.replay` | Replay a Job |
| `job.events.read` | Read Job events |
| `job.result.read` | Read Job results |
| `job.submit` | Submit and execute |
| `step.infer` | Infer step outputs |
| `job.infer` | Infer Job outputs |
| `service.capabilities` | Service capabilities |
| `union.membership.get` | Union membership |
| `node.status` | Node resources and capacity |
| `job.prepare` | Prepare sub-job |
| `job.start` | Start sub-job |
| `job.status` | Execution status |
| `job.cancel` | Cancel execution |
| `step.materialization.status` | Persisted output status |
| `step.output.read` | Read cross-node step output |
| `request.unsupported` | Unsupported control request |

## Exception diagnostics

Server runtime logs include full exception stacks and cause chains, with business `operation`, `request_id`, and the known `job_id`. Before a Job is created, its ID is marked unavailable. Background execution uses correlation identifiers; dispatched sub-job execution IDs correlate with Union execution history. Access logs retain outcomes and correlation identifiers without exception stacks, request bodies, or credentials. Job execution errors return a concise description and an available request ID to clients. Runtime logs are for operators: exception stacks can contain internal file paths and third-party error details.
