# User access logs

DACP nodes write completed Flight requests as JSON Lines for line-oriented collection. Logs are independent of execution history and do not retain stream contents. The default directory is `.dacp-access/<port>/` relative to the server working directory, with a 64 MiB file limit.

```yaml
access_log:
  directory: /var/log/dacp/access
  max_bytes: 67108864
```

`max_bytes` must be a positive integer. If the next record would exceed the limit of a nonempty file, a new file is opened before appending; one JSON record never spans files. An oversized record occupies its own file. Names are `access-<timestamp>-<UUID>.jsonl`; restarts never overwrite old logs. Old files are retained without automatic deletion; deployment tooling owns collection and retention. Concurrent requests append serially, and completed records are synchronized to disk before returning.

Fields include request ID, start/end timestamps, TLS-verified identity fingerprint, operation, logical Dataset/Item/view where applicable, status, elapsed time, sent batch rows and bytes. Bytes views count raw binary cell payloads; records bytes sum Arrow vector buffers and are not network wire traffic. Task exchanges count outer frames. Requests without a started stream have zero traffic.

Statuses include `completed`, `failed`, `denied`, and `cancelled`. Discovery, metadata, data streams, task exchanges and Union actions are logged. TLS handshake failures happen before Flight request dispatch and require transport/proxy logging. Entries omit raw tickets, operator source, parameters, data content, credentials, physical backend addresses, and raw exception messages.

The configured directory must be writable. Startup initializes logging; shutdown releases file handles. Use [execution history](execution-history_en.md) for task states and events; request completion does not establish business-job success.
