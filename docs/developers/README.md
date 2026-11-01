# Service integration and development

DACP publishes Datasets and exposes remote Job execution using an embedded XStream engine. Start with the [user guide](../users/README.md) for Shell and SDK usage.

## Integrate an application

- [Embed a server](../embedded-server.md): startup, shutdown and injection of an application catalog.
- [Scala client](../../dacp-client-scala/README.md): remote data and Job calls.
- [Python collaboration client](../../dacp-collaboration-client/README.md) and [Scala collaboration client](../../dacp-collaboration-client-scala/README.md): node collaboration calls.
- [Install server plugins](../plugins.md): host configuration and plugin lifecycle.

## Protocol reference

- [DACP protocol](../protocol.md): discovery, remote reads and caller capabilities.
- [Service reference](../reference.md): descriptors, tickets and configuration.
- [Union protocol](../union.md): member trust, subjob dispatch and direct streams.
- [Authentication](../security.md): roles, credentials and authorization checks.
- [Execution history](../execution-history.md): service records and administration.

The wire definitions are in [dacp.proto](../../proto/dacp.proto). `dacp-server` hosts service ingress and authorization; the clients implement remote calls; `dacp-union-server` coordinates members.

## Data and execution extensions

Use the [XStream development guides](http://xstream.conoai.cn/docs.html) for Dataset backends, formats, operators, packaging, stream contracts and execution-storage implementations. DACP integrates these plugins through its server configuration.

## Validate changes

Run from the repository root:

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
(cd dacp-union-server && sbt test)
```

For cross-node container integration, run `bash scripts/test-collaboration.sh` with Docker and the certificates and runtimes specified by that script. Keep service callers, protocol definitions, examples and English/Chinese documents synchronized.
