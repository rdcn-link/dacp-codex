# DACP plugin integration

XStream owns data backends, format and container handlers, catalogs, operator executors and execution stores. See [XStream plugins](https://github.com/rdcn-link/xstream-codex/blob/main/docs/plugins.md) for implementation contracts, registration and development tests. DACP owns service integration, authorization and host resource management, without redefining those contracts.

## Installation and configuration

Install plugin JARs and private dependencies not already supplied by the host in `plugin_directory`, then restart. Do not bundle duplicate public APIs. Data plugins compile against `xstream-data-core`; execution extensions use `xstream-execution-api`, with `Provided` scope. Register through `META-INF/services/` inside each JAR. Built-in and external implementations use the same mechanism.

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  documents:
    provider: directory
    source:
      root: /srv/data
```

Plugins run in the server JVM as administrator-installed trusted code. They are distinct from user-submitted operators and are not isolated by operator containers. Plugins validate parameters; DACP enforces service authorization. Registration changes require a restart.

## Embedded hosts and lifecycle

`DacpServer.builder().plugins(environment, owned = false)` injects an application-owned `PluginEnvironment` that survives server shutdown. Request access handles close after each request; the server releases resources it owns on shutdown.

Execution history can use `execution_store: {provider: file, settings: {directory: /var/lib/dacp/executions}}`. The file implementation lives in XStream's `plugins/xstream-plugin-execution-file`. XStream defines the interfaces, atomicity and materialization behavior.

## Service adaptation

DACP maps the shared catalog to remote access and authorizes protected operations. Its S3 adapter reuses the same data access object without a second backend registration. Records-only Items can expose JSONL bytes through the shared adapter; see [S3 service](s3-service.md).

See [configuration reference](reference.md) and [embedded server](embedded-server.md) for host integration.
