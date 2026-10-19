# Review of user customization and extension boundaries

[中文](plugin-roadmap.md)

This review follows current access, configuration, execution, collaboration, and persistence paths. Implemented contracts are listed separately below; remaining proposals are not implemented. See [plugins/](../plugins/README_en.md) and [the plugin guide](plugins_en.md) for available extension points and installation.

The criterion is whether adding a user implementation requires no core registry changes, server rebuild, or client changes. Plugins should own implementation-specific settings and dependencies while preserving common protocol, authorization, cancellation, backpressure, and resource semantics. Plugins are trusted server code; plugin support does not provide sandbox isolation.

## Implemented foundation contracts

- `PluginEnvironment`: per-server discovery, injection and ownership, without JVM-global registries or thread-context classloader changes.
- `ByteSource`: reopenable format inputs with optional ranges, size, version and local-path optimization; local, sequential remote and range-readable objects share the format contract.
- `mounted` Dataset provider: nested Dataset definitions compose arbitrary backend namespaces. PublishedItem/SqlConfig live in the backend plugin; core sources retain only provider/settings.
- `ExecutionStoreProvider`: core owns execution transitions/recovery; the file plugin owns atomic files and the single-writer lock. Nodes and Union choose implementations through execution_store.

Registration, configuration and ownership are covered in [plugins](plugins_en.md) and [Dataset providers](dataset-providers_en.md). Concrete MongoDB/S3 backends are not bundled; extension contracts do not imply those implementations exist.

## Priority boundaries

| Priority | Current coupling and location | Proposal and practical benefit |
|---|---|---|
| Medium | [Config.authorize](../dacp-server/src/main/scala/dacp/Server.scala) uses node URI prefixes; [Coordinator](../dacp-union-server/src/main/scala/dacp/unionserver/Coordinator.scala) uses a client set; [UnionMembership](../dacp-server/src/main/scala/dacp/UnionMembership.scala) manages approval scopes | Add injectable, discoverable authorization policies receiving verified identity, action, and logical resource, supporting institutional policies, roles, and dynamic grants. Core must still enforce TLS authentication, Union approval boundaries, and common denial behavior |
| Medium | [TaskExecutor](../dacp-server/src/main/scala/dacp/TaskExecutor.scala) directly creates Docker clients and containers; [OperatorLibrary.resolve](../dacp-server/src/main/scala/dacp/OperatorLibrary.scala) also accepts DockerClient | Separate artifact resolution from runtime preparation, then define a full `ExecutionBackend` lifecycle. Existing `OperatorExecutor` continues producing language/entrypoint launch commands; Docker and future Podman/Kubernetes backends own execution, cancellation, resources, and cleanup. This does not authorize unrestricted host execution |

Native Dataset providers can already expose MongoDB/S3-style Items and streams directly. Byte sources and generic mounting address **format-plugin reuse and cross-backend composition**, rather than prerequisites for a single backend. `mixed` currently combines files and JDBC explicitly.

The instance-scoped plugin environment defines ownership rules for construction, configuration validation, startup, rollback, request resources, shutdown, and classloader closure. Closing a request or embedded server must not unexpectedly close application-owned shared connection pools. These goals do not require hot reload.

## Further extensions when needed

- **Observation outputs**: execution and history logic already produce task states, elapsed time, and traffic. An `ExecutionObserver`/`MetricsSink` could integrate OpenTelemetry or institutional audit systems. Optional observers must not accidentally block result streams; durable audit guarantees cannot depend on a lossy callback.
- **Repository storage**: [RepositoryServer](../dacp-operator-repository/src/main/scala/dacp/repository/RepositoryServer.scala) and [Publishing](../dacp-operator-repository/src/main/scala/dacp/repository/Publishing.scala) directly use local directories. A manifests/blobs storage interface could support S3-backed repositories while preserving HTTP publication/downloads, checksum validation, and version immutability.
- **Scheduling policies**: Coordinator currently requires explicit task placement and embeds capacity waiting, preparation, and startup. Extract `PlacementPolicy`/`SchedulingPolicy` when automatic placement, data locality, quotas, or priorities are needed; core continues validating plans, authorization, and stream connections. This does not claim general DAG scheduling, retries, or checkpoint recovery are implemented.
- **Credentials**: introduce a shared credential resolver when several backends need Vault, STS, or refreshable institutional credentials. Backends can already interpret private source configuration without adding core branches for each authentication method.

## Improve contracts instead of opening arbitrary plugin hooks

[Producer.fail](../dacp-server/src/main/scala/dacp/Server.scala) still recognizes `java.sql.SQLException` and matches some exception messages. Replace this with typed failures: plugins translate internal errors into safe public categories, and core consistently maps them to Flight status. Do not expose arbitrary plugin exception text to clients.

[Item.views](../dacp-plugin-api/src/main/scala/dacp/Models.scala), stream ports, and Flight reads explicitly support records/bytes. Arrays, images, multidimensional variables, and selections require cross-client descriptions, schemas, capabilities, and selection semantics before plugins implement them. Registering arbitrary strings or silently flattening scientific structures into records is insufficient.

[OperatorMain](../dacp-operator-runtime/src/main/scala/dacp/OperatorMain.scala) dispatches identity/format_convert by operator name. These are concrete example behaviors that can become ordinary operator JARs, leaving the shared runtime generic. Thin JARs, Python, and independent entrypoints already support user-defined operators, so each operator does not need a new server plugin interface. FormatConvert's CSV/NDJSON/Parquet business constraints are not core protocol limits.

Flight, URI/ticket parsing, namespace security validation, basic stream protocols, cancellation, backpressure, and resource limits remain core responsibilities. Extract policies and implementations that actual user scenarios need to replace, rather than creating an extension point for every branch.

Next priorities are authorization policies, execution backends and scheduling policies backed by concrete requirements. Each step should include a real implementation, registration/configuration examples, and contract tests for discovery, errors, cancellation, and resource ownership.
