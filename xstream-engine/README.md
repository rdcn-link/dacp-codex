# XStream Engine

Standalone and embeddable Scala execution of streaming YAML Jobs. One JVM shares one **DataCatalog → Dataset → Item** hierarchy. DACP Server embeds this engine and uses its own DataCatalog directly; no server is needed for local execution.

Requires JDK 17 and sbt. From the repository root:

```bash
cd xstream-engine
sbt test stage
cd ..
xstream-engine/target/universal/stage/bin/xstream-runner \
  examples/xstream/engine.yaml examples/xstream/read.yaml
```

`xstream-runner` is a program shipped by this module, not a separate module. Records are printed as JSONL; bytes are streamed directly to stdout. The launcher accepts `CONFIG.yaml JOB.yaml [VARIABLES.yaml] [POLICY.yaml]`.

Use `read_item.params.uri: context:/dataset/item` for the bound DataCatalog, or an explicit `dacp://host:port/dataset/item` URI. Configure remote connections and credentials in `nodes`. Operators come from approved public repositories; existing isolated runtimes and compatible-operator fusion remain in use.

See [the full guide](../docs/xstream.md) for Scala embedding, plugin registration, cancellation, materialization/replay, catalog lifecycle and designer configuration. [中文](README_zh.md).

## Components and scope

- `xstream-engine`: the embeddable execution engine and `xstream-runner` launcher.
- [xstream-designer](../xstream-designer/README.md): visual YAML Job authoring, local and remote catalog browsing, schema inference and bounded samples.
- DACP Server: hosts a DataCatalog, embeds the engine and exposes data access and execution services.
- Union Server: plans and dispatches cross-node Jobs using the existing coordinator.

Local execution reads Items directly from the JVM catalog. Explicit remote URIs use configured client connections; the standalone runner automatically pushes closed processing chains to their configured remote data service. Stream connections do not require intermediate files. Materialization and replay are explicit execution-policy choices; operator packages, runtimes and execution history may still use disk.

The example configures an operator repository at `http://localhost:8820`; its built-in `read_item` step needs no repository download. Jobs using repository operators require the configured repository or a verified local cache. Run the launcher from the repository root so its relative data and storage paths resolve correctly. It selects exactly one Job output and writes that stream to stdout. The embedded API supports multiple outputs.

See [example configuration](../examples/xstream/engine.yaml), [example Job](../examples/xstream/read.yaml), and [Job syntax](../docs/jobs.md).

See [automatic pushdown and policy boundaries](../docs/xstream.md#automatic-compute-pushdown). Disable globally with `compute_pushdown_enabled: false`, or use `nodes[].compute_pushdown: false` for a data-only connection.
