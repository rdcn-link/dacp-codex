# A user-defined Scala operator

This standalone example implements `scala_identity@1.0` without importing or rebuilding the DACP server. It produces a thin JAR, sharing Scala, Arrow, Jackson, and the JVM from `dacp/operator-jvm:1.1`. The operator preserves the original input bytes; replace its batch processing with your transformation.

Example files:

- [Complete Scala source](src/main/scala/example/ScalaIdentity.scala)
- [Thin-JAR sbt build](build.sbt)
- [Operator definition](operators/scala_identity/1.0/operator.yaml)
- [Step-by-step authoring and upload guide](../../../docs/operator-authoring_en.md)

## Build and publish

From the project root, with JDK 17, sbt, and Docker access:

```bash
# Build the shared environment once (skip when already installed).
scripts/build-operator-image.sh
(cd examples/operators/scala && sbt package)
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators examples/operators/scala/operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --port 8820
```

Configure the execution node:

```yaml
remote_operator_library_urls:
  - http://localhost:8820
```

Run in the connected shell:

```text
run scala_identity@1.0 --input source=/events
```

The operator also works locally: set `local_operator_library: ../examples/operators/scala/operators` when launching from `dacp-server`. Build the JAR and install the runtime image first.

## Entry point and stream contract

The JVM launcher passes `operator /input /input/params.json` to `example.ScalaIdentity.main`. Read named input metadata (`port`, `file`, `representation`, `format`) from `/input/manifest.json`. The example accepts one bytes port named `source`, represented by an Arrow IPC stream containing one binary `data` column. Stop at IPC EOS, including for FIFO inputs.

Write start/data/end frames to stdout for the `result` port; diagnostics belong on stderr. The shell consumes output incrementally, without publishing a saved dataset. The complete source demonstrates framing and resource cleanup. See the [execution contract](../../../docs/operator-authoring_en.md).

## Keep the artifact small

[build.sbt](build.sbt) marks runtime libraries `Provided`, so they stay out of the operator JAR. Match Scala and dependency versions to the shared runtime. If your operator needs another library, build a new shared runtime with that library under `/opt/lib`; package private libraries in a self-contained JAR only when appropriate. Reuse existing runtime digests for code-only updates.

[operator.yaml](operators/scala_identity/1.0/operator.yaml) selects the runtime image, executor, main class, and JAR path. Publish a distinct name/entrypoint for another operator. Several operators can reference the same JAR containing different main classes. Adding them requires no core registry edits, server rebuild, or client changes.

To exercise this JAR and both Python artifact types in real containers, build the Python environment as described in the [Python example](../python/README_en.md), then run from the project root:

```bash
(cd dacp-server && DACP_TEST_OPERATOR_PACKAGES=1 sbt 'testOnly dacp.OperatorPackageSpec')
```

See [operator packaging](../../../docs/operator-packaging.md) for executor plugins, validation, caching, resource limits, and platform restrictions.

For complete source examples, wheel packaging, remote file transfer, administrator publication, and execution instructions, see [Write and upload custom operators](../../../docs/operator-authoring_en.md).
