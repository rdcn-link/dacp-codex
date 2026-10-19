# DACP operator runtime

[中文](README.md)

Standalone Scala code for `format_convert@1.1` and `identity@1.1`, without the data server, Docker client, SQL drivers, Excel readers, or task coordinator. Both operators share one thin JAR and a JVM/Arrow/Parquet dependency environment. The server has only a test dependency on this module.

Build from the project root on Linux, with JDK 17 including `jlink`, sbt, Python 3, `ldd`, and local Docker engine access:

```bash
scripts/build-operator-image.sh
.venv/bin/python scripts/test-operator-image.py
```

The builder produces:

- `dacp/operator-jvm:1.1`: stripped JVM, native libraries, and shared dependency JARs under `/opt/lib`; no operator implementation JAR.
- `dacp-operator-runtime/target/universal/stage/lib/default.dacp-operator-runtime-0.1.0-SNAPSHOT.jar`: small code bundle, published separately by both starter definitions using `executor: jvm`.
- `dacp/format-convert:1.1`: optional complete container adding the code bundle as a separate layer on the shared environment.

The optional first argument selects the complete-image tag, for example `scripts/build-operator-image.sh dacp/format-convert:test`. Test with `.venv/bin/python scripts/test-operator-image.py --image dacp/format-convert:test`.

Images use `scratch`; no base-image pull, JDK, sbt, shell, or server distribution is included. Build on the Docker engine's Linux architecture (x86_64/aarch64). Initial dependencies need Maven access. Snappy/Zstd native libraries are extracted at build time, avoiding executable extraction into `/tmp`. The environment supplies native codec paths via `JAVA_TOOL_OPTIONS`.

The entrypoint receives `operator /input /input/params.json`; `DACP_OPERATOR` selects an exact version. Operators implement the [Arrow IPC contract](../docs/reference_en.md#task-execution-and-operators). Parquet output uses Java file I/O without Hadoop shell commands. Conversion supports CSV/NDJSON, inferred/explicit schemas, and none/Snappy/Zstd compression. The image test checks both formats, all compression modes, schema metadata, Arrow decoding, and identity byte preservation with disabled networking, read-only root, and temporary `/tmp`.

Publish from the project root after staging the repository:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
```

On the tested Linux x86_64 build, the shared runtime archive is 116,087,296 bytes and the starter code bundle is 37,090 bytes. Both are stored once per digest, rather than a full archive per operator. Dependency/JDK/architecture changes affect measurements. Versions are immutable and unsupported version aliases are not provided.

See [operator packaging](../docs/operator-packaging.md), the [Scala contribution example](../examples/operators/scala/README_en.md), and the [Python example](../examples/operators/python/README_en.md).

Outputs use stdout start/data/end frames and create no output dataset. Parquet scratch files exist only in container `/tmp`. See the [stream contract](../docs/operator-streams.md).
