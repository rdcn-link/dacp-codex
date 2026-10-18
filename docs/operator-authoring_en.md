# Write and publish user-defined operators

A user-defined operator is a standalone program plus a repository manifest. You do not implement a server plugin or rebuild the server for each new operator. Use a thin Scala/Java JAR or a Python source file/wheel; reuse a dependency environment across operators.

**Publication currently requires the repository administrator's CLI.** The website is public for search, viewing, and downloads; it has no HTTP upload endpoint or self-service contributor account. “Upload” therefore means transferring your code to the repository machine and having an authorized administrator run `publish` or `seed`.

All commands below start from the project root unless marked otherwise. Local publication needs JDK 17, sbt, Linux, Docker engine access, and initial dependency downloads. Serving already-published artifacts needs no Docker. Use an empty development catalog for the current manifest format.

## 1. Understand the execution contract

The executor starts your main class, script, or module with:

```text
operator /input /output /input/params.json
DACP_OPERATOR=<name>@<version>
```

- Read `/input/params.json` and validate supported parameters. The identity examples use `{}` and do not transform values.
- Read `/input/manifest.json`, an array such as `[{"file":"0.arrow","path":"/events","format":"csv"}]`. The `file` is relative to `/input`; the public `path` need not be a physical filename.
- Each input is an Arrow IPC stream with a single binary column named `data`. Values carry chunks of original file bytes, **not automatically decoded records**. For a CSV transformation, decode those chunks as a CSV byte stream before processing rows. Chunk boundaries need not coincide with records or characters.
- Process bounded batches and close resources. A single-input task can use FIFOs; stop at Arrow EOS rather than waiting for operating-system EOF. Multiple task inputs are materialized before execution.
- Write each result as a binary Arrow IPC stream under `/output`, then write `_output.json`. For example:

```json
[{"file":"_stream-0.arrow","path":"part-0.csv","format":"csv"}]
```

`file` identifies your IPC stream; `path` is the final decoded filename; `format` describes the resulting bytes. Use single filenames, never paths or symlinks; final `path` cannot start with `_`. Do not label transformed bytes with an incorrect format. The server decodes and publishes files only after successful process exit. Throw an exception or exit nonzero to report failure.

Your container has no network, a read-only root/code/input, writable `/output`, and temporary `/tmp`. Dependencies must already exist in the runtime. Current limits include 1 GiB memory, 128 PIDs, 512 MiB temporary storage, and a 30-minute execution deadline. Runtime images must match the execution platform.

## 2. Scala example: preserve the input bytes

The complete runnable project is [examples/operators/scala](../examples/operators/scala/README_en.md). Start with this working implementation and replace the batch-copy section with your own processing. It imports Arrow/Jackson, not the DACP server implementation.

`src/main/scala/example/ScalaIdentity.scala`:

```scala
package example

import java.nio.file.{Files, Path, StandardOpenOption}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import scala.jdk.CollectionConverters._

/** Independently compiled operator: no dependency on DACP server classes. */
object ScalaIdentity {
  def main(args: Array[String]): Unit = {
    require(args.length == 4 && args(0) == "operator", "expected operator INPUT OUTPUT PARAMS")
    val input = Path.of(args(1)); val output = Path.of(args(2))
    val json = new ObjectMapper()
    val params = json.readTree(Path.of(args(3)).toFile)
    require(params.isObject && params.size() == 0, "scala_identity accepts no parameters")
    val manifest = json.readTree(input.resolve("manifest.json").toFile)
    val result = json.createArrayNode()
    val allocator = new RootAllocator(256L * 1024 * 1024)
    try manifest.elements.asScala.zipWithIndex.foreach { case (item, index) =>
      val format = item.path("format").asText
      require(format.matches("[a-z][a-z0-9_-]*"), "invalid input format")
      val filename = item.path("file").asText
      require(filename.nonEmpty && !filename.contains("/") && !filename.contains("\\"), "invalid input filename")
      val streamName = s"_stream-$index.arrow"
      val reader = new ArrowStreamReader(Files.newInputStream(input.resolve(filename)), allocator)
      try {
        val root = reader.getVectorSchemaRoot
        require(root.getSchema.getFields.size() == 1 && root.getVector(0).isInstanceOf[org.apache.arrow.vector.VarBinaryVector] &&
          root.getSchema.getFields.get(0).getName == "data", "expected Arrow bytes view")
        val writer = new ArrowStreamWriter(root, null,
          Files.newOutputStream(output.resolve(streamName), StandardOpenOption.CREATE_NEW))
        try {
          writer.start()
          // Process one bounded batch at a time. IPC EOS also works with DACP FIFO inputs.
          while (reader.loadNextBatch()) writer.writeBatch()
          writer.end()
        } finally writer.close()
      } finally reader.close()
      val extension = Map("excel" -> "xlsx", "sql" -> "bin").getOrElse(format, format)
      result.addObject().put("file", streamName).put("path", s"part-$index.$extension").put("format", format)
    } finally allocator.close()
    Files.write(output.resolve("_output.json"), json.writeValueAsBytes(result))
  }
}
```

`build.sbt`:

```scala
name := "scala-identity"
organization := "example"
version := "1.0"
scalaVersion := "2.13.16"
// Supplied by dacp/operator-jvm:1.1; keep them out of the thin operator JAR.
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "18.3.0" % Provided,
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0" % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3" % Provided)
Compile / mainClass := Some("example.ScalaIdentity")
```

Mark dependencies supplied by the runtime as `Provided`; otherwise a fat JAR can duplicate them. The environment `dacp/operator-jvm:1.1` supplies compatible Scala 2.13, Arrow 18.3.0, and Jackson libraries. If your operator requires different/additional libraries, build a matching shared runtime; do not download dependencies during tasks.

Build the shared JVM environment and repository launcher once:

```bash
# From the dacp-codex project root.
scripts/build-operator-image.sh
(cd dacp-operator-repository && sbt stage)
```

Build the operator and publish its small JAR on the repository machine:

```bash
(cd examples/operators/scala && sbt package)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --root .operator-catalog --image dacp/operator-jvm:1.1 \
  --executor jvm --entrypoint example.ScalaIdentity \
  --artifact examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  --description "Copy input bytes using a small Scala operator"
```

The tested example JAR is about 5.5 KB. Runtime size is separate and downloaded once per digest on each node.

## 3. Python example: source or wheel

The runnable source is [identity.py](../examples/operators/python/identity_python/1.0/identity.py):

```python
"""DACP byte-preserving operator. Dependencies live in the shared runtime image."""
import json
from pathlib import Path
import re
import sys

import pyarrow as pa


def main():
    if len(sys.argv) != 5 or sys.argv[1] != "operator":
        raise ValueError("expected operator INPUT OUTPUT PARAMS")
    source, target = map(Path, sys.argv[2:4])
    params = json.loads(Path(sys.argv[4]).read_text())
    if not isinstance(params, dict) or params:
        raise ValueError("identity_python accepts no parameters")
    result = []
    schema = pa.schema([("data", pa.binary())])
    for index, item in enumerate(json.loads((source / "manifest.json").read_text())):
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", item["format"]):
            raise ValueError("invalid format")
        name = f"_stream-{index}.arrow"
        with (source / item["file"]).open("rb") as raw:
            with pa.ipc.open_stream(raw) as reader:
                if reader.schema != schema:
                    raise ValueError("expected Arrow bytes schema")
                with (target / name).open("xb") as sink:
                    with pa.ipc.new_stream(sink, schema) as writer:
                        for batch in reader:
                            writer.write_batch(batch)
        # IPC EOS terminates FIFO reads; do not wait for operating-system EOF.
        extension = {"excel": "xlsx", "sql": "bin"}.get(item["format"], item["format"])
        result.append({"file": name, "path": f"part-{index}.{extension}", "format": item["format"]})
    (target / "_output.json").write_text(json.dumps(result))


if __name__ == "__main__":
    main()
```

The runtime [Dockerfile](../examples/operators/python/Dockerfile) uses Python 3.12 with `pyarrow==23.0.1`. Build it once and publish the source:

```bash
docker build -t dacp/operator-python:1.0 examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_python@1.0 \
  --root .operator-catalog --image dacp/operator-python:1.0 \
  --executor python --entrypoint identity \
  --artifact examples/operators/python/identity_python/1.0/identity.py \
  --description "Copy input bytes using Python"
```

For several modules/operators or compiled extensions, distribute a wheel. The included [pyproject.toml](../examples/operators/python/pyproject.toml) packages the same script as the `identity` module:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "dacp-example-ops"
version = "1.0"
description = "Small DACP operator code using a shared Python/Arrow runtime"
requires-python = ">=3.12"
dependencies = ["pyarrow==23.0.1"]

[tool.setuptools]
py-modules = ["identity"]
package-dir = {"" = "identity_python/1.0"}
```

```bash
# Requires setuptools and wheel in the selected build environment.
.venv/bin/python -m pip wheel --no-deps --no-build-isolation \
  --wheel-dir examples/operators/python/dist ./examples/operators/python

dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 \
  --root .operator-catalog --image dacp/operator-python:1.0 \
  --executor python --entrypoint identity \
  --artifact examples/operators/python/dist/dacp_example_ops-1.0-py3-none-any.whl
```

The example wheel is about 2 KB. Its `dependencies` metadata records the expected runtime dependency; the executor installs with `--no-index --no-deps`, so PyArrow must already be present. For source artifacts, `entrypoint` is required metadata and the script itself executes; for wheels, it selects the Python module to run. The module must call its processing function under `if __name__ == "__main__"`.

Multiple main classes in one JAR or multiple modules in one wheel can be published as separate operator names, sharing the exact same artifact and runtime. Change the name/entrypoint when publishing each operator.

## 4. Optional definition files and batch publication

Instead of individual `publish` commands, add `<definitions>/<name>/<version>/operator.yaml`:

```yaml
name: scala_identity
version: "1.0"
description: Copy bytes with Scala
image: dacp/operator-jvm:1.1
executor: jvm
entrypoint: example.ScalaIdentity
artifact: scala-identity_2.13-1.0.jar
```

Place the JAR beside that definition, or set `artifact` to its relative path. `publish --artifact` resolves relative to your shell's working directory; `operator.yaml` resolves relative to the definition file. The provided example definition already points to its build output.

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/scala/operators --root .operator-catalog
```

Use either `publish` or `seed` for a new version. `publish` refuses an existing name/version; repeated `seed` verifies existing artifacts without replacing them. For changed code, publish a new version and update job references. Names/versions match `[A-Za-z0-9_-][A-Za-z0-9_.-]*`. Python definitions use `executor: python`, `entrypoint: identity`, and a `.py` or `.whl` artifact. Complete custom containers use `executor: container`, omit `artifact`/`entrypoint`, and provide the same Arrow boundary in their image entrypoint.

## 5. Upload to a repository on another machine

Ask the administrator for a transfer destination and the approved runtime image. The following paths/hostnames are examples; the administrator creates the incoming/catalog directories and grants appropriate access. This does not publish through the public website.

```bash
# Contributor: transfer the small JAR to an administrator-provided directory.
scp examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  repository-user@repository-host:/srv/dacp/incoming/

# Administrator: run on repository-host, from its dacp-codex checkout.
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --root /srv/dacp/operator-catalog --image dacp/operator-jvm:1.1 \
  --executor jvm --entrypoint example.ScalaIdentity \
  --artifact /srv/dacp/incoming/scala-identity_2.13-1.0.jar
```

The administrator must have the runtime image installed in their Docker engine to publish its pinned image ID/archive. If it is already installed, transfer only the small code file. Otherwise transfer the environment once, then run the publication command:

```bash
# Contributor, only when the repository machine lacks the required runtime.
docker save -o /tmp/dacp-jvm-runtime.tar dacp/operator-jvm:1.1
scp /tmp/dacp-jvm-runtime.tar repository-user@repository-host:/srv/dacp/incoming/
# Administrator, on repository-host:
docker load -i /srv/dacp/incoming/dacp-jvm-runtime.tar
```

For Python, transfer `identity.py` or the wheel and use `--executor python --entrypoint identity` with the Python runtime image. Publishing stores a copy of the code, so later changes to the incoming file do not alter a published version.

## 6. Serve, browse, and run

If the repository is not running, start it with the **same catalog root used for publication**:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve \
  --root .operator-catalog --host 0.0.0.0 --port 8820
```

For the remote example, replace `.operator-catalog` with `/srv/dacp/operator-catalog`. An already-running server sees new publications without restart. Use the [HTTPS options](../dacp-operator-repository/README_en.md#https-and-operation) or your deployment's HTTPS proxy for public access.

Browse `http://localhost:8820/?q=identity`, or verify manifests:

```bash
curl -f http://localhost:8820/v1/operators/scala_identity/1.0
curl -f http://localhost:8820/v1/operators/identity_python/1.0
```

Pages show separate code/runtime sizes, hashes, download links, and entrypoints. To download manually, read `artifact.sha256` and `runtime.archive.sha256` from the manifest and request `/v1/blobs/sha256/<digest>`; verify SHA-256 before use.

Configure the execution node (replace localhost with a reachable repository host):

```yaml
remote_operator_library_urls:
  - http://localhost:8820
operator_cache: /var/lib/dacp/operators
```

Choose a cache path writable by the node process. Restart the node after changing its configuration. Save this as `custom-job.yaml`, replacing the input URI with an existing authorized dataset/item:

```yaml
job: custom-operators
tasks:
  - id: scala-copy
    inputs: [dacp://localhost:8815/demo/events]
    operator: scala_identity@1.0
    params: {}
    output: scala_result
  - id: python-copy
    inputs: [scala_result]
    operator: identity_python@1.0
    params: {}
    output: python_result
```

`dacp-shell` currently browses and queries datasets; it does not have an operator/job submission command. From a Bash terminal, use `Job.run` as below. With the Python SDK installed and the coordinating DACP node running, submit using your authorized client certificates. These certificate paths are for the local examples:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python - <<'RUN_JOB'
from pathlib import Path
from dacp import Job, Source

outputs = Job.run("dacp://localhost:8815", Path("custom-job.yaml").read_text(), timeout=180)
for name, uri in outputs.items():
    print(name, uri)
    with Source.open(uri) as dataset:
        for item in dataset.list():
            print(item.path, item.format)
RUN_JOB
```

To use the wheel instead, change `identity_python@1.0` to `identity_wheel@1.0`. No operator-specific client or core registry changes are needed. Output datasets expire according to the execution node's TTL.

## 7. Verify your operator before publication

Build the Scala example and Python environment, then run the existing real-container contract tests:

```bash
(cd dacp-server && DACP_TEST_OPERATOR_PACKAGES=1 sbt 'testOnly dacp.OperatorPackageSpec')
```

These cover the example JAR, Python source, and wheel across the Arrow boundary. For your transformation, test representative inputs, invalid parameters/formats, multiple batches/inputs, expected output content/schema, and failures. Do not test only process exit: decode the returned bytes and confirm the transformation. Preserve scientific arrays/groups through an explicit representation rather than silently flattening them.

For implementation details and executor-plugin registration, see [operator packaging](operator-packaging.md) and [plugin contracts](plugins.md).
