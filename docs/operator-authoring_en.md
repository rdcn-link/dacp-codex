# Write and publish operators

DACP operators consume named data-item streams and produce named output streams. Publish a small Scala/Java JAR, Python source or wheel, or a complete container. Dependencies belong in a shared runtime image. Repository operators run in NORMAL or DEBUG mode; [inline source submission](operator-source_en.md) requires DEBUG.

The repository website supports public search, viewing, and downloads. Publication uses the administrator CLI or the default-enabled [token-authenticated HTTP API](../dacp-operator-repository/README_en.md#token-authenticated-publication-api). An administrator can run `publish` / `seed`, or a publisher can use `scripts/upload-operator.py` with a publication token to upload a locally generated catalog.

## Execution contract

The launcher passes `operator /input /input/params.json`. `/input/manifest.json` contains one entry per named input: `port`, `file`, `representation`, `format`, and an optional base64 Arrow IPC schema message. Input files are FIFOs containing Arrow IPC streams. Bytes use a single binary column named `data`; records retain their original Arrow schema. Stop on Arrow EOS, without waiting for OS EOF. Consume multiple inputs in lexical port order.

Stdout carries frames, and stderr carries diagnostics. Each frame contains two big-endian 32-bit lengths (JSON header, payload), followed by header and payload bytes. Each output port emits `start`, zero or more `data`, and `end`. All output starts precede any data. Start headers declare `port`, `representation`, `format`, and a base64 IPC schema for records. Bytes payloads contain raw chunks; a records payload is one self-contained Arrow IPC stream with one batch. Limits are 64 KiB per header and 8 MiB per payload. Exit nonzero to fail the job. Results already emitted remain partial results; completion is reported only after successful exit.

Outputs are streamed to clients or downstream tasks, with no output directory, saved dataset, or `_output.json`. Container roots and inputs are read-only; private scratch belongs in `/tmp`. Containers have no network and have memory, PID, temporary-storage, cancellation, and deadline limits. Runtime dependencies must already exist. New scientific representations require explicit shared contracts; do not flatten structures silently.

## Scala example

The complete [ScalaIdentity.scala](../examples/operators/scala/src/main/scala/example/ScalaIdentity.scala) independently compiles to a thin JAR using [build.sbt](../examples/operators/scala/build.sbt). It accepts a bytes input named `source`, preserving chunks and their format on output `result`. The [Uppercase.scala source example](../examples/operators/inline/Uppercase.scala) demonstrates the same framing with a transformation.

Run these commands from the project root:

```bash
scripts/build-operator-image.sh
(cd examples/operators/scala && sbt package)
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --image dacp/operator-jvm:1.1 --executor jvm --entrypoint example.ScalaIdentity \
  --artifact examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  --ports examples/operators/scala/operators/scala_identity/1.0/operator.yaml \
  --root .operator-catalog
```

JDK 17, sbt, Linux, and Docker access are required to build/publish. Ordinary JAR execution does not require a compiler in the runtime. Mark shared dependencies `Provided`; bundle only application code and dependencies absent from the selected runtime. Multiple main classes can share one JAR and runtime.

## Python example

The complete [identity.py](../examples/operators/python/identity_python/1.0/identity.py) preserves either byte chunks or Arrow record batches. The shared runtime [Dockerfile](../examples/operators/python/Dockerfile) uses Python 3.12 and PyArrow 23.0.1.

```bash
docker build -t dacp/operator-python:1.0 examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_python@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact examples/operators/python/identity_python/1.0/identity.py \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

For wheels, use the supplied [pyproject.toml](../examples/operators/python/pyproject.toml):

```bash
python -m pip wheel --no-deps --no-build-isolation --wheel-dir /tmp/dacp-wheels examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact /tmp/dacp-wheels/dacp_example_ops-1.0-py3-none-any.whl \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

Install build tools locally first. Wheels install offline with `--no-index --no-deps` into ephemeral `/tmp`, with pip output on stderr. Supply runtime dependencies in the image. Distinct modules can share one wheel/runtime.

## Port declarations and batch publication

`publish --ports FILE` is required and accepts YAML or JSON containing `inputs` and `outputs`. An operator definition can serve as this file:

```yaml
name: identity_python
version: '1.0'
image: dacp/operator-python:1.0
executor: python
entrypoint: identity
artifact: identity.py
inputs:
  source:
    representations: [bytes, records]
    formats: []
outputs:
  result:
    representations: [bytes, records]
    formats: []
```

Each port declares representations (`bytes`, `records`), format restrictions (`[]` means any), and optionally an Arrow schema JSON value. Every task must bind exactly the declared input ports. Place definitions at `<directory>/<name>/<version>/operator.yaml`; artifact paths resolve relative to the definition. Direct CLI artifact paths resolve relative to the current directory. Container executors omit artifact and entrypoint and implement the same protocol through their image entrypoint.

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/python --root .operator-catalog
```

`publish` rejects existing name/version pairs; `seed` verifies existing immutable entries. Names and versions match `[A-Za-z0-9_-][A-Za-z0-9_.-]*`. Runtime archives and code are verified and shared by digest. Serving an existing catalog requires no Docker.

## Transfer and run

Transfer the JAR/source/wheel and its port definition using an administrator-approved channel, for example `scp`. If the runtime is missing, transfer it once with `docker save` and import with `docker load` on the repository machine. Publication exports the installed image by immutable ID. Then serve:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve \
  --root .operator-catalog --host 0.0.0.0 --port 8820
```

Configure the DACP node with `remote_operator_library_urls: [http://repository-host:8820]`, or install a definition and code under `local_operator_library`. In the connected shell:

```text
run identity_python@1.0 --input source=/reports/orders
run scala_identity@1.0 --input source=/reports/orders
```

The shell waits and prints the result incrementally. No output dataset is saved. See [stream contracts](operator-streams.md) and [DEBUG heredoc source](operator-source_en.md).
