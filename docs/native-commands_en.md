# Native command operators

Implemented: `executor: command` adapts one bytes input to a program's stdin and its stdout to one bytes output. Programs may be compiled C, C++, Rust, Go, or existing command-line tools. Execution remains inside an isolated Docker container; no host-process executor is provided. Normal mode supports published command operators. Inline native source compilation is not supported.

## Definition

```yaml
name: uppercase
version: "1.0"
image: example/native-runtime:1
executor: command
entrypoint: /usr/bin/tr
executor_config:
  args: [a-z, A-Z]
  stdin: source
  stdout: {port: result, representation: bytes, format: text}
inputs:
  source: {representations: [bytes], formats: []}
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

The runtime must provide `/usr/local/bin/python`, PyArrow, the executable, and its OS/architecture-compatible dependencies. The bridge reuses the existing Python runtime; it does not require Python or Arrow dependencies in the native program. `dacp/operator-python:1.0` can run the [C artifact example](../examples/operators/native/README_en.md), but does not necessarily contain tools such as `tr`. Install required tools in a derived image before publishing.

`entrypoint` is an absolute executable path. `args` is a list of literal strings, passed directly without shell interpretation, expansion, or substitution of job parameters. All three settings `args`, `stdin`, and `stdout` are required; unknown keys fail. Declare the stdin port in `inputs` with only the `bytes` representation. Set `stdin: null` for a source operator or `stdout: null` for a sink. Do not declare `outputs` in the manifest: the mandatory executor inference method derives it from `executor_config.stdout`. A non-null stdout requires `port`, `representation: bytes`, and a nonempty `format`. Formats label content; the adapter does not convert it.

To publish a separate executable artifact, set `artifact: path/to/program` and `entrypoint: /operator/program`, matching the artifact filename. Paths in local definitions are relative to `operator.yaml`. The downloaded artifact is verified and mounted read-only. Because content-addressed storage does not preserve executable permission bits, the bridge copies it into private container `/operator-work`, sets executable permissions, and launches that copy. The plugin requests a bounded 512 MiB executable tmpfs for this copy; ordinary `/tmp` stays non-executable. Shared libraries must already be in the runtime. There is no dependency download or native compilation during execution.

## Stream behavior

The adapter unwraps Arrow IPC byte cells from the named input FIFO, writes their bytes to stdin, and closes stdin after input ends. Concurrently it reads stdout in chunks of at most 64 KiB and emits DACP `start/data/end` frames. Bounded pipes propagate backpressure; the complete input or output is never collected. Stderr remains diagnostics and does not enter the output stream.

Output `end` is emitted only after successful program exit and successful input delivery. A nonzero exit or broken input delivery fails the job; previously delivered data is partial. Cancellation or timeout uses the server's existing container removal lifecycle, terminating the program and its child processes. Commands should consume their complete stdin; early stdin closure is not a successful truncation/preview mechanism. Use shell preview flags to cancel a job intentionally.

The adapter supports zero or one bytes input and zero or one bytes output. Records serialization, multiple pipes, filename arguments, seekable temporary input files, and shell pipelines are not implemented. Use a custom operator wrapper for these cases; do not implicitly flatten structured or multidimensional data.

## Publication and execution

For an existing runtime executable, save the `executor_config` mapping alone as `command.yaml`, and the port declarations as `ports.yaml`:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish uppercase@1.0 \
  --root .operator-catalog --image example/native-runtime:1 \
  --executor command --entrypoint /usr/bin/tr \
  --ports ports.yaml --executor-config command.yaml
```

For an executable artifact, also pass `--artifact path/to/program` and set `--entrypoint /operator/program`. Alternatively, use `seed --operators DIR --root .operator-catalog` with complete definitions. The repository preserves executor-owned settings in `execution.config`; execution nodes validate them through the plugin. Existing repository checksum, shared-runtime, download, and offline-cache behavior applies.

In the shell, use the ordinary command:

```text
run uppercase@1.0 --input source=/reports/orders
```

No client change or native-specific job syntax is required.

## Plugin contract and verification

`CommandExecutor` registers in `META-INF/services/dacp.OperatorExecutor` and ships in `plugins/dacp-plugin-operator-executors`. External launchers use the same registration: compile against `dacp-plugin-api` as Provided, package the service file, put the JAR and private dependencies in `plugin_directory`, then restart the server. `OperatorProgram.configuration` contains the generic executor-owned map; `inputs` and `outputs` map port names to allowed representation sets. Each plugin owns validation. Core only carries these settings and manages resources; it has no command-specific dispatch branch.

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-repository && sbt test)
(cd dacp-server && sbt test stage)
# Requires cc, certificates, Docker access, and dacp/operator-python:1.0:
.venv/bin/python scripts/test-native-execution.py
```

The live test starts and stops its own server on port 18817 (`DACP_NATIVE_TEST_PORT` overrides it), compiles a C filter, and checks streaming beyond pipe capacity, artifacts without executable permission bits, partial failure, cancellation, and recovery. Unit contracts cover registration, literal arguments, unsupported ports, invalid settings, and preservation of plugin settings through publication, download, and cache reuse.
