# DEBUG source operators

Inline source submission is implemented in the Python shell and job API. The server accepts it only when its YAML configuration contains `mode: DEBUG`; the default is `NORMAL`. This is a server execution setting, independent of logging verbosity. Both the coordinator and every execution node enforce the gate, including direct worker requests. Repository operators remain available in NORMAL mode.

Configure a fixed allowlist of runtime references:

```yaml
mode: DEBUG
source_runtimes:
  python@1.0: {executor: python, image: 'dacp/operator-python:1.0'}
  scala@1.0: {executor: jvm, image: 'dacp/operator-scala-debug:1.0'}
```

Runtime images must already exist in the execution node's Docker engine. The server resolves each image to its local immutable ID. Submitted code cannot select an image, install dependencies, or change the executor. Dependencies belong in the administrator's runtime image. Source and compilation products are temporary; compilation runs inside the same container isolation as execution: no network, read-only root filesystem, writable limited `/tmp`, memory and process limits. DEBUG mode is intended for trusted development clients authenticated with mTLS.

From `dacp-server`, start the supplied example with `sbt 'run ../examples/source-debug.yaml'`. Its port is 18815 and it publishes `/orders` in `data1`. Build the Python runtime using the instructions in [operator authoring](operator-authoring_en.md). To prepare Scala source compilation, first build the shared JVM runtime with `scripts/build-operator-image.sh`, then run `scripts/build-scala-source-runtime.sh` from the project root. The latter adds Scala compiler/reflect 2.13.16 and JDK compiler modules to a separate DEBUG image. A compiler is not required in ordinary JAR execution images.

Connect the shell to `dacp://localhost:18815/data1` using the example client certificates. Enter source with a heredoc:

```text
run --runtime python@1.0 --input source=/orders --params '{"output_format":"csv"}' <<END
def process(inputs, params):
    for chunk in inputs['source']:
        yield chunk.upper()
END
```

Source entry always uses a heredoc. There is no `--source FILE` or `--code` option. Choose an identifier as the delimiter; it must occur alone on the closing line. Indentation and newlines are preserved. Ctrl-C or EOF aborts entry without submitting. Source is limited to 512 KiB and the complete job request to 1 MiB.

The Python plugin calls `process(inputs, params)`. `inputs` maps each declared port to a streaming iterator. Byte views yield `bytes`; record views yield PyArrow `RecordBatch` objects. Consume inputs in lexical port order. Yield byte chunks or record batches for the single output port `result`; a stream must keep the same representation and record schema throughout. Empty output is represented as an empty byte stream. `params.output_format` supplies the output format label, defaulting to `binary`. Use `--view records` to request record inputs. `print` diagnostics go to stderr. The shell prints every result incrementally, using record tables, text for known text formats, or hex for other bytes. Ctrl-C closes the result stream and cancels work; an error after data is marked as partial output. No output dataset is saved.

Scala source is a complete compilation unit with a main class:

```text
run --runtime scala@1.0 --entrypoint Uppercase --input source=/orders <<END
// Paste the contents of examples/operators/inline/Uppercase.scala here.
END
```

The complete runnable [Scala example](../examples/operators/inline/Uppercase.scala) reads the named Arrow input from `/input/manifest.json` and writes framed output to stdout. The arguments are `operator /input /input/params.json`. Stdout is reserved for protocol frames; diagnostics belong on stderr. The header is JSON, preceded by its length and the payload length as two big-endian 32-bit integers. Emit `start`, zero or more `data` frames, then `end` for `result`. Records use one self-contained Arrow IPC stream per data frame and a base64 Arrow schema in `start`. Headers are limited to 64 KiB and payloads to 8 MiB. Compilation diagnostics go to stderr, and compilation failure fails the job. No dependency downloads occur during compilation.

Job YAML and SDK submissions use the same source representation:

```yaml
job: uppercase
tasks:
  - id: uppercase
    operator:
      runtime: python@1.0
      source: |
        def process(inputs, params):
            for chunk in inputs['source']:
                yield chunk.upper()
    inputs:
      source: {uri: 'dacp://localhost:18815/data1/orders', view: bytes}
    params: {output_format: csv}
```

Use `with Job.run(node, yaml_text, **credentials) as events:` and iterate events to consume the stream; leaving the context closes it. `run identity@1.1 --input source=/orders` continues to select a repository operator. An operator ID and inline source are mutually exclusive.

Launch plugins implement `OperatorExecutor.sourceCommand` and register through `META-INF/services/dacp.OperatorExecutor`, the same discovery path as artifact launchers. The default rejects source support; enabling DEBUG alone does not make an arbitrary plugin accept source. Administrators select plugin IDs in `source_runtimes`; adding a language requires no core registry or client changes.

Preview: `run` and `submit` accept `--preview-rows N` and `--preview-bytes N`, counted per port. Reaching either limit cancels the remaining whole job. The default prints the entire stream; records headers appear once and text lines carry port labels.
