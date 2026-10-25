# Write, publish and run an operator

Output structures come exclusively from the mandatory inference method; manifests declare input contracts only. See [structure inference](structure-inference_en.md).

An operator is a data-processing program: it receives named Item input streams and JSON business parameters, then returns named output streams. This guide starts with a complete Python workflow, followed by Scala, packaging, publication and the wire contract.

## Prefer operator templates

Use the [Scala / Python SDK](../dacp-operator-sdk/README_en.md) for new operators and implement only business transformations. The lower-level examples below remain useful when direct frame control is needed.

The Scala continuous byte-stream template integrates directly with Java streaming libraries; see [ByteStreamOperator and GZIP example](../dacp-operator-sdk/README_en.md#scala-continuous-byte-stream-template). Table streams use `RecordsOperator` to process Arrow batches.

## Concrete startup arguments

For example, `/input/params.json` contains `{"min_amount":20,"factor":2}`. The equivalent Python entry invocation is (the actual launcher also uses a loader wrapper):

```bash
/usr/local/bin/python /operator/filter_amount.py operator /input /input/params.json
```

`sys.argv[1:]` is `['operator', '/input', '/input/params.json']`. `operator` is a fixed protocol marker, followed by the streams directory and the parameter file path, not a JSON string. Scala `main(args)` receives the same three arguments, for example:

```bash
/opt/java/bin/java --add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true -cp '/operator/scala-identity_2.13-1.0.jar:/opt/lib/*' example.ScalaIdentity operator /input /input/params.json
```

## Multiple inputs and outputs

Complete example: [split_orders](../examples/operators/python/split_orders/1.0/split_orders.py), [port definition](../examples/operators/python/split_orders/1.0/operator.yaml), [job file](../examples/operators/python/split_orders/1.0/job.yaml). Both `left` / `right` inputs have `id:int64, amount:float64`. With `min_amount=20`, the supplied data produces ids 2 and 3 on `accepted`, and ids 1 and 4 on `rejected`.

The framework generates the manifest. Look up ports by name rather than hardcoding file numbers:

```json
[{"port":"left","file":"0.arrow","representation":"records","format":"csv"},{"port":"right","file":"1.arrow","representation":"records","format":"csv"}]
```

Shell binding example:

```text
run split_orders@1.0 --input left=/orders_a --input right=/orders_b --view records --output accepted --output rejected --params '{"min_amount":20}'
```

`--view` applies to all inputs; use job YAML for different representations per input. Each output may have its own schema. Declare every output before writing data; even empty outputs need start/end events. The SDK handles these frames. Consume inputs in lexical port-name order through EOS to avoid blocking from alternating FIFO reads. The shell returns the selected ports `accepted` / `rejected`; job YAML `outputs` can rename them to labels such as `qualified` / `other`. Each output supports only one consumer; duplication requires explicit separate ports. The DEBUG inline Python helper still has only `result`, and the native executor still has one bytes stdin/stdout pair.


## Distinguish data inputs, parameters and outputs

| Part | Example | How the program obtains it |
|---|---|---|
| Input port | `source` | Find its stream file in `/input/manifest.json` |
| Input Item | `/orders` in the current Dataset | The framework reads the Item and supplies Arrow IPC; the source filesystem path is not passed to the program |
| Input representation | `records` | The manifest's `representation`; read typed Arrow record batches |
| Business parameters | `{"min_amount":20,"factor":2}` | Read the JSON file identified by a launch argument, normally `/input/params.json` |
| Output port | `result` | Label stdout protocol frames with the port name |
| Output representation and format | `records` / `arrow` | Declare representation, format and schema in `start`, then send batches in `data` |

`--input` binds data; `--params` supplies business settings. Port names come from the operator definition: `source` and `result` are choices in this guide, not universal fixed names. Each port carries one Item stream; a program may declare several ports.

`representation` determines how content is read: `records` is a typed table; `bytes` is file/binary content. `format` describes content, such as `csv`, `parquet`, `text` or `arrow`. One CSV Item may expose original bytes and parsed records. Records always travel as Arrow IPC, not CSV text. **Shell `run` defaults to bytes; this table operator requires `--view records`.**

## Example: filter orders and add a calculated column

The complete program is [filter_amount.py](../examples/operators/python/filter_amount/1.0/filter_amount.py), accompanied by its [definition](../examples/operators/python/filter_amount/1.0/operator.yaml), [input](../examples/operators/python/filter_amount/1.0/orders.csv) and [job](../examples/operators/python/filter_amount/1.0/job.yaml).

Input CSV:

```csv
id,amount
1,12.5
2,30.0
3,60.0
```

The example requires `id: int64` and `amount: float64`. `min_amount` sets the threshold and `factor` sets the multiplier:

```json
{"min_amount": 20, "factor": 2}
```

Keep `amount >= min_amount`, then output `id`, `amount` and `scaled_amount = amount * factor`:

| id | amount | scaled_amount |
|---|---|---|
| 2 | 30.0 | 60.0 |
| 3 | 60.0 | 120.0 |

Both parameters are optional, defaulting to `0.0` and `1.0`. Values must be finite numbers; booleans, strings and unknown parameters are rejected. Null amounts do not match. No matching rows still produce the three-column schema and a normal end. Calculations use float64, not fixed-point financial arithmetic.

## Step 1: define the ports

```yaml
name: filter_amount
version: '1.0'
description: Filter amount >= min_amount and append scaled_amount = amount * factor.
image: dacp/operator-python:1.0
executor: python
entrypoint: filter_amount
artifact: filter_amount.py
inputs:
  source:
    representations: [records]
    formats: []
```

`source` accepts records only. `formats: []` imposes no input-format restriction; the program still validates columns and types. `result` emits records labeled arrow. Port definitions may also contain an Arrow schema JSON value; see [port contracts](reference_en.md#task-execution-and-operators). Business parameters are validated by the program; `params` is not a parameter-schema field in this definition.

## Step 2: read parameters and input batches

The launcher passes `operator /input /input/params.json`. In Python, `sys.argv[1]` is `operator`, `sys.argv[2]` is the input directory, and `sys.argv[3]` is the parameters file. Business JSON is not passed directly as a positional argument.

The framework-generated manifest looks like this. `file` is framework-assigned; do not hardcode it:

```json
[
  {"port":"source","file":"source.arrow","representation":"records","format":"csv"}
]
```

At runtime the file is a FIFO containing Arrow IPC. Read it as follows:

```python
params = read_params(Path(sys.argv[3]))
directory = Path(sys.argv[2])
manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
item = manifest[0]  # First validate one records input named source in this example.
with (directory / item['file']).open('rb') as raw:
    with pa.ipc.open_stream(raw) as reader:
        for batch in reader:
            selected = batch.filter(
                pc.greater_equal(batch.column('amount'), params['min_amount']))
```

Imports, `read_params` and complete validation are in the [full program](../examples/operators/python/filter_amount/1.0/filter_amount.py). Process batches without collecting the entire Dataset. Stop at Arrow EOS rather than waiting for operating-system FIFO EOF. Consume multiple inputs in lexical port order, for example `left` before `right`.

## Step 3: declare and send the output

Output is not `print(table)` or plain CSV on stdout. Python/Scala operators write protocol frames on stdout and diagnostic text on stderr. Determine the records schema before starting:

```python
OUTPUT_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('amount', pa.float64()),
    ('scaled_amount', pa.float64()),
])
```

The program sends `start`, batch `data` events and finally `end`:

```python
emit('start', representation='records', format='arrow',
     schema=base64.b64encode(OUTPUT_SCHEMA.serialize().to_pybytes()).decode('ascii'))

# selected contains the retained rows in this batch; skip data for empty batches.
output = pa.record_batch([
    selected.column('id'), selected.column('amount'),
    pc.multiply(selected.column('amount'), params['factor']),
], schema=OUTPUT_SCHEMA)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, OUTPUT_SCHEMA) as writer:
    writer.write_batch(output)
emit('data', sink.getvalue().to_pybytes())

# Run once after all input and output batches finish.
emit('end')
```

The example file contains the complete `emit` implementation. It prefixes headers and payloads with lengths and labels every frame `port: result`. Each records data payload is one independent Arrow IPC stream containing exactly one batch. It is neither JSON lines nor an arbitrary concatenation of IPC streams treated as one stream. Output batch schemas must match start.

The example slices input batches into at most 512 rows before generating output frames. Empty output sends start/end only. The framework reports successful task completion after the process exits successfully and all ports terminate correctly; the operator does not send the final `complete` event itself.

## Step 4: validate locally, then publish

From the repository root, run the local check without starting DACP or Docker. The Python environment must already contain PyArrow:

```bash
python3 scripts/test-operator-authoring.py
```

This script substitutes ordinary Arrow IPC files for FIFOs and checks batches, parameters, nulls, expected results, output schema, empty output and invalid parameters. It validates the operator protocol, not container, Flight or authorization integration.

Build the shared runtime and publish:

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish filter_amount@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint filter_amount \
  --artifact examples/operators/python/filter_amount/1.0/filter_amount.py \
  --ports examples/operators/python/filter_amount/1.0/operator.yaml \
  --root .operator-catalog
```

Dependencies are preinstalled in the runtime, not fetched during execution. Versions are immutable; publish a new version when changing published code.

## Step 5: bind data and run

Publish the example CSV on a DACP node. Merge this fragment into the existing configuration and replace the file path with your absolute path. Listener, TLS and authorization configuration must remain complete:

```yaml
datasets:
  data1:
    provider: mixed
    items:
      /orders:
        file: /absolute/path/to/dacp-codex/examples/operators/python/filter_amount/1.0/orders.csv
        schema: {id: int64, amount: float64}
remote_operator_library_urls: [http://localhost:8820]
```

The repository must serve `.operator-catalog` over HTTP; its launch command is shown below. Connect the certificate-configured shell:

```bash
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

At the shell prompt:

```text
run filter_amount@1.0 --input source=/orders --view records --params '{"min_amount":20,"factor":2}'
```

The shell waits and displays the two result rows as batches arrive, then shows execution information and returns to its prompt. This job expresses the same binding and also allows separate views for multiple inputs:

```yaml
job: filter-amount-demo
tasks:
- id: read_filter_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://localhost:8815/data1/orders
    view: records
- id: filter
  title: Filter orders and calculate scaled amounts
  operator: filter_amount@1.0
  inputs:
    source: read_filter_source
  params:
    min_amount: 20
    factor: 2
outputs:
  result: filter
```

```text
submit examples/operators/python/filter_amount/1.0/job.yaml
```

`id` identifies dependency references; `title` describes the task for display. Top-level `outputs.result` exposes a task port to the client. All results are one-shot streams: they are not automatically retained or published as Datasets. Cancelling consumption cancels upstream work. Data already displayed is partial if execution later fails.

## What bytes output looks like

Use bytes for text, compressed files, images and other binary content. For example, uppercasing raw UTF-8 `hello\n` produces:

```text
HELLO
```

Its start header is `{"type":"start","port":"result","representation":"bytes","format":"text"}`. Data payloads are directly `b'HELLO\n'`, without another Arrow IPC wrapper. Input bytes still arrive through an Arrow IPC `data: binary` column, which differs from output frame payloads. The shell displays text incrementally and unknown binary as hex. Chunks need not align with lines or UTF-8 characters.

Native stdin/stdout programs can use the [command executor](native-commands_en.md), whose adapter handles frames. Python/JVM executors require the program to implement framing. Existing [Scala bytes identity](../examples/operators/scala/src/main/scala/example/ScalaIdentity.scala) and [Python identity](../examples/operators/python/identity_python/1.0/identity.py) programs provide starting points.

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
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
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

The shell waits and prints the result incrementally. No output dataset is saved. See [stream contracts](operator-streams_en.md) and [DEBUG heredoc source](operator-source_en.md).

## Upload to a remote repository

The public website supports search, review and downloads; publication uses the API or administrator CLI, not a web upload form. After local `publish` has generated `.operator-catalog`, obtain a publication token from the repository administrator and set `DACP_REPOSITORY_PUBLISH_TOKEN` in your environment. Then run:

```bash
python3 scripts/upload-operator.py filter_amount@1.0 \
  --catalog .operator-catalog --url https://repository.example.org
```

The uploader transfers the manifest and required runtime/code blobs, authenticating with the environment token. Remote publication requires HTTPS. An administrator may instead run the publication CLI on the repository host. See [token-authenticated publication](../dacp-operator-repository/README_en.md#token-authenticated-publication-api) and [source/review evidence](operator-provenance_en.md). Uploading source, license and build instructions helps reviewers assess the operator; it does not establish safety by itself.

## Protocol limits and troubleshooting

Frames contain two big-endian 32-bit lengths (JSON header and payload), followed by UTF-8 JSON and payload bytes. Headers are limited to 64 KiB and payloads to 8 MiB. Each port emits start, zero or more data events, then end. For multiple outputs, send every start before any data. Start may contain a base64 Arrow IPC schema message; records require it. Input manifests may also include schema, but validate the actual reader schema.

Common problems:

- Unsupported input representation: check `--view records`, port declarations and the Item's advertised views.
- Parameter errors: `--params` must be a JSON object; check spelling and types. The framework does not validate business parameters for your program.
- Frame parsing failures: remove `print`/logs from stdout; diagnostics belong on stderr.
- Schema mismatch: all batches must use the names, order and types declared by start.
- A program that never finishes: stop on Arrow stream-reader EOS instead of continuing to read the raw FIFO for EOF.
- Failure after end: end closes one port, but the process must still exit successfully. Nonzero exit means failure and displayed data is partial.

Container roots and inputs are read-only; scratch files belong in `/tmp`. Containers have no network and enforce memory, PID, temporary-storage and execution-time limits. Dependencies must be preinstalled. Reuse shared runtimes for small JAR/source/wheel releases; operators are not loaded into the server JVM. Repository operators run in NORMAL/DEBUG, while inline source requires DEBUG. New array/image representations require shared contracts; never silently flatten them into records.
