# Operator and workflow structure inference

[中文](structure-inference_zh.md)

Every operator must provide an inference method. Manifests declare input and static parameter contracts; `outputs` is rejected. The method supplies output ports, representations, formats, schemas, and explicit unknowns. Sinks return an empty mapping; sources accept empty inputs. Missing methods, malformed responses, and timeouts fail validation without a manifest fallback.

## Usage

Inspect structures before submitting the same workflow in the shell:

```text
infer workflow.yaml --vars variables.yaml
submit workflow.yaml --vars variables.yaml
```

`infer` queries metadata without processing or previewing actual data. Nodes and Unions expose Flight actions `dacp.infer` (one operator) and `dacp.infer_job` (a complete workflow). Ordinary clients use their server connection credentials. Unions request inference from the selected execution members; public reports contain task names and structures without internal member addresses.

Python:

```python
from dacp import Job
report = Job.infer("dacp://localhost:18861", workflow_text, variables={"LIMIT": 123})
# An already connected Union SDK instance:
report = union.infer(workflow_text, variables={"LIMIT": 123})
```

Scala:

```scala
val report = dacp.client.Job.infer("dacp://localhost:18861", workflowText)
val unionReport = union.infer(workflowText)
```

Structure inference runs only when explicitly requested with `infer`, for workflow authoring, validation, and preview. Submission, scheduling, retries, replay, and step execution do not invoke inference automatically; submitted workflows are assumed valid. Execution uses representations, formats, and schemas from actual stream headers, retaining protocol integrity, connected-port, authorization, and resource checks. Sinks need no artificial output port.

## Shared request and response

One-shot command-line inference uses `infer /input/inference.json`, reads the request file, and writes one JSON response to stdout. Servers use the persistent worker protocol below. Logs go to stderr; no Arrow data frames are emitted.

Request:

```json
{
  "inputs": {
    "source": {"representations": ["bytes"], "formats": ["text"], "certainty": "known"}
  },
  "params": {"uppercase": true}
}
```

Response:

```json
{
  "outputs": {
    "result": {"representations": ["bytes"], "formats": ["text"], "certainty": "known"}
  },
  "diagnostics": []
}
```

`certainty` must be `known` or `unknown`. Empty `representations` means an unknown representation; empty `formats` means an unknown format. Output port names must always be enumerated. Known records require `schema`, a base64 Arrow IPC schema message matching stream headers. An empty schema must not stand in for an unknown one. An optional `structure` object carries domain structure, such as multidimensional array dimensions, without flattening scientific data into tables. The current execution protocol still supports only bytes/records; new representations also require execution protocol support.

Diagnostics take the form `{"severity":"error","code":"unknown_column","message":"Unknown column: temperature"}`, or use `warning`. Errors block execution; warnings do not. Manifest input contracts still determine whether an operator accepts an incoming stream.

## Python authors

`BytesOperator` and `RecordsOperator` implement inference. The records template reuses `output_schema` and preserves unknowns when the input schema is unknown. Custom multiport operators implement `infer_outputs(inputs, params)`, for explicit use during editing:

```python
from dacp_operator import descriptor, result, run_operator, preview_process

def infer_outputs(inputs, params):
    if set(inputs) != {"source"}:
        raise ValueError("Expected source input")
    return result({"result": descriptor(["bytes"], ["text"], certainty="known")})

def process(ctx):
    out = ctx.bytes_output("result", "text")
    for chunk in ctx.input("source").bytes():
        out.write(chunk.upper())

def preview_outputs(inputs, params, limits):
    return preview_process(process, inputs, params, limits)

if __name__ == "__main__":
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)
```

`descriptor(..., schema=pyarrow_schema, certainty="known")` serializes Arrow schemas; `records_schema(input_descriptor)` reads an input schema. Do not start business processing during module initialization or inference.

## Scala authors

Extending `Operator` requires the method; byte and records templates already implement it:

```scala
import dacp.{StreamDescriptor,InferenceResult}
import dacp.operator.{Operator,OperatorContext}
import com.fasterxml.jackson.databind.JsonNode

object Uppercase extends Operator {
  def inferOutputs(inputs: Map[String, StreamDescriptor], params: JsonNode): InferenceResult = {
    require(inputs.keySet == Set("source"), "Expected source input")
    InferenceResult(Map("result" -> StreamDescriptor(Set("bytes"), Set("text"), certainty="known")))
  }
  def process(ctx: OperatorContext): Unit = {
    val out = ctx.bytesOutput("result", "text")
    ctx.input("source").foreachBytes(chunk => out.write(new String(chunk, "UTF-8").toUpperCase.getBytes("UTF-8")))
  }
}
```

The native `command` executor's adapter method infers bytes ports from stdin/stdout configuration without starting the executable. External executor plugins implement `inferenceWorkerCommand` (`sourceInferenceWorkerCommand` for source submissions) using the same JSON protocol, without core registry changes. Manifests and plugin settings must not establish a separate output schema source.

## Isolation and limits

Inference methods perform metadata computation only, without reading actual data, contacting external services, or causing side effects. The server invokes them in isolated worker containers with no network and a read-only root filesystem. Each worker is limited to 512 MiB memory, 64 processes, and one CPU. Requests and responses are each limited to 1 MiB. Only metadata pipes, worker adapter code, and read-only operator artifacts are mounted, never datasets or credentials. The core `read_item` transport operator queries Flight metadata without opening content streams; dataset plugins should also avoid scanning content when describing Items.

Rebuild affected runtime environments and operator packages after SDK/runtime updates. Publication does not execute user code in the repository process; execution nodes verify inference capability by invoking it. Remove old manifest output declarations directly; there is no compatibility path.

Contract tests: `sbt test` in `dacp-operator-sdk` and `dacp-server`, plus `.venv/bin/python scripts/test-operator-inference.py` from the repository root.

## On-demand startup and idle reclamation

Nodes start workers on demand for explicit inference. Requests reuse a worker only within the same node instance, caller identity, operator artifact, runtime image, and launch configuration. Parameters and input structures are excluded from this identity: every request invokes inference again, without a result cache. Different callers, artifact contents, or images never share a worker. Python modules or wheels initialize/install once; JVMs start and load once. SDK template instances are reused inside the worker. Inference methods must be deterministic and independent of previous requests.

Top-level node configuration:

```yaml
inference_workers:
  max_workers: 4
  idle_seconds: 60
  timeout_seconds: 60
```

These are the defaults. Integer ranges are 1–64 for `max_workers`, 1–86400 for `idle_seconds`, and 1–120 for `timeout_seconds`. The deadline includes queueing, cold startup, and computation; an enclosing Flight call may have a shorter deadline. Each worker handles requests serially. At capacity, the least recently used idle worker is evicted, or the request waits for capacity. A one-second reaper destroys workers past their idle deadline. Exceptions, malformed responses, timeouts, or cancellation during an active request destroy its worker; the next explicit request starts a new one. The failed inference is not automatically retried. Cancelling a queued request does not destroy a worker serving another request. Node shutdown cleans up all workers.

This reuses runtime resources, never task execution containers. Submission, scheduling, execution, retries, and replay still do not invoke inference automatically. Union forwards explicit inference to members; members manage the workers, and Union never loads their operator code.

## Worker protocol for executor plugins

External executors register through `META-INF/services/dacp.OperatorExecutor`, with JARs and dependencies installed in `plugin_directory`. Implement `inferenceWorkerCommand(program, artifactPath)` and, for source support, `sourceInferenceWorkerCommand(program, artifactPath)` without core registry or client changes. The default implementation replaces ordinary launch arguments with:

```text
infer-worker /input/requests.fifo /input/responses.fifo
```

Open the request FIFO for reading and the response FIFO for writing. First write a big-endian 32-bit zero to the response pipe to signal readiness. Then repeatedly read a big-endian 32-bit length followed by UTF-8 JSON, and return a JSON inference result with the same framing. Lengths must be 1–1048576 bytes. Request and result JSON follow the shared protocol above. Keep the process and loaded operator alive between requests. EOF between requests ends the worker; truncation, oversized frames, method exceptions, or invalid results terminate it with stderr diagnostics. Never put logs in the metadata pipes or send/receive actual data streams.

The Java/Scala SDK's `OperatorRunner.infer` and `Operator.main`, and Python's `run_inference`, `run_operator`, and `Operator.main`, support this mode. Built-in JVM, Python, and native command executors supply adapters and the current worker SDK. Source compilation and wheel installation happen only during worker initialization. Operators with custom container entrypoints must include a worker-capable SDK or implement this protocol. The native adapter infers structure without launching the executable.

Verification: `InferenceWorkerPoolSpec` covers reuse, capacity, idle reclamation, recovery, cancellation, timeout, and shutdown. SDK `InferenceSpec` and `scripts/test-operator-inference.py` cover multiple requests and framing limits. `scripts/test-inference-workers.py` uses real containers and Flight to verify Python/JVM process reuse with changing parameters, idle reclamation, and failure recovery.

Parameter types are declared statically in [parameter contracts](operator-parameters.md); reading them does not start an inference worker.

The online designer uses the shared `dacp-operator-inference` module to start local workers and resolve operators from its configured repository. Ordinary operators are inferred without Union or member-node inference calls; `read_item` only queries Item metadata from the configured target. Nodes and the designer share operator resolution, worker pools, and graph structure propagation.

## Bounded output sample interface

In addition to structure inference, operators must implement JVM `previewOutputs(inputs, params, limits)` or Python `preview_outputs(inputs, params, limits)`. Return a mapping from output port names to samples; omitted ports report samples unavailable. Input descriptors may carry upstream samples. Process only these bounded samples or construct domain examples; do not invoke the complete workflow's `process`. Operators accessing external services or producing side effects must provide a safe sample implementation rather than running production operations for previews.

Only sample requests add:

```yaml
preview:
  max_rows: 10
  max_bytes: 16384
```

`max_rows` ranges from 1 to 100. `max_bytes` ranges from 256 to 65536, counting the entire sample's serialized UTF-8 JSON, including fields, escapes, and Base64 overhead. Both limits apply. Hosts enforce limits again after SDK truncation. Samples total at most 64 KiB per operator response and 256 KiB per designer graph. An oversized row is discarded whole, preserving field structures. `truncated` explicitly indicates budget truncation. Sample failures produce `sample_unavailable` warnings while preserving successfully inferred Schema.

Each output descriptor may contain a `sample`:

```yaml
sample:
  representation: records
  format: json
  rows:
    - station_id: ST001
      temperature: 21.5
  truncated: false
```

Byte samples contain `representation: bytes`, `format`, `data_base64`, and `truncated`, without `rows`. Samples must use representations supported by the output port. Samples come from operator interfaces, never browser-generated Schema examples. Ordinary inference does not open data streams. The designer displays structures incrementally, then requests samples; built-in `read_item` uses row-limited records reads or bounded bytes reads and closes source streams promptly.

Python example (identity conversion, registering the required module function):

```python
from dacp_operator import bounded_sample, run_operator

def preview_outputs(inputs, params, limits):
    sample = inputs['source'].get('sample')
    return {'result': bounded_sample(sample, limits)} if sample else {}

# process / infer_outputs are the operator's processing and inference methods.
if __name__ == '__main__':
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)
```

Python `Operator` subclasses override `preview_outputs`; `main()` registers it automatically. Construct samples with `sample_records(rows)` or `sample_bytes(data, format='binary')`, bounding generation according to `limits`.

Scala example (add to an `Operator` or template subclass):

```scala
import dacp.{StreamDescriptor, PreviewLimits, OutputSample}
import com.fasterxml.jackson.databind.JsonNode

override def previewOutputs(
    inputs: Map[String, StreamDescriptor],
    params: JsonNode,
    limits: PreviewLimits
): Map[String, OutputSample] =
  inputs.get("source").flatMap(_.sample)
    .map(s => Map("result" -> s.bounded(limits))).getOrElse(Map.empty)
```

JVM `Operator.main` registers this method automatically; Java can implement the same JVM interface. Inference workers reuse their existing lifecycle. Samples never enter execution result streams or workflow YAML.

## Schema type names in the interface

Designer step cards and the right-hand Outputs tab use the same type names. These names are for display only: inference APIs and streams retain the complete Arrow Schema without converting data to strings. Complete schema in Outputs shows the original schema, including dictionary encoding and field metadata.

| Type | Display name |
|---|---|
| Boolean | `bool` |
| Signed integer | `int8`, `int16`, `int32`, `int64` |
| Unsigned integer | `uint8`, `uint16`, `uint32`, `uint64` |
| Floating point | `float16`, `float32`, `float64` |
| Decimal | `decimal128(precision, scale)`, `decimal256(precision, scale)` |
| String | `string`, `large_string`, `string_view` |
| Binary | `binary`, `large_binary`, `binary_view`, `fixed_binary[N]` |
| Date | `date32` (days), `date64` (milliseconds) |
| Time | `time32(unit)`, `time64(unit)` |
| Timestamp | `timestamp(unit)`, `timestamp(unit, timezone)` |
| Duration and interval | `duration(unit)`, `interval(unit)` |
| List | `list<T>`, `large_list<T>`, `fixed_list<T>[N]`, `list_view<T>`, `large_list_view<T>` |
| Structure | `struct<field: type, …>` |
| Map | `map<K, V>`, with `[sorted]` when keys are sorted |
| Union | `union[dense or sparse]<type ID: field: type, …>` |
| Run-end encoding | `run_end_encoded<run-end type, value type>` |
| Null | `null` |

Time units are `s`, `ms`, `us` and `ns`; intervals retain units such as `year_month`, `day_time` and `month_day_nano`. A trailing `?` denotes a nullable field, for example `temperature: float64?`; `list<string?>` allows null list elements. Unknown structures display `Schema unknown` and unknown types display `unknown`; neither means `null`. Unrecognized types retain their names and can be inspected in the complete schema rather than being mislabeled as an existing type.

For example, Arrow `utf8` displays as `string`, and `floatingpoint` with `DOUBLE` precision displays as `float64`. Nested fields retain their hierarchy. Display support does not imply that every operator can process that type; operator and dataset plugins determine processing capabilities.

### Required sample interface and existing operators

Every operator must provide processing, structure inference and output preview interfaces. JVM `Operator.previewOutputs` is abstract. Python function-style entrypoints must register `preview_outputs`; missing registration fails explicitly, and custom `Operator` subclasses must implement the method. Pure byte templates can reuse their transforms for samples; Python records templates can also process bounded samples. Without `preview` in a request, the sample method is not invoked. Report specific reasons such as missing input samples rather than presenting an unchanged input sample as transformed output.

Side-effect-free Python operators may explicitly use the SDK's `preview_process` helper:

```python
from dacp_operator import preview_process, run_operator

def preview_outputs(inputs, params, limits):
    return preview_process(process, inputs, params, limits)

if __name__ == "__main__":
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)
```

The helper provides only bounded in-memory input samples, parameters and output collectors, without production streams, directories or remote access. Collection stops when row or byte limits are reached. It is not a sandbox for arbitrary code: use it only for transforms the author has verified are side-effect-free. When the inferred Schema is unknown, received sample values may define an in-memory table solely for preview processing; this does not publish those types as a known inferred Schema. Operators accessing external systems need a dedicated safe preview implementation.

All six operators in the current repository implement `preview_outputs`:

| Operator | Sample behavior |
|---|---|
| `piflow_csv_parser` | Parse upstream CSV byte samples into records |
| `piflow_filter` | Apply the predicate to upstream sample records |
| `piflow_join` | Join bounded left and right sample records |
| `piflow_merge` | Merge both samples until the limit is reached |
| `piflow_execute_sql_stop` | Run a read-only sample query in in-memory SQLite |
| `piflow_csv_save` | Serialize sample records to CSV bytes without writing files |

Samples do not represent complete-data statistics; for example, a join may return no sample rows when the two samples have no matching keys. Defaults are 10 rows and 16 KiB, with serialized sizes bounded again by SDK and host. Run `./.venv/bin/python scripts/test-operator-previews.py` to verify all six implementations, size limits and sample propagation with unknown Schemas, without Docker.
