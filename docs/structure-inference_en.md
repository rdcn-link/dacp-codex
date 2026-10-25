# Operator and workflow structure inference

[中文](structure-inference.md)

Every operator must provide an inference method. Manifests declare input contracts only; `outputs` is rejected. The method supplies output ports, representations, formats, schemas, and explicit unknowns. Sinks return an empty mapping; sources accept empty inputs. Missing methods, malformed responses, and timeouts fail validation without a manifest fallback.

## Usage

Inspect structures before submitting the same workflow in the shell:

```text
infer workflow.yaml --vars variables.json
submit workflow.yaml --vars variables.json
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

Before execution, inference propagates through steps, checking compatible input representations, formats and known schemas, referenced output existence, and operator error diagnostics. Unknown structures propagate without being treated as validated. Immediately before starting each step, the method is called again using actual input stream headers; output ports, representations, formats and known schemas must satisfy the inferred contracts. Sinks need no artificial output port.

## Shared request and response

Programs receive `infer /input/inference.json`, read the request file, and write one JSON response to stdout. Logs go to stderr; no Arrow data frames are emitted.

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

`BytesOperator` and `RecordsOperator` implement inference. The records template reuses `output_schema` and preserves unknowns when the input schema is unknown. Custom multiport operators implement `infer_outputs(inputs, params)`, sharing it between editing and execution validation:

```python
from dacp_operator import descriptor, result, run_operator

def infer_outputs(inputs, params):
    if set(inputs) != {"source"}:
        raise ValueError("Expected source input")
    return result({"result": descriptor(["bytes"], ["text"], certainty="known")})

def process(ctx):
    out = ctx.bytes_output("result", "text")
    for chunk in ctx.input("source").bytes():
        out.write(chunk.upper())

if __name__ == "__main__":
    run_operator(process, infer_outputs)
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

The native `command` executor's adapter method infers bytes ports from stdin/stdout configuration without starting the executable. External executor plugins implement `inferenceCommand` (`sourceInferenceCommand` for source submissions) using the same JSON protocol, without core registry changes. Manifests and plugin settings must not establish a separate output schema source.

## Isolation and limits

Inference methods perform metadata computation only, without reading actual data, contacting external services, or causing side effects. The server invokes them in a networkless container with a read-only root filesystem, limited to 15 seconds, 512 MiB memory, 64 processes, and one CPU. Requests and responses are each limited to 1 MiB. Only inference requests and read-only operator artifacts are mounted, never datasets. The core `read_item` transport operator queries Flight metadata without opening content streams; dataset plugins should also avoid scanning content when describing Items.

Rebuild affected runtime environments and operator packages after SDK/runtime updates. Publication does not execute user code in the repository process; execution nodes verify inference capability by invoking it. Remove old manifest output declarations directly; there is no compatibility path.

Contract tests: `sbt test` in `dacp-operator-sdk` and `dacp-server`, plus `.venv/bin/python scripts/test-operator-inference.py` from the repository root.
