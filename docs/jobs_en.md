# Streaming jobs

[中文](jobs.md)

DACP Server and Union share the same Job format. Steps connect through output streams referenced in `inputs`. List order does not define execution order. Control dependencies (`depends_on`) are not supported.

```yaml
name: weather-filter
tasks:
  - id: read
    title: Read weather observations
    operator: read_item@1.0
    inputs: {}
    params:
      uri: dacp://localhost:18861/weather/observations
      view: records
  - id: copy
    title: Filter valid observations
    operator: piflow_filter@1.0
    inputs:
      source: read
    params:
      condition: "quality = '0'"
outputs:
  result: copy
```

## Fields and stream connections

`name` is the required readable workflow name, separate from the generated execution ID. It must contain 1–256 characters, cannot be blank, and cannot contain control characters. Each task has a unique `id`, optional descriptive `title`, versioned `operator`, named `inputs`, and optional `params` and placement `node`. Input values and Job outputs use `task` or `task:port`; omitted ports mean `result`. Object bindings and connection-level `view` are not supported. Each output has at most one consumer. Use explicit duplication operators or separate ports for fan-out.

`read_item@1.0` is a built-in transport source requiring no operator container. It takes no inputs and requires `params.uri` addressing one Item and `params.view` set to `bytes` or `records`; its output is `result`. Zero-input generators also have empty inputs. Processing operators consume upstream outputs. Representation conversion is explicit.

On Union, use public Union dataset URIs. Omit `node` for automatic placement or use a logical node returned by `dacp.nodes`. Physical member addresses are not accepted in user submissions.

## Structure inference and sinks

Every operator must implement [structure inference](structure-inference_en.md). Output descriptors come from this method, not manifest output declarations. A sink may infer an empty output map and finish successfully without emitting frames. A Job may set `outputs: {}`; the client then waits for completion without receiving result data. Unselected branches still execute. Submission and execution do not invoke inference automatically; explicitly call `infer` during authoring when needed.

Upstream and downstream steps run concurrently with bounded buffering. Failure, cancellation and timeout cancel the attempt and release resources. Retries and materialization are configured in a separate [execution policy YAML](materialization_en.md). Retries can repeat external side effects; operators must provide appropriate idempotency.


See the [operator catalog](operator-catalog_en.md) for currently published collaboration operators, parameters and complete input/output examples.

## Shell

```text
submit job.yaml
submit job.yaml --vars variables.yaml
run piflow_filter@1.0 --input source=/observations --view records --params '{"condition":"CAST(quality AS INTEGER) = 0"}'
run piflow_csv_parser@1.0 --input source=/observations --view bytes
```

The shell converts Item inputs into `read_item` steps and prints results incrementally. It connects to either a node or Union using [TLS and a token](security_en.md). Repeat `--output PORT` to select multiple ports, or use `--no-output` for a sink. Inline heredoc source is DEBUG-only; see [source submission](operator-source_en.md).

## Workflow variables

Supply variables in a separate YAML file when submitting a workflow, without editing the template. Its root must be a mapping, for example `VAR_NAME: 123`; numbers, booleans and lists retain their types.

Write a complete value as `$VAR_NAME` in the workflow:

```yaml
params:
  limit: $VAR_NAME
  schema: $SCHEMA
```

The matching `variables.yaml` is:

```yaml
VAR_NAME: 123
SCHEMA:
  temperature: float64
```

In a shell already connected to the server, run:

```text
submit workflow.yaml --vars variables.yaml
submit workflow.yaml --vars variables.yaml --policy policy.yaml
```

Interactive input also works: run `submit --vars variables.yaml`, enter the workflow, and finish with `END` alone on a line. A complete conversion example is provided:

```text
submit examples/convert-variables.yaml --vars examples/convert-variables-values.yaml
```

Paths are relative to the shell's working directory. Change `INPUT_URI` in the variable file to an Item URI accessible to the target server and ensure the referenced operators are available.

Variable names are case-sensitive and match `[A-Za-z_][A-Za-z0-9_]*`. When a variable object is supplied, the client parses YAML and recursively substitutes complete string values, preserving numbers, booleans, strings, lists, mappings, and `null`, then submits the resolved workflow. Mapping keys are unchanged; `prefix-$VAR_NAME` is not interpolated; `$OTHER` inside an inserted value is not expanded again. Environment variables are not read and Shell commands are not executed. Missing variables or invalid variable files fail before submission; unused variables are allowed. Omitting `--vars` / the SDK `variables` argument preserves the existing submission behavior.

The Python SDK accepts variable dictionaries for both node and Union submissions:

```python
import yaml
from pathlib import Path
from dacp import Job

workflow = Path("workflow.yaml").read_text()
variables = yaml.safe_load(Path("variables.yaml").read_text())
with Job.run("dacp://localhost:18861", workflow, variables=variables) as result:
    for event in result:
        print(event)

# union is an already connected dacp.union.Union instance
execution = union.submit(workflow, variables=variables)
```

The Scala SDK uses the same rules:

```scala
import dacp.client.Job
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.{Files, Path}

val workflow = Files.readString(Path.of("workflow.yaml"))
val variables = dacp.union.YamlDocument.parse(Files.readString(Path.of("variables.yaml")))
val result = Job.run("dacp://localhost:18861", workflow, variables = variables)
try result.foreach(println) finally result.close()
// union is an already connected dacp.client.Union instance
val execution = union.submit(workflow, variables = variables)
```

The client reads the variable file; it need not exist on the node filesystem. The server records and executes the resolved workflow, so automatic retries and replays retain the original resolved values. Replays cannot override variables; submit a new workflow to use new values. Resolved values may appear in job history and dashboards, so do not put passwords or other secrets in variables.

Workflows, execution policies and variables use YAML and can be submitted together:

```text
submit workflow.yaml --policy policy.yaml --vars variables.yaml
```

Union subjobs retain the global job’s `name`. Node and Union dashboards share Name, Job ID, Source, Type, Status, Started at and Duration columns. Names lead the list, while details retain the full ID. Subjob definitions and execution policies display as YAML. YAML documents are limited to 1 MiB and reject duplicate keys, unsafe object tags and collection aliases. Quote textual numbers, dates and boolean labels.
