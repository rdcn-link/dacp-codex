# Submit streaming jobs to DACP

[中文](jobs_zh.md)

The shared YAML fields, port bindings, source steps and execution semantics are defined in the [XStream Job reference](https://github.com/rdcn-link/xstream-codex/blob/main/docs/jobs.md). This guide covers DACP Shell/SDK submission, Union integration and service credentials.

## Shell

```text
submit job.yaml
submit job.yaml --vars variables.yaml
run piflow_filter@1.0 --input source=/observations --view records --params '{"condition":"CAST(quality AS INTEGER) = 0"}'
run piflow_csv_parser@1.0 --input source=/observations --view bytes
```

The shell converts Item inputs into `read_item` steps and prints results incrementally. It connects to either a node or Union using [TLS and a token](security.md). Repeat `--output PORT` to select multiple ports, or use `--no-output` for a sink. Inline heredoc source is DEBUG-only; see [source submission](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-source.md).

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

When `read_item.view` is omitted, Item metadata selects records, then bytes, then the first available scientific view. An explicit choice is preserved and fails if unsupported.

## Item paths, remote services and SQL

```yaml
name: hot-weather-observations
tasks:
- id: read
  operator: read_item@1.0
  inputs: {}
  params:
    uri: context:/ds-6zmr-crvh/observations
    sql: |
      SELECT station_id, temperature
      FROM source
      WHERE temperature > 30
      LIMIT 100
outputs:
  result: read
```

For an explicitly addressed service, use `uri: dacp://remote.example:18861/weather/observations`. A `context:` URI refers to the DataCatalog bound in the receiving JVM; it is resolved before remote dispatch. Paths address the submission target’s public directory: a node resolves local names, while Union resolves its public names and schedules readers on the owning members. Authors need no internal member addresses.

SQL supports `SELECT *` or a column list, fixed `FROM source`, optional `WHERE` and `LIMIT`. Predicates support `= != <> < <= > >=`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT` and parentheses. Strings use single quotes (double an embedded quote); double quotes delimit column names. Only single-Item read-only queries are supported: no joins, aggregation, functions, ordering, multiple statements or mutations. SQL lowers to the shared `Scan` contract, executed by the dataset plugin rather than interpolated into database SQL. Plugins may push down supported operations and explicitly reject unsupported capabilities. Data remains batch-streamed, and projected schemas apply to inference and bounded samples.

Configure remote credentials on nodes that actually execute reads and separately on the designer. The target service’s token is never forwarded to another service; credentials are not Job parameters. Every member eligible to read an external service should have its required credentials.

```yaml
remote_item_credentials:
  "dacp://remote.example:18861": /etc/dacp/remote-reader.token
```

[Scientific view selections and samples](https://github.com/rdcn-link/xstream-codex/blob/main/docs/designer-scientific.md).
