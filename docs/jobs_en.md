# Streaming jobs and control dependencies

[中文](jobs.md)

DACP Server and Union share one Job format. `tasks` declares steps, `inputs` connects only other steps' output streams, `depends_on` declares execution prerequisites, and Job `outputs` selects streams returned to the client. List order does not define execution order.

```yaml
job: weather-ingestion
tasks:
  - id: prepare
    title: Initialize the target table
    operator: prepare_weather_table@1.0
    inputs: {}
  - id: read
    title: Read observation records
    operator: read_item@1.0
    inputs: {}
    params:
      uri: dacp://localhost:18861/weather/observations
      view: records
  - id: write
    title: Write observations incrementally
    operator: write_weather_table@1.0
    inputs:
      source: read
    depends_on: [prepare]
outputs:
  summary: write:summary
```

`prepare_weather_table` and `write_weather_table` are illustrative business operators that must be implemented and published before use. The former may declare `outputs: {}`; the latter returns a final write summary on `summary`. Ordinary Union clients must use public Union dataset names in read URIs, omitting `node` or using logical node names returned by `dacp.nodes`. Administrators may specify actual member addresses.

## Stream connections

Input values must be strings, either `task` or `task:port`; an omitted port means `result`. Job outputs use the same syntax. A missing default port is an error; no other port is selected. `{uri, view}` and `{task, port}` objects and connection-level `view` are not supported.

Source operators declare `inputs: {}` and read external data or generate data themselves. Processing steps consume other steps' outputs. Stream metadata identifies the actual representation and schema, checked against the receiving operator's contract; bytes/records conversion is never implicit. Each output has at most one consumer and cannot feed both another task and the client. Duplication requires an explicit operator or separate output ports.

## Built-in Item source

`read_item@1.0` is a built-in server transport operator requiring no downloaded container. Its `params` must contain exactly the string values `uri` and `view`. The URI addresses one Item, and `view` is `records` or `bytes`. Inputs are empty; the output is `result`. Reads use public Dataset/Item plugin interfaces, preserving structure, backpressure, cancellation and resource cleanup without materializing the complete input. Read targets participate in authorization and data-local placement; parameters cannot bypass member grants or task input prefixes.

```text
run identity@1.1 --input source=/observations --view records
```

The shell generates `read_item` steps and connects `run` inputs to them. `--view` selects the Item representation only; generated connections carry no `view`. Zero-input generators still support `run live_ticks@1.0` directly.

Shell receives `result` by default; repeat `--output PORT` for multiple outputs, or use `--no-output` for a sink.

## Control dependencies and sinks

Data connections allow upstream and downstream steps to run concurrently. `depends_on: [prepare]` instead waits for successful predecessor termination, never treating the last frame or output EOS as proof of success. Local execution uses task state; cross-node execution uses certificate-pinned, consumer-authorized Flight status requests without manufacturing a data stream. Intermediate data still travels directly between nodes rather than through Union.

Both dependency types participate in unknown-reference, duplicate-reference and cycle validation. A control predecessor cannot also lie on the task's streaming ancestor path: backpressure could deadlock execution. Split the work and use explicit materialization/replay boundaries instead. DAG solid edges represent streams, and purple dashed edges represent control dependencies.

Operators may declare `outputs: {}`. Python/Scala SDKs allow successful termination with no output frames. Native commands may set `executor_config.stdin` or `stdout` to `null`, with the corresponding port declaration set to `{}`. Sinks, independent branches and control prerequisites execute even when not selected as final results. A Job may specify `outputs: {}`; the client waits for overall success or failure without receiving result data.

## Failures, retries and resources

Predecessor failure prevents downstream startup and cancels related tasks and streams in the execution attempt. Cancellation, timeouts and membership withdrawal retain their execution-policy semantics. Retries run a new attempt and recheck control dependencies. Materialization stores data outputs, not external side effects or database transactions. Retries may repeat initialization, writes or notifications; business operators must provide idempotency. Replaying a materialized step does not rerun predecessor work replaced by that replay.

Control dependencies and transport sources form execution boundaries rather than ordinary container operators to fuse. Memory queues, Flight and FIFOs preserve backpressure; logs, code caches and explicit materialization have separate storage purposes. See [materialization and replay](materialization_en.md) for execution policies.
