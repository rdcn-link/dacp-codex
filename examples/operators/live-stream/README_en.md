# Continuous output stream example

[中文](README.md)

`live_ticks@1.0` uses the Python SDK to emit one synthetic weather observation per second as JSONL, starting at sequence 1. Values are synthetic demonstrations. Its code is a few KB and shares the existing Python/PyArrow runtime. This zero-input source declares `inputs: {}` and starts generating immediately without reading an Item.

## Port declaration

Both the operator manifest and job have empty inputs; `result` is the only output port:

```yaml
inputs: {}
outputs:
  result:
    representations: [bytes]
    formats: [ndjson]
```

## Observe in the shell

Run from the project root against existing demonstration node A:

```bash
.venv/bin/dacp-shell dacp://localhost:18861 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

```text
dacp-shell> use weather
dacp-shell> run live_ticks@1.0 --params '{"interval_seconds":1,"max_records":0}' --timeout 300
```

Example output:

```json
{"sequence":1,"timestamp":"2026-10-23T10:30:00+00:00","station_id":"DEMO-001","temperature_c":20.5}
{"sequence":2,"timestamp":"2026-10-23T10:30:01+00:00","station_id":"DEMO-001","temperature_c":20.99}
```

Shell results have no preview limit by default and print as chunks arrive. Press Ctrl+C during execution to cancel and return to the prompt. `max_records: 0` generates continuously but remains subject to server and request timeouts; it is not a permanent job. `interval_seconds` ranges from 0.01 to 60 seconds, default 1. `max_records` is a nonnegative integer, default 0; set it to 5 to finish automatically. No background scheduler accumulates pending records; backpressure blocks output writes.

For Union, select a public name through `datasets` / `use`, and ensure a member can resolve and execute `live_ticks@1.0`. Local nodes can execute directly.

## Build, publish and validate

Building requires a local catalog containing the `piflow_csv_parser@1.0` manifest and runtime blob to reuse its Python environment. Publication uses token-authenticated APIs without a Web upload interface:

```bash
python3 examples/operators/live-stream/test.py
python3 examples/operators/live-stream/build.py --runtime-catalog .operator-catalog --output /tmp/live-ticks-catalog
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
python3 scripts/upload-operator.py live_ticks@1.0 --catalog /tmp/live-ticks-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

The source archive includes code, build script, tests and bilingual instructions. No additional third-party dependencies are introduced. Validate streaming by recording first-output and completion times, canceling continuous generation, and checking released task slots. An animated graph or small output file alone does not prove streaming. Operators can implement blocking algorithms; stream interfaces do not guarantee immediate output from every algorithm.

## Multi-step streaming workflow

[workflow.yaml](workflow.yaml) runs `live_ticks → live_ticks_decode → piflow_select_field → piflow_convert_schema → piflow_csv_save`, converting JSONL bytes into Arrow records, projecting and renaming columns, then emitting CSV. Every step processes individual rows or batches without waiting for infinite input to finish. `live_ticks_decode` uses an explicit weather record schema and preserves observations; it is not a generic JSON flattener. Each line is limited to 64 KiB.

[stream-only-policy.json](stream-only-policy.json) forbids intermediate output materialization and sets a 300-second overall timeout. Payloads travel through bounded memory queues, FIFO pipes and process stdout, without intermediate result files. Execution logs, status records, runtime caches and startup parameter files still use the server filesystem. This example guarantees no intermediate data payload staging to disk.

Start the shell from the project root, connect to node A, and select a Dataset before submitting:

```text
use weather
submit examples/operators/live-stream/workflow.yaml --policy examples/operators/live-stream/stream-only-policy.json
```

The terminal continuously prints CSV rows with `sequence,station_id,timestamp,air_temperature_c`. Press Ctrl+C to stop. The generator needs no input Item. The workflow omits node placement and runs on the connected node. For Union, members must be able to resolve all five operator versions; the scheduler selects execution nodes.

Build and publish the decoder:

```bash
python3 examples/operators/live-stream/build.py --operator live_ticks_decode --runtime-catalog .operator-catalog --output /tmp/live-ticks-decode-catalog
python3 scripts/upload-operator.py live_ticks_decode@1.0 --catalog /tmp/live-ticks-decode-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```
