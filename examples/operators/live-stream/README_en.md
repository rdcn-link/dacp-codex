# Continuous output stream example

[中文](README.md)

`live_ticks@1.0` uses the Python SDK to emit one synthetic weather observation per second as JSONL, starting at sequence 1. Values are synthetic demonstrations. Its code is a few KB and shares the existing Python/PyArrow runtime. Input `source` is only a trigger: it is fully drained in chunks before generation, without buffering the whole input. Current Job contracts require at least one input.

## Observe in the shell

Run from the project root against existing demonstration node A:

```bash
.venv/bin/dacp-shell dacp://localhost:18861 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

```text
dacp-shell> use weather
dacp-shell> run live_ticks@1.0 --input source=/observations --params '{"interval_seconds":1,"max_records":0}' --timeout 300
```

Example output:

```json
{"sequence":1,"timestamp":"2026-10-23T10:30:00+00:00","station_id":"DEMO-001","temperature_c":20.5}
{"sequence":2,"timestamp":"2026-10-23T10:30:01+00:00","station_id":"DEMO-001","temperature_c":20.99}
```

Shell results have no preview limit by default and print as chunks arrive. Press Ctrl+C during execution to cancel and return to the prompt. `max_records: 0` generates continuously but remains subject to server and request timeouts; it is not a permanent job. `interval_seconds` ranges from 0.01 to 60 seconds, default 1. `max_records` is a nonnegative integer, default 0; set it to 5 to finish automatically. No background scheduler accumulates pending records; backpressure blocks output writes.

For Union, select a public ID through `datasets` / `use`, and ensure a member grants `live_ticks@1.0`. Direct local node execution needs no Union approval.

## Build, publish and validate

Building requires a local catalog containing the `piflow_csv_parser@1.0` manifest and runtime blob to reuse its Python environment. Publication uses token-authenticated APIs without a Web upload interface:

```bash
python3 examples/operators/live-stream/test.py
python3 examples/operators/live-stream/build.py --runtime-catalog .operator-catalog --output /tmp/live-ticks-catalog
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
python3 scripts/upload-operator.py live_ticks@1.0 --catalog /tmp/live-ticks-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

The source archive includes code, build script, tests and bilingual instructions. No additional third-party dependencies are introduced. Validate streaming by recording first-output and completion times, canceling continuous generation, and checking released task slots. An animated graph or small output file alone does not prove streaming. Operators can implement blocking algorithms; stream interfaces do not guarantee immediate output from every algorithm.
