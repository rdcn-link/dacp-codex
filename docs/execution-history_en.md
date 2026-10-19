# Task execution records

DACP Server and Union Server persist execution metadata by default; configuration cannot disable it. Results remain single-consumption streams. Node records do not store result payloads, complete Job YAML, parameters, or submitted source code. Union records retain the complete Job YAML, including its parameters, to display the task script; result payloads are not stored.

## Users and operations

DACP Server defaults to `.dacp-executions/PORT` under its working directory. Override this in server YAML:

```yaml
execution_store:
  provider: file
  settings:
    directory: /var/lib/dacp/node-a/executions
```

U uses `state_directory/executions`. Running instances need separate directories; only one execution-record writer may own a directory. History metadata is independent of `retention_seconds` and is never automatically deleted. That setting only clears in-memory execution sessions, result queues, and status event windows. Administrators can back up the record directory.

Records include an independent `record_id`, type, submitter, node, task or execution correlation IDs, operator, input references, state transitions, start/end/update times, elapsed time, available per-port byte and record counters, and failure or cancellation reasons. Inline programs record only their runtime identifier. Error text is limited to 2048 characters and may contain program diagnostics. Ordinary DACP Jobs and their worker tasks have separate records correlated by `parent_execution_id`. Union global Jobs, node executions, and individual tasks have separate records correlated by their shared `execution_id` and `parent_record_id`.

From the corresponding module directory, query records using the local CLI, including while the service is running:

```bash
# DACP node
 target/universal/stage/bin/dacp-server history list /var/lib/dacp/node-a/executions
 target/universal/stage/bin/dacp-server history list /var/lib/dacp/node-a/executions 100 100
 target/universal/stage/bin/dacp-server history get /var/lib/dacp/node-a/executions RECORD_ID

# U
 target/universal/stage/bin/dacp-union-server history list ../.union-demo/x/executions
 target/universal/stage/bin/dacp-union-server history get ../.union-demo/x/executions JOB_ID
```

The Union HTTP dashboard's execution-history view paginates U's persistent jobs, task states, and traffic. `GET /api/history?offset=0&limit=100` queries history; `GET /api/jobs/ID` continues to return historical metadata after its in-memory session expires. The maximum page size is 1000; the website uses 100 records per page, with search and state filters applying to that page. History and dashboard queries are publicly readable. It cannot replay outputs, resume Jobs, or grant access to source data.

On restart, unfinished records become `interrupted`, with `interruption_detected_at_ms` recording detection time. Unknown actual completion times are not presented as certain: `finished_at_ms` and `elapsed_ms` use the last observation, while the event records when interruption was detected. Tasks cleaned up during a normal shutdown are recorded as cancelled or completed. Retaining history does not resume tasks.

## Developers

The shared implementation is `dacp-server/ExecutionHistory.scala`, reused by U. A record is written before execution, then updated on state transitions, Union progress observations, and completion. A single JSON document contains both its latest snapshot and ordered state events, avoiding separate commits. A temporary file is synchronized to disk, atomically replaces the record, and its parent directory is synchronized. Write failures fail the associated request rather than silently disabling recording. A directory writer lock prevents two instances from recovering each other's active executions; the CLI opens history read-only.

`TaskService` covers ordinary Flight Jobs and direct worker requests; `UnionWorker` records node executions and each task; `Coordinator` records global jobs. Record IDs are independent of user task names, so repeated runs never overwrite previous records. Late running-state updates cannot replace terminal outcomes. Pagination currently scans the directory and sorts by creation time, suitable for initial deployments. Database indexing, automatic archival, and remote aggregation of node history are unavailable. U persists its observed node status; each node's local history retains local task transitions.

Validate with `cd dacp-server && sbt test` and `cd dacp-union-server && sbt test`. After `sbt stage` in both modules and preparing the example certificates and `dacp/operator-python:1.0` image, run `python3 scripts/test-execution-history.py` and `python3 scripts/test-union.py` from the root to check actual Docker execution and history. Both scripts use temporary ports and directories and shut down the services they start.

The Union dashboard displays the submitted Job YAML and a DAG of datasets, tasks, port dependencies and final outputs. Union history retains the complete submitted Job YAML (including parameters) for this view; node audit records do not store it. The dashboard and query APIs expose this information publicly; server administrators manage the history files. Node health reports include JVM process start time (`started_at_ms`) and monotonic uptime (`uptime_ms`); unreachable nodes retain their last observation. Older records without saved source display an explicit unavailable message.

## Storage extension

Both nodes and Union accept the `execution_store` mapping above. `provider` selects a ServiceLoader-registered `ExecutionStoreProvider`; the implementation validates private `settings`. The default `file` implementation is in `plugins/dacp-plugin-execution-file`; `ExecutionHistory` retains state transitions, interrupted-execution recovery and pagination. Applications can construct `new ExecutionHistory(store, ownsStore = false)` to retain ownership. See [plugin registration](plugins_en.md#instance-environments-and-execution-storage).
