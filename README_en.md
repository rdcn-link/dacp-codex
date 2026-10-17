# dacp MVP

<img src="assets/dacp-logo.png" alt="DACP logo: connected nodes forming a C for Collaborative" width="480">

[中文](README.md)

An Arrow Flight data collaboration protocol for autonomous data nodes. It uses a Scala 2.13 server, a Python/PyArrow SDK, protobuf metadata, YAML DAGs, and one Docker container per task. Nodes require neither a cluster nor a central registry. Directories and JDBC SQL databases can both be exposed as datasets, with SQL tables represented as a single level of Items.

## Quick start

Requires JDK 17, sbt 1.10.x, and Python 3.10+. Container tasks also require Linux, `mkfifo`, and a local Docker Engine. The user running the service must have access to the Docker socket. The examples use the following working directories.

```bash
cd dacp-codex
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt run
```

In another terminal, run from the project root:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python examples/client.py
```

The output includes `events.json`, `nested/orders.csv`, and `pixel.png`, two CSV records, and the PNG byte count. The TLS certificates are for local examples only. The script generates CA, server, and client certificates. The server certificate also permits clientAuth so the local task executor can connect to other nodes that trust the same CA.

```python
from dacp import Source

with Source.open("dacp://localhost:8815/data1") as ds:
    print(ds.list())
    for batch in ds.open("nested/orders.csv").as_records():
        print(batch.to_pydict())
    with ds.open("pixel.png").as_bytes() as stream:
        header = stream.read(8)
```

The default URI port is 8815. An Item can also be opened directly: `Source.open("dacp://localhost:8815/data1/nested/orders.csv")`. The three certificates can instead be passed to `Source.open` as bytes through `tls_root_certs`, `cert_chain`, and `private_key`. Datasets and directly opened Streams support context managers.

## Interactive client

After installing the Python SDK, connect to a remote dataset to list Items and preview their contents in the terminal. Use the existing `DACP_CA`, `DACP_CERT`, and `DACP_KEY` environment variables, or provide certificate paths explicitly:

```bash
.venv/bin/pip install -e './dacp-client[test]'
.venv/bin/dacp dacp://localhost:8815/data1 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

For a remote connection, replace `localhost:8815/data1` with the actual server address, port, and dataset name. You can also replace `.venv/bin/dacp` with `.venv/bin/python -m dacp.cli`.

```text
ls
show 1
show "nested/orders.csv" records
show "events.json" text
show "pixel.png" hex
use csv_data
connect dacp://localhost:8815/data1
help
quit
```

`ls` refreshes the listing and shows item numbers, formats, and paths. `show` accepts a number or a quoted path; it defaults to records for CSV/JSON/NDJSON/Parquet and hexadecimal previews for other formats. `text` displays raw bytes as UTF-8 while escaping terminal control characters. Use `records` for SQL tables, whose bytes view is Arrow IPC. Previews default to at most 20 rows or 4096 bytes, configurable with `--rows` and `--bytes`; rendered rows also have a character limit set by `--bytes`. Stopping a record preview cancels upstream reading to avoid downloading the entire dataset. Binary files appear as hexadecimal and ASCII; images are not opened directly. After a failure, retry the command or reconnect; a failed connection switch preserves the previous connection.

The server currently has no dataset discovery endpoint, so startup and `connect` require a known dataset URI. `use` switches to a known dataset on the same server. Exit with `quit`, `exit`, or EOF to close the connection.

## Serving directories and SQL databases together

The `sql_datasets` configuration adds SQL support while remaining compatible with the existing `datasets` directory configuration. SQL tables use the same `Source.open / list / open / as_records` API. Table listings include Arrow schemas, and the bytes view is an Arrow IPC stream. H2 and PostgreSQL JDBC drivers are included, with table allowlists, credentials from environment variables, and bounded fetch and batch sizes. See [SQL configuration, types, and run instructions](examples/sql/README.md).

```bash
cd dacp-server
sbt 'run ../examples/sql/server.yaml'
# In another terminal, from the project root, after setting DACP_CA/CERT/KEY above:
.venv/bin/python examples/sql/client.py
```

## Running conversion tasks

Stop the previous service instance, then build the operator and start the service with a task:

```bash
# Project root
docker build -f operators/format_convert/1.0/Dockerfile -t dacp/format-convert:1.0 .
cd dacp-server
sbt 'run ../examples/server.yaml ../examples/convert.yaml'
```

The DAG in the YAML file runs after Flight starts. `csv_data` is a CSV dataset pointing to `examples/data/nested`; it shares local files with `data1`, the mixed dataset used to demonstrate PNG reads. Successful execution logs the URIs for `s1` and `s2`. Parquet files are stored under `examples/output/<output>-<uuid>/`. The service continues running and exposing outputs, which are automatically deleted when their TTL expires. Stopping the service also cleans up outputs.

From the project root, using the certificate environment variables above:

```bash
.venv/bin/python examples/verify_job.py
```

Parquet Items in output listings must include a schema. Listing schemas for raw CSV/JSON may be empty because enumeration does not scan data; schemas are inferred when retrieving FlightInfo for a specific Item.

The operator library supports a standalone web service. When `operator_library_url` is configured, execution nodes download the image archive for the requested operator version, verify and cache it, and execute it in local Docker isolation. See [dacp-operator-library](dacp-operator-library/README.md) for startup, publishing, and configuration. Without a URL, the local `operators/<name>/<version>/operator.yaml` is used. Versions cannot be overwritten, and there are no dynamic version updates. The standard Dockerfile runs `sbt stage` during the build and retains only the JRE and application at runtime. Do not mount generated certificates or data into operator images.

### DAG and operator contract

`input` is a single URI or an upstream `output` name; `inputs` is a list of such values. Upstream references also support `s1/part-0.parquet`. Tasks validate duplicate names, unknown dependencies, cycles, and target nodes before running serially in topological order. Execution defaults to the node hosting the first input URI, or can be set explicitly with `node: dacp://host:port`. The coordinating node resolves upstream references into concrete URIs and dispatches tasks through mTLS Flight actions; no central registry is required. Each subtask gets a new container with networking disabled, a read-only `/input` mount, and a writable `/output` mount.

`/input/manifest.json` contains `file/path/format`; `file` points to an Arrow IPC stream. The bytes view uses `data: binary`, with one raw byte chunk per row. `/input/params.json` contains the parameter object. Single inputs use a FIFO, with IPC EOS marking completion; container reads drive upstream consumption. Multiple inputs are fully materialized on disk before the container starts. Temporary input directories are removed on completion or failure, and failed output directories are also deleted. Outputs are registered only when the container exits with code 0. The maximum task timeout is 30 minutes.

The container entry point receives `operator /input /output /input/params.json`; the `DACP_OPERATOR` environment variable specifies the versioned operator name. Custom operators follow the same Arrow input contract. They produce an `/output/_output.json` manifest (`file/path/format`), where each `file` is a single-column binary Arrow IPC stream and `path` is the final filename. After successful exit, the executor decodes these streams, removes temporary IPC files, and registers the final directory. Once the Parquet footer is complete, built-in operators wrap files in this Arrow output contract with bounded memory. The built-in converter processes CSV/JSON Items individually into `part-N.parquet`, does not merge Items with different schemas, and rejects PNG and other formats.

### format_convert parameters

| Parameter | Default / supported values |
|---|---|
| `from` | `auto`; `csv` / `json` |
| `to` | `parquet` |
| `schema` | `infer` or an ordered column declaration, such as `{id: int64, amount: float64}` |
| `infer_sample` | 4; each chunk contains 1024 parsed records; the schema is fixed after sampling |
| `on_conflict` | `fail` only |
| `compression` | `zstd` / `snappy` / `none`; defaults to zstd |
| `row_group_size` | `128MB`; also accepts B / KB / GB or a byte count |

Explicit schemas support `int64`, `float64`, `bool`, and `utf8` (or `string`); column names and order must match. Empty CSV fields and JSON null values become null. JSON uses NDJSON, with one flat object per line. Conflicting types during sampling immediately produce `SCHEMA_CONFLICT`, and subsequent batches are checked against the fixed types. Explicit float64 permits valid integer representations, while explicit utf8 preserves text. There is no automatic type widening. Memory usage depends on sampled chunks, the current Arrow batch, parser buffers, and the Parquet row group, rather than total file size. An exceptionally large individual record must still fit in memory.

The converter uses Arrow Java to decode IPC and build vectors, and the native Apache Parquet Java writer with an Avro row bridge to produce Parquet; Arrow Java has no native Parquet writer. The output Arrow schema is written to both `dacp.arrow.schema` in the Parquet footer and the reserved dataset file `_schema.json`. Reserved files are not exposed as Items.

## Protocol and configuration

The sole metadata definition is [proto/dacp.proto](proto/dacp.proto). Scala generates Java classes during the sbt build, and Python bindings are committed. After changing the protocol, regenerate them using the system protoc (3.21+):

```bash
protoc -I proto --python_out=dacp-client/dacp proto/dacp.proto
```

| Flight call | Response |
|---|---|
| GetFlightInfo `[datasets, dataset]` | `app_metadata` contains a protobuf `DataSet` with a recursively flattened Item listing; no DoGet endpoint |
| GetFlightInfo `[datasets, dataset, relative/item]` | Arrow schema and a FlightEndpoint containing a protobuf `Ticket` |
| DoGet `Ticket(dataset,item,view)` | `view=records` returns records; `view=bytes` returns a single binary column containing raw file bytes |

The records view supports CSV, NDJSON, and dacp-generated Parquet. Any file can use the bytes view. The SDK wraps binary batches in an incrementally readable `io.BufferedReader`. Tickets describe resources and views; they are not authentication tokens. Both GetFlightInfo and DoGet validate URI allowlists, normalized paths, and symbolic-link boundaries. Links outside allowed boundaries are excluded from listings.

Configuration in `examples/server.yaml`:

| Field | Meaning |
|---|---|
| `host` / `port` | Listening address / port |
| `public_host` | Hostname for external URIs; allowlists use this name and an explicit port |
| `datasets` | Dataset name → local directory; MVP names occupy one URI path segment |
| `allowed_uri_prefixes` | Node-level URI prefix allowlist, matched at path segment boundaries; output names also require authorization |
| `tls_cert` / `tls_key` / `tls_ca` | PEM certificate, private key, and trusted client CA; mTLS is mandatory |
| `batch_size` | Maximum rows per batch in CSV/JSON records views |
| `byte_chunk` | Maximum bytes per binary row in bytes views |
| `infer_sample` | Number of batches sampled for server-side records schema inference |
| `output_ttl_seconds` | Output lifetime starting at successful registration |
| `sql_datasets` | JDBC datasets: url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
| `operator_library_url` / `operator_cache` / `operator_max_bytes` | Remote operator library URL / local immutable version cache / maximum archive size in bytes (default 8 GiB) |
| `operator_library` / `output_root` | Local operator library / temporary output root |
| `task_peers` | Node origins permitted for task dispatch; the local node is always allowed; no central discovery service |
| `task_input_prefixes` | Input URI prefixes execution nodes may pull; defaults to `allowed_uri_prefixes` when omitted |
| `max_concurrent_tasks` | Concurrency limit for local tasks and control requests (default 2); returns UNAVAILABLE immediately when full |

The prefix `dacp://localhost:8815/data1` does not match `data10`. In this version, allowlists apply to all connections verified by the node's CA; there are no permission groups based on certificate subjects. Task executors initiate mTLS using the node's configured certificates. Multiple nodes must trust the corresponding CAs.

Service errors use Flight/gRPC statuses and explicit dacp error names: missing resources return `NOT_FOUND`; expired outputs return `NOT_FOUND` with `EXPIRED` in the message; denied access returns `UNAUTHORIZED` with `FORBIDDEN`; schema conflicts return `INVALID_ARGUMENT` with `SCHEMA_CONFLICT`. Protobuf also defines error codes for protocol extensions. Any transport failure invalidates the current reader, and callers must reopen from the beginning; there are no retries within a stream. Existing DoGet tickets also fail after output expiry, and active streams check TTL at the next batch boundary.

## Validation

```bash
(cd dacp-server && sbt test)
.venv/bin/pytest -q dacp-client/tests
# After starting the service and setting certificate environment variables
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
# After successful registration of s1/s2 from convert.yaml
.venv/bin/python examples/verify_job.py
```

Validation recorded for the current environment includes 20 Scala tests, 15 Python tests (including operator library service tests), and real mTLS checks for local directory/H2 SQL tables (records and Arrow IPC bytes views), enumeration/CSV/PNG, zstd conversion in containers, materialized dual inputs/snappy, output schemas, and DoGet after TTL expiry. `sbt run` was also started and verified. The current network cannot pull Temurin base images from Docker Hub, so container integration used a temporary test image built with the local JRE and the same `sbt stage` artifacts. Downloading base images for the repository's standard multi-stage Dockerfile still requires access to Docker Hub.

Unit tests cover placement across nodes, upstream URI resolution, worker permissions, capacity rejection, no retry on failure, descriptors/protobuf, recursive enumeration, path traversal boundaries, TTL cleanup, schema inference/explicit declarations/conflicts, DAG validation, Arrow binary chunk conversion to Parquet, container Arrow output contract round trips, and partial byte reads in the SDK. Security checks verify connection failure without a client certificate and rejection of unauthorized datasets even with a valid certificate.

## Architecture decisions and implementation

| Agreed decision | MVP implementation |
|---|---|
| Location in URI, autonomous nodes, no registry | Python `Source` / Scala `Config.datasets`, Flight self-description |
| Flight GetFlightInfo / DoGet | `Producer`, two descriptor forms and protobuf tickets |
| Downstream pull and backpressure | Flight `isReady` / cancellation callbacks; Pekko `mapAsync(1)`, bounded FIFO; gRPC may perform limited prefetching |
| RecordBatch chunking | `batch_size` / `byte_chunk`; no application-level push |
| No chunk retry / checkpoint / resume / deduplication | Reader failure terminates reading; tasks restart from the source; these mechanisms are not implemented |
| Connection-level mTLS and URI prefix allowlists | `useMTlsClientVerification`, authorization in both GetFlightInfo and DoGet; no login/token |
| DataSet → Item → Stream | `DataSetAdapter`, `Item`, SDK `DataSet/Stream` |
| Recursively flattened directories and equivalent single-level database access | `DirectoryAdapter` recursive files; `SqlAdapter` JDBC tables at one level with Arrow schemas |
| Records / bytes views | Arrow schema / single binary column and SDK byte input stream |
| YAML DAGs and a global name@version library | `Jobs` topological sorting, standalone HTTP(S) operator library, execution-node verification and caching of image archives; local directory mode retained, no hot updates |
| Computation follows data; nodes collaborate | `JobCoordinator` places tasks by URI and dispatches through mTLS Flight actions; workers execute locally without forwarding |
| One container per subtask | docker-java creation/start/exit-code inspection/removal |
| Streaming single inputs; materialized multiple inputs | Single-input Arrow FIFO; temporary IPC files for multiple inputs, cleaned up in finally |
| Upstream outputs exposed through the local node's Flight service with TTL | `OutputRegistry`, scheduled cleanup, expiry tombstones, explicit errors for old tickets |
| Streaming CSV/JSON → Parquet conversion | Scala `FormatConvert`, Arrow Java and Parquet Java, fixed row groups |
| Inferred / explicit schema, fail on conflict | Bounded sampling, subsequent validation, explicit column mapping; no automatic widening |
| Output schemas in metadata | Parquet footer, `_schema.json`, output Item protobuf |
| Cross-language protobuf / YAML | `proto/dacp.proto`, YAML jobs; Python bindings require no deployed protoc |
| No S3 / drill-down / pandas / HF | Not implemented, preserving the basic interface; SQL adaptation was added as a subsequent extension |

The output registry in this version resides only in memory. Tasks must be rerun after a restart; old tasks are not restored. Parquet record reads support files generated by this converter with an Arrow schema footer; other Parquet files can still be read as bytes. Directory listing size grows with the number of files, while data content is always streamed.

Collaboration across nodes is implemented: any authorized node can accept `Job.run(node, yaml)`, coordinate the DAG through Flight `dacp.run_job`, and dispatch resolved tasks through `dacp.execute_task` to the node hosting the inputs or specified in YAML. Outputs remain on the execution node and are pulled through the existing Flight data interface. See [the two-node example and run instructions](examples/multi-node/README.md).

```bash
# From the root: build operator images first, then run real two-node / multi-container validation
bash scripts/test-collaboration.sh
```

The built-in `identity@1.0` operator preserves raw bytes through Arrow streams so nodes can consume existing outputs from other nodes; upstream data is not restricted to CSV/JSON. Failure stops downstream dispatch, while existing outputs remain managed by each node's TTL. Lost control responses do not trigger automatic retries, and cancellation of already started remote tasks is not guaranteed.
