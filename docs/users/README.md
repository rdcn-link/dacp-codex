# User guide

[中文](README_zh.md) · [Documentation](../README.md) · [Developer guide](../developers/README.md)

Job inputs and outputs use `step[:port]`, defaulting to `result`. External data enters through `read_item@1.0`; only stream connections establish dependencies. See [streaming jobs](../jobs.md).

For people browsing data, using SDKs, running operators, and configuring DACP nodes with existing plugins. Commands below run from the project root unless stated otherwise. To implement a new backend, format, or operator, use the developer guide.

## Access published data

Discover Datasets with `datasets`, select one with `use`, then browse or read Items by logical path. Use paths and views advertised by the service; private filesystem paths are not published. For data concepts and format examples, see the [XStream data model](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-model.md).

## Quick start

The data service requires JDK 17, sbt 1.10.x, and Python 3.10+. Browsing data does not require Docker; configure Docker when you want to run container steps.

From the project directory, install the Python SDK, generate local example certificates, and start the server:

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt stage
(umask 077
 mkdir -p .dacp-security/8815
 target/universal/stage/bin/dacp-admin create-account .dacp-security/8815 --role runner --subject alice > .dacp-security/8815/alice-issued.json
 python3 -c 'import json,pathlib; p=pathlib.Path(".dacp-security/8815"); (p/"alice.token").write_text(json.loads((p/"alice-issued.json").read_text())["token"])'
)
target/universal/stage/bin/dacp-server
```

The default server listens on `localhost:8815`; its configuration is [examples/server.yaml](../../examples/server.yaml). Connect to the server origin and use the argument-free shell command `datasets` to list its directory before selecting a Dataset; a sole Dataset is selected automatically. Nodes publish local catalogs; Union merges them online, and its management website also displays selected members separately. See [dataset directories](../dataset-directory.md).

In another terminal, connect from the project root:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_TOKEN_FILE="$PWD/dacp-server/.dacp-security/8815/alice.token"
.venv/bin/dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

Inside the shell:

```text
ls /
ls /reports
ls /archive
cat /archive/reports/orders.csv records
ls /images
cat /images/images/pixel.png hex
ls /bundle_gzip
cat /bundle_gzip/reports/orders.csv records
ls /orders.csv.gz
cat /orders.csv.gz/orders.csv records
cat /orders.csv.gz hex
cat /reports/orders records
cat /events text
cat /image hex
count /reports/orders
quit
```

The example Dataset contains `/events`, `/reports/orders`, `/image`, and the empty folder `/empty`, plus ZIP folders `/archive` and `/images`, TAR folder `/bundle`, compressed TAR folder `/bundle_gzip`, and GZIP folder `/orders.csv.gz`. `/archive` contains CSV, NDJSON, and an empty folder; `/images` contains a PNG and demonstrates the uppercase `.ZIP` extension. Run `.venv/bin/python examples/client.py` to check SDK reads as well. Generated certificates are for local examples; replace the URI and certificates for remote connections.

## Browsing and reading

### Python SDK

Using the CA and token-file environment variables from the quick start:

```python
from dacp import Source

with Source.open("dacp://localhost:8815/data1") as ds:
    for item in ds.list("/"):
        print(item.path, item.kind, item.format, item.views)

    with ds.open("/reports/orders") as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()

    with ds.open("/image") as stream:
        with stream.as_bytes() as reader:
            print(reader.read(8))

    print(ds.count("/reports/orders"))
```

| Operation | Behavior |
|---|---|
| `ds.list()` | Lists all Items recursively |
| `ds.list("/")` | Lists immediate children of the root folder |
| `ds.list("/reports")` | Lists immediate children of a particular folder |
| `ds.open(path)` | Opens a readable Item and returns a Stream |
| `ds.count(path)` | Explicitly requests an exact record count; non-record data and folders return unknown |

Paths must start with `/`; `.`, `..`, repeated separators, and trailing `/` are rejected except for the root path. The `records` view returns Arrow RecordBatches, while `bytes` provides incremental byte reads. File bytes are the original content; SQL bytes are Arrow IPC.

Use Streams before their owning Dataset closes. Close record iterators when stopping early and use context managers for byte readers. After failure, reopen and read from the beginning; reads cannot resume. Python also accepts a complete Item URI to open a Stream directly.

### Records filtering, projection, and limits

Both SDKs can request server-side records queries:

```python
from dacp import Source, col

with Source.open("dacp://localhost:8815/data1") as ds:
    with ds.open("/reports/orders", predicate=col("id") > 1,
                 columns=["amount"], limit=100) as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()
```

Execution follows filter → projection → limit. Predicates use public column names and typed literals, with comparisons, boolean logic, membership, and null checks. Filters may reference columns omitted from the output. SQL and Parquet push supported operations into their readers; other handlers use bounded server-side filtering. Reaching the limit stops reading, and bytes views reject these options. Item metadata and exact counts remain unchanged.

See [records query semantics and plugin contracts](../records-scans.md) for Python/Scala examples, null and type rules, validation, and source-specific optimizations.

### dacp-shell

`ls` displays a bordered table with #, Path, Format, Views, Bytes, Records, and Modified (UTC). The first column numbers items starting from 1. Unknown values appear as `-`, timestamps use `2026-10-16 06:22:02`, and terminal folder paths are highlighted in bold cyan.

`cat` accepts a path or a number from the latest listing. It prefers records, then bytes (hex), then the first advertised scientific view. Records appear in a table with column names; `text` displays text, and `hex` displays offsets, hexadecimal bytes, and ASCII. Previews default to at most 20 rows or 4096 bytes, adjustable with `--rows` and `--bytes`. `cat 2 array`, `cat /sky.fits/hdu0 array`, and `cat /genome.fa/sequences sequence` show a bounded structure descriptor and a table of block locations/identities and samples. Array/image samples decode numeric values using the declared data type and byte order. Scientific previews stop at the element/block or payload-byte budget and cancel the reader; one incoming block can be larger than the display budget. Unknown scientific representations/encodings show metadata and hexadecimal payload instead of assuming a file format. `cat ITEM bytes` is equivalent to `hex`. Views not advertised by the Item fail explicitly. The `Views` column lists all advertised representations; an empty list displays `-`.

`datasets` shows `#`, `Name`, and `Description (JSON)`, without URIs. Use `use NAME` to select a dataset by name, or `use 1` for the first row of the latest `datasets` output. Row numbers start at 1; refreshing the list updates the mapping and reconnecting clears it. Numeric arguments are interpreted as row numbers. Use `connect URI` to connect elsewhere, and `help` for commands. Certificate paths can also be passed through `--ca`, `--cert`, and `--key`.

Enter a single-line SQL query directly at the shell prompt:

```sql
SELECT amount FROM "/reports/orders" WHERE id > 1 LIMIT 10
SELECT amount FROM 2 WHERE id > 1 LIMIT 10
SELECT * FROM "/reports/orders" WHERE id IN (1, 2) AND amount IS NOT NULL
```

The shell uses SQLGlot to translate a supported SQL subset into the same remote scan request as the SDKs. `FROM` accepts one double-quoted absolute Item path within the current Dataset, or an Item number from the latest `ls` result (for example, `FROM 2`). Numbers follow the latest listing order, including `ls PATH`; out-of-range numbers produce an error. Use `SELECT *` or a list of public columns, optional `WHERE`, and optional `LIMIT`; SQL keywords are case insensitive. Results appear in a bordered table. The `--rows` preview cap still applies: the server receives the smaller of SQL `LIMIT` and the preview cap, or the preview cap when `LIMIT` is omitted. Use `help select` for syntax. Joins, sorting, aggregation, aliases, and subqueries are rejected. See [SQL shell details](../records-scans.md#sql-in-dacp-shell).


### Help and command examples

Run `help` for the command list and a short example session. Run `help cat`, `help select`, `help run`, or `help submit` for command-specific syntax and examples. Paths shown in help are examples; use paths advertised by your Dataset. Item numbers refer to the latest `ls` result.

### Scala SDK

The standalone [Scala 2.13 SDK](../../dacp-client-scala/README.md) provides the same data access model without depending on the server project. Publish it to the local Ivy repository first:

```bash
cd dacp-client-scala
sbt test publishLocal
```

Add this dependency to your Scala application's `build.sbt`:

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

Use `Source.open`, `Dataset.list`, `Dataset.open`, `asRecords()`, and `asBytes()`, managing resources with `scala.util.Using`. The SDK guide covers complete examples, TLS, timeouts, and Arrow JVM options. The artifact is not yet published to Maven Central.

## Publish data

See the [administrator guide](../administrators/README.md) for the listener, TLS, credentials and catalog publication settings. Configure backends using the [XStream Dataset guide](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-providers.md) and reader options using the [file format reference](https://github.com/rdcn-link/xstream-codex/blob/main/docs/file-formats.md). For a complete SQL publication example, see the [SQL example](../../examples/sql/README.md).

## Collaboration and steps

DACP executes YAML Jobs through the embedded XStream engine. Users describe logical stream dependencies without assigning execution nodes. Union resolves Item locations and plans node-local subjobs automatically; member collaboration uses mutual TLS and node-issued service credentials. Eligible node subjobs [share one container and process](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion.md), while other steps execute independently; named ports consume data-item streams and send output directly to clients or downstream steps, without saving output datasets.

Steps also require Linux, `mkfifo`, and access to Docker Engine. From the project root, configure an operator repository containing approved operators and start the server. Stop the quick-start server first if it is still running:

```bash
cd dacp-server
sbt 'run ../examples/server.yaml'
```

In a connected shell started from the project root, run the complete weather workflow in the [current operator catalog](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-catalog.md), or select a weather dataset and run `run piflow_csv_parser@1.0 --input source=/observations --view bytes`. The shell waits and prints results incrementally; Ctrl-C cancels execution.

See [remote Job submission](../jobs.md) for execution commands and [XStream operator development](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring.md) for authoring.

`run` and `submit` print `Execution time: 1.234 s` after completion, failure or cancellation. This is elapsed time measured by the shell from submission through closing the result stream, including connection, runtime preparation, execution, transfer, display and client cleanup. It excludes heredoc entry time and is not server step CPU time. Multi-step jobs print one total for the whole job.

## Security and operating limits

DACP metadata is anonymous over TLS. Data reads require a token whose subject has reader or runner; execution and inference require runner. Union collaboration uses mutual TLS and service credentials. No Dataset/Item ACL is configured. Public metadata, tickets, and data-service errors hide private storage bindings; data content still contains the source's own fields and values. Python's `DacpError` and Scala's `DacpException` provide error codes and readable messages, while the shell displays concise prompts.

Unpublished physical files and database tables are not automatically exposed. Failed reads restart from the beginning, with no in-stream retries, checkpoints, or resume support. Step outputs are live streams that clients must consume or cancel. There are no saved result URIs, and steps are not restored after restart. See the [technical reference](../reference.md#protocol-and-configuration) for full configuration and protocol details.

In DEBUG mode, enter heredoc source with `run --runtime python@1.0 --input source=/orders <<END`. Servers default to NORMAL and reject source steps at both coordinator and execution nodes. See [source operators](https://github.com/rdcn-link/dacp-codex/blob/main/docs/operator-source.md) for configuration, Python/Scala examples, and the plugin contract.

Native executables can use stdin/stdout through the `command` executor without implementing the DACP protocol in the program. It supports one bytes input and output, with execution isolated inside a container. See [configuration and publication](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands.md) and the [C example](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/native/README.md).

## Startup DEBUG logs

At startup, DEBUG logs from `dacp.Main` print the count of `remote_operator_library_urls` and each URL in configured order. An empty list reports count 0. Ordinary INFO logging does not print these URLs. After staging the server, run from the project root:

```bash
(cd dacp-server && target/universal/stage/bin/dacp-server \
  -J-Dorg.slf4j.simpleLogger.log.dacp.Main=debug ../examples/server.yaml)
```

This logging level is independent of the `mode: DEBUG` source-submission setting. Enabling DEBUG logs does not enable source execution.



## Union collaboration jobs

Submit explicit-placement DAGs through the [Union server](../../dacp-union-server/README.md), subscribe to execution status and traffic, and read final streams. Module documentation covers member configuration, service credentials, Python and Scala usage; the [two-node example](../../examples/union/README.md) combines meteorological observations and climate baselines across nodes.

DACP and Union persist all execution records by default; the Union dashboard can query history after restart. For local query commands and storage directories, see [execution history](../execution-history.md).

## Execution policies, retries, and replay

Both single-node DACP Server multi-step jobs and Union collaborative jobs accept a separate execution-policy YAML with `max_retries`, `timeout_seconds`, and `materialization`. Use `submit workflow.yaml --policy policy.yaml` in the shell and `replay EXECUTION_ID` to replay from valid materialized boundaries. Python and Scala clients must explicitly supply a retry callback to clear the preceding attempt's partial results; closing the result stream cancels execution. See [execution policy, output materialization, and replay](../materialization.md) for configuration, complete calls, and success confirmation rules.

Union provides standard DACP discovery, Items, Jobs and S3. For public short names, placement and management access, see [Union proxy service](../union-proxy.md).

[Online Job Designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README.md): design local and cross-node jobs in the browser and export YAML for Shell / SDK submission.

For metadata-only browsing, omit `--token-file`: `dacp-shell dacp://host:8815`. Dataset and Item listings and Item metadata remain available; `cat`, queries and computed counts require reader or runner, and `run`/`submit` require runner.

### Anonymous browsing by address

```bash
.venv/bin/dacp-shell dacp://10.0.82.103:18861
```

With no `DACP_TOKEN_FILE`, `DACP_CA` or client certificate configured, public catalogs and Item metadata need no `ca.crt`. This mode keeps TLS encryption without server certificate verification. Supplying `--ca` enables verification; token-authenticated access still requires a trusted server certificate. Anonymous access cannot read data streams or execute jobs.
