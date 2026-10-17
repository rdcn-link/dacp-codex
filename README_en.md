# DACP

<img src="assets/dacp-logo.png" alt="DACP logo: connected nodes forming a C for Collaborative" width="480">

[中文](README.md)

DACP is a data collaboration protocol and prototype built on Apache Arrow Flight. Nodes publish data through a common interface: clients browse files, SQL tables, and archives and compressed files using logical names, and submit YAML tasks to run computation where the data resides.

The project includes a Scala 2.13 server, Python and Scala client SDKs, and the interactive `dacp-shell` terminal. Nodes are addressed directly by URI and connect through mTLS, without a central registry.

## Data model

DACP describes data access through Datasets, Items, and Streams:

| Concept | Meaning | Example |
|---|---|---|
| Dataset | A named logical namespace published by a node, combining data from different sources | `dacp://localhost:8815/data1` |
| Item | A resource with an absolute logical path within a Dataset | `/reports/orders` |
| `folder` Item | A grouping of child Items; archive and compression folders also offer original file bytes | `/reports`, `/archive` |
| `data` Item | Readable data with a format and supported views | CSV file, SQL table, image |
| Stream | The reading interface obtained by opening an Item | Arrow record batches or a byte stream |

A Dataset is a logical namespace and may contain both files and SQL tables. Administrators choose public paths, keeping server file paths and database table names private. Every Item path starts with `/`; the root folder `/` is implicit and does not appear in listings.

**kind** describes whether an Item can contain children, **format** identifies its representation, and **views** describes how to read it. Clients should check views rather than infer record-reading capabilities from filenames.

## Quick start

The data service requires JDK 17, sbt 1.10.x, and Python 3.10+. Browsing data does not require Docker; configure Docker when you want to run container tasks.

From the project directory, install the Python SDK, generate local example certificates, and start the server:

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt run
```

The default server listens on `localhost:8815`; its configuration is [examples/server.yaml](examples/server.yaml). Startup logs list the published Dataset names and URIs. There is currently no dataset discovery endpoint, so connections require a known URI.

In another terminal, connect from the project root:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/dacp-shell dacp://localhost:8815/data1
```

Inside the shell:

```text
ls /
ls /reports
ls /archive
show /archive/reports/orders.csv records
ls /images
show /images/images/pixel.png hex
ls /bundle_gzip
show /bundle_gzip/reports/orders.csv records
ls /orders.csv.gz
show /orders.csv.gz/orders.csv records
show /orders.csv.gz hex
show /reports/orders records
show /events text
show /image hex
count /reports/orders
quit
```

The example Dataset contains `/events`, `/reports/orders`, `/image`, and the empty folder `/empty`, plus ZIP folders `/archive` and `/images`, TAR folder `/bundle`, compressed TAR folder `/bundle_gzip`, and GZIP folder `/orders.csv.gz`. `/archive` contains CSV, NDJSON, and an empty folder; `/images` contains a PNG and demonstrates the uppercase `.ZIP` extension. Run `.venv/bin/python examples/client.py` to check SDK reads as well. Generated certificates are for local examples; replace the URI and certificates for remote connections.

## Browsing and reading

### Python SDK

Using the certificate environment variables from the quick start:

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

### dacp-shell

`ls` displays a bordered table with Path, Format, Bytes, Records, and Modified (UTC). Unknown values appear as `-`, timestamps use `2026-10-16 06:22:02`, and terminal folder paths are highlighted in bold cyan.

`show` accepts a path or a number from the latest listing. It prefers records when available and otherwise shows hexadecimal bytes. Records appear in a table with column names; `text` displays text, and `hex` displays offsets, hexadecimal bytes, and ASCII. Previews default to at most 20 rows or 4096 bytes, adjustable with `--rows` and `--bytes`.

Use `use DATASET` to switch datasets on the same server, `connect URI` to connect elsewhere, and `help` for commands. Certificate paths can also be passed through `--ca`, `--cert`, and `--key`; `dacp` remains a compatibility command alias.

### Scala SDK

The standalone [Scala 2.13 SDK](dacp-client-scala/README.md) provides the same data access model without depending on the server project. Publish it to the local Ivy repository first:

```bash
cd dacp-client-scala
sbt test publishLocal
```

Add this dependency to your Scala application's `build.sbt`:

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

Use `Source.open`, `Dataset.list`, `Dataset.open`, `asRecords()`, and `asBytes()`, managing resources with `scala.util.Using`. The SDK guide covers complete examples, TLS, timeouts, and Arrow JVM options. The artifact is not yet published to Maven Central.

## Publishing data

Configure public paths under `datasets.<name>.items`, binding private sources through `file` or `sql` / `table`. For example, this Dataset publishes a CSV file, a SQL table, a ZIP archive, and an empty folder together:

```yaml
datasets:
  public:
    items:
      /reports/orders: {file: ../examples/data/nested/orders.csv}
      /clients: {sql: warehouse, table: CUSTOMERS}
      /archive: {file: /private/reports.zip}
      /empty: {kind: folder}
sql_sources:
  warehouse:
    url: jdbc:h2:../examples/sql/sales
    user: sa
    password: ""
    schema: PUBLIC
    tables: [CUSTOMERS]
```

This is a configuration fragment; merge it with listener, TLS, and URI allowlist settings. Paths resolve relative to the server process working directory. Replace `/private/reports.zip` with a real file, and create SQL tables beforehand. Parent folders are inferred; other files and tables are not automatically published.

SQL supports H2 and PostgreSQL JDBC, with table allowlists, environment-based credentials, fetch sizes, and query timeouts. See the [SQL example](examples/sql/README.md) for initialization and run instructions. Legacy automatic directory publication and `sql_datasets` configurations should migrate to explicit Item mappings.

### Formats and views

| Source | format | kind | views |
|---|---|---|---|
| CSV | `csv` | `data` | `records`, `bytes` |
| NDJSON (`.jsonl`, `.ndjson`) | `ndjson` | `data` | `records`, `bytes` |
| Ordinary JSON | `json` | `data` | `bytes` |
| Excel (`.xls`, `.xlsx`) | `excel` | `data` | `records`, `bytes` |
| Parquet with DACP Arrow schema metadata | `parquet` | `data` | `records`, `bytes` |
| SQL table | `sql` | `data` | `records`, `bytes` |
| ZIP | `zip` | `folder` | `bytes` |
| TAR | `tar` | `folder` | `bytes` |
| GZIP (`.gz`) | `gzip` | `folder` | `bytes` |
| Compressed TAR (`.tar.gz`, `.tgz`) | `tar-gzip` | `folder` | `bytes` |
| Other files | `bin` | `data` | `bytes` |

Detection supports explicit `format` settings, signatures, and case-insensitive extensions, recognizing compound suffixes such as `.tar.gz` first and falling back to binary for unknown formats. Set `format: ndjson` for `.json` files containing one object per line to enable records. Excel supports sheet selection, header rows, and explicit schemas.

Browse ZIP, TAR, and compressed TAR entries through paths such as `/archive/reports/orders.csv`; opening the folder's bytes view reads the original archive. Plain GZIP exposes one decompressed child, such as `/orders.csv.gz/orders.csv`. Its name comes from the public alias, never a private filename; aliases without a `.gz` suffix use the child name `content`.

ZIP listings read the directory index. TAR requires a sequential scan, and `.tar.gz` / `.tgz` also requires decompression, so initial listings and reads can be slow for large archives. Reading stages only the requested entry into a temporary file. Nested containers are not expanded; `.bz2`, `.xz`, and `.zst` currently remain ordinary binary data.

See the [technical reference](docs/reference_en.md) for format details, Excel behavior, archive limits, and schema rules, and [format extensions](docs/format-handlers.md) for third-party handler development.

### Metadata and counting

Items may carry modification time, byte size, and an exact record count. Unknown values differ from `0`. All three properties remain unknown for every folder; computing them never traverses children.

Ordinary browsing does not scan CSV, NDJSON, or Excel to count records, or execute SQL `COUNT(*)`. Request counts explicitly with `count`; Parquet can obtain them from its footer. Metadata is an observation at request time, rather than an atomic snapshot with subsequent reads.

## Collaboration and tasks

DACP organizes tasks as YAML DAGs. A coordinating node resolves dependencies and places each task on the node hosting its first input unless an execution node is specified. Nodes collaborate through mTLS Flight actions. Each subtask uses a separate Docker container; outputs are published as temporary Datasets on execution nodes and managed by TTL.

Tasks also require Linux, `mkfifo`, and access to Docker Engine. From the project root, build the conversion operator and start the example job. Stop the quick-start server first if it is still running:

```bash
docker build -f operators/format_convert/1.0/Dockerfile -t dacp/format-convert:1.0 .
cd dacp-server
sbt 'run ../examples/server.yaml ../examples/convert.yaml'
```

On success, the logs print the `s1` and `s2` URIs. With the certificate environment variables still set, run `.venv/bin/python examples/verify_job.py` from the project root to check outputs.

Further reading:

- [Two-node example](examples/multi-node/README.md): task placement, remote inputs, and consuming outputs.
- [Operator library service](dacp-operator-library/README.md): publishing immutable operator versions and downloading, verifying, and caching images.
- [Task and operator reference](docs/reference_en.md#task-execution-and-operators): DAGs, container input/output contracts, and conversion parameters.

## Security and operating limits

All connections require mTLS and URI prefix authorization. Public metadata, tickets, and data-service errors hide private storage bindings; data content still contains the source's own fields and values. Python's `DacpError` and Scala's `DacpException` provide error codes and readable messages, while the shell displays concise prompts.

Allowlists currently apply at node level, without permissions grouped by certificate subject. Failed reads restart from the beginning, with no in-stream retries, checkpoints, or resume support. The task output registry is in memory; outputs are cleaned on TTL expiry or server shutdown, and tasks are not restored after restart. See the [technical reference](docs/reference_en.md#protocol-and-configuration) for full configuration and protocol details.

## Development and validation

Run from the project root:

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
```

After starting the server and setting the certificate environment variables, run the SDK and security checks:

```bash
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
```

Use `bash scripts/test-collaboration.sh` for cross-node container integration; it requires Docker. Tests cover namespaces, format readers, archive browsing, SQL streams, schema validation, authorization, output expiry, and task collaboration.

The protocol is defined in [proto/dacp.proto](proto/dacp.proto). Source code lives in [dacp-server](dacp-server), [dacp-client](dacp-client) for Python, and [dacp-client-scala](dacp-client-scala) for Scala.
