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

Use `use DATASET` to switch datasets on the same server, `connect URI` to connect elsewhere, and `help` for commands. Certificate paths can also be passed through `--ca`, `--cert`, and `--key`.

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

## Constructing and publishing Datasets

Datasets are constructed by extensible providers. Three providers are built in, exposing the same `list`, `open`, and `count` APIs to clients:

| provider | Construction | Private source |
|---|---|---|
| `directory` | Discovers files, subdirectories, and archives beneath a root | `source.root` |
| `sqldb` | Publishes tables from one SQL connection under public aliases | `source.connection` |
| `custom` | Defines each Item explicitly, combining files, SQL tables, and logical folders | Item `file`, `sql`, and `table` bindings |

```yaml
datasets:
  documents:
    provider: directory
    source:
      root: /home/test/dataset1
    reader_overrides:
      /events.json: {format: ndjson}

  sales:
    provider: sqldb
    source:
      connection: warehouse
    tables:
      /customers: {table: CUSTOMERS}

  research:
    provider: custom
    items:
      /measurements: {file: /private/results.csv}
      /customers: {sql: warehouse, table: CUSTOMERS}
      /empty: {kind: folder}

sql_sources:
  warehouse:
    url: jdbc:h2:../examples/sql/sales
    user: sa
    password: ""
    schema: PUBLIC
    tables: [CUSTOMERS]
```

This is a configuration fragment; merge it with listener, TLS, and URI allowlist settings, and replace example paths and database connections. Relative paths resolve against the server process working directory.

Directory Datasets expose paths relative to their root, such as `/reports/orders.csv`. Empty directories are folders, and supported archives can be browsed further. Subsequent listings reflect additions, deletions, and changes. The root stays private, all symbolic links are ignored, and access remains confined to the root. Optional directory `reader_overrides` override reader format, sheet, header row, and schema; they do not rename paths or add other sources.

SQL Dataset `tables` bind public paths to private tables in the selected connection; tables are not automatically exposed. `custom` retains explicit mappings and mixed-source flexibility. Every Dataset must explicitly specify `provider`.

The default example provides `data1` and `csv_data` (custom), plus `documents` (directory). In the shell, use `use documents`, `ls /`, or read `/nested/orders.csv`. See the [SQL example](examples/sql/README.md) for a complete SQLDB initialization example. Future sources such as MongoDB can be added through provider JARs without client changes; see the [Dataset provider guide](docs/dataset-providers.md).

### User-defined Dataset providers

A user-defined provider is a server-side Scala/JVM plugin. `provider` selects the plugin; `source` supplies private parameters for this Dataset instance. Multiple Datasets can use the same provider with different parameters. The plugin validates its settings; clients see public paths and data.

This complete provider example publishes one CSV file at the fixed public path `/records`:

```scala
package example

import dacp._
import java.nio.file.Path

class CsvFileProvider extends DatasetProvider {
  val id = "csv-file"

  def create(d: DatasetDefinition, context: DatasetContext): PublishedDataset = {
    require(d.source.keySet.subsetOf(Set("file")),
      "unknown csv-file parameter")
    require(d.source.contains("file"), "csv-file requires source.file")
    require(d.items.isEmpty && d.bindings.isEmpty,
      "csv-file does not accept item mappings")
    val file = Path.of(d.source("file").toString)
    context.publish(Seq(PublishedItem(
      "/records", file = Some(file),
      options = FileOptions(format = Some("csv"))
    )))
  }
}
```

Compile the code into a JAR against the standalone `dacp-plugin-api`, using the matching API and Scala versions and a `Provided` dependency. Include this service registration file inside the JAR:

```text
META-INF/services/dacp.DatasetProvider
```

Its contents are the implementation class name:

```text
example.CsvFileProvider
```

Place the plugin and its required dependency JARs in `plugin_directory` (without duplicating the API or dependencies supplied by the server), then restart with this configuration fragment merged with listener, TLS, and URI allowlist settings:

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  measurements:
    provider: csv-file
    source:
      file: /private/measurements.csv
```

Clients read `/records` without receiving `source.file`. Plugins needing per-item mappings receive them through `items` by default, or can override `DatasetProvider.itemSettingsKey` to declare their own configuration key; parsed mappings are always available as `definition.items`. `source` can contain nested plugin-specific parameters; the plugin must validate types, required values, and unknown parameters.

Directory providers use `reader_overrides`, SQLDB uses `tables`, and custom uses `items`. Directory and SQLDB reject `items`. Native backends such as a future MongoDB provider can implement `PublishedDataset` and `DatasetAccess` directly for listing, metadata, counting, and streaming without mapping to files or SQL. See the [provider development guide](docs/dataset-providers.md) for interfaces, cancellation, and resource cleanup requirements. MongoDB is not implemented yet.

### File and structured-content plugins

File plugins implement `FormatHandler` and register in `META-INF/services/dacp.FormatHandler`. Each handler validates its private `options` parameters; for example, Excel uses `options: {sheet: Orders, header_row: 2}`. Common `format` and `schema` remain at the top level of reader settings.

To browse logical objects inside a file, implement `StructuredFileHandler` and register through the same format service file. Its `openStructure` returns `DatasetAccess` and can stream internal objects directly without extracting temporary files. The file root is a folder with original bytes; children explicitly advertise `records` or `bytes`. Archive readers can implement `ContainerReader` and register in `META-INF/services/dacp.ContainerReader`, without modifying core format registries.

The standalone API lives in [dacp-plugin-api](dacp-plugin-api). Runnable contributions and build/install instructions are in [examples/plugins](examples/plugins/README.md). `PluginChecks.listing` provides reusable metadata contract checks. The server closes request-scoped access handles and releases provider resources on shutdown. Concrete NetCDF/FITS parsers and new array/image views are not implemented yet; contributors can represent internal file objects through the current interfaces, without silently flattening arrays into records.

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

The example server enables `count_file_records_on_startup: true`: custom file mappings and initially discovered directory record files such as CSV, NDJSON, and Excel are counted once when published, so `/events` and `/reports/orders` show `2` in Records. This option defaults to off; enabling it increases startup time, and counting failures fail startup. SQL, archive children, and all folders are excluded from startup counting. Newly discovered directory files do not trigger counting during browsing; query explicitly or restart to populate them.

Ordinary browsing does not scan files for counts or execute SQL `COUNT(*)`. Saved file counts become unknown when size or modification time changes. Restart to repopulate them, or use `count` to request the latest value explicitly; explicit requests do not update saved listing counts. Parquet can still obtain counts from its footer. Metadata is not an atomic snapshot with subsequent reads.

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
- [Scala operator repository and website](dacp-operator-repository/README_en.md): public search, version details, downloads, and immutable operator publication.
- [Task and operator reference](docs/reference_en.md#task-execution-and-operators): DAGs, container input/output contracts, and conversion parameters.

Start a repository with the starter operators using `scripts/setup-operator-repository.sh --host 0.0.0.0 --port 8820`, then open `http://localhost:8820/` to search, view, and download operators without signing in. See the repository guide for HTTPS deployment.

## Security and operating limits

All connections require mTLS and URI prefix authorization. Public metadata, tickets, and data-service errors hide private storage bindings; data content still contains the source's own fields and values. Python's `DacpError` and Scala's `DacpException` provide error codes and readable messages, while the shell displays concise prompts.

Allowlists currently apply at node level, without permissions grouped by certificate subject. Failed reads restart from the beginning, with no in-stream retries, checkpoints, or resume support. The task output registry is in memory; outputs are cleaned on TTL expiry or server shutdown, and tasks are not restored after restart. See the [technical reference](docs/reference_en.md#protocol-and-configuration) for full configuration and protocol details.

## Development and validation

DACP prefers plugins for extension. New dataset backends, file formats, and processing operators should implement public extension contracts and be installed as plugins, without changes to core dispatch. Built-in and community implementations should use the same contracts; plugins own their parameter validation and third-party dependencies, while clients access data through common paths and capabilities. See the [plugin contribution guide](docs/plugins.md) for contribution guidance, available mechanisms, and scientific-data extension boundaries.

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
