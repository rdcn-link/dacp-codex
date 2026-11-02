# Shell and SDK guide

[中文](README_zh.md) · [User guide](../usage.md)

Connect to nodes and Union, browse catalogs and read streams with the Shell, Python or Scala. For installation, credentials and the first connection, follow the [quick start](../quickstart.md). The examples below use `dacp://localhost:8815` and the example service's `data1` Dataset; replace addresses, paths and credentials for your service.

Configure SDK credentials through environment variables:

```bash
export DACP_CA=/path/to/ca.crt
export DACP_TOKEN_FILE=/path/to/reader.token
```

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


## Execution and operations

- [Submit Jobs, variables and consume results](../jobs.md)
- [Retries, timeout, cancellation, materialization and replay](../materialization.md)
- [Roles, credentials and access failures](../security.md)
- [Publish and deploy a service](../operations/README.md)

Reopen an Item stream after a read failure. Job attempts and ordinary Item reads are distinct operations; execution policy controls automatic retries and materialization.
