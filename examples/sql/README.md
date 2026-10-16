# Serve files and SQL tables from one DACP node

Run from the repository root. Requires JDK 17, sbt, Python and OpenSSL. Docker is unnecessary for reading datasets.

```bash
python3 -m venv .venv
.venv/bin/pip install -e ./dacp-client
bash scripts/certs.sh
cd dacp-server
sbt 'run ../examples/sql/server.yaml'
```

In another terminal, from the repository root:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python examples/sql/client.py
```

This starts one node serving a directory at `dacp://localhost:8815/files` and an embedded H2 SQL database at `dacp://localhost:8815/sales`. H2 creates the demonstration database and seeds two tables using `init.sql` on connection. These demo initialization statements are not part of normal SQL adapter operation. Table names are case-sensitive in DACP; H2 exposes unquoted SQL names as uppercase (`ORDERS`, `CUSTOMERS`).

```python
from dacp import Source

with Source.open("dacp://localhost:8815/sales") as ds:
    for table in ds.list():
        print(table.path, table.schema)
    for batch in ds.open("ORDERS").as_records():
        print(batch.to_pydict())
```

## PostgreSQL

The PostgreSQL and H2 JDBC drivers are bundled. Replace the `sales` entry under `sql_datasets` with:

```yaml
sql_datasets:
  sales:
    url: jdbc:postgresql://db-host:5432/sales?sslmode=verify-full
    user_env: DACP_DB_USER
    password_env: DACP_DB_PASSWORD
    schema: public
    tables: [orders, customers]
    fetch_size: 256
    query_timeout_seconds: 60
```

Set those environment variables before starting the node, and configure the PostgreSQL driver's trusted server CA for `verify-full`. Use a database account with SELECT privileges on exposed tables and schema USAGE. Credentials remain on the server and are never sent to DACP clients. Plain `user` / `password` fields are also accepted; environment fields take precedence and missing referenced variables fail configuration loading. Each SQL dataset represents one database schema; expose multiple schemas using separate dataset names.

Existing `datasets: {name: /directory}` configuration continues to work. `sql_datasets` is optional; SQL-only nodes may omit `datasets`. Dataset names must be distinct across the two maps. Each SQL dataset supports:

| Field | Meaning |
|---|---|
| `url` | JDBC connection URL |
| `user`, `password` | Optional server-side credentials |
| `user_env`, `password_env` | Environment variable names for credentials |
| `schema` | Exact database schema; defaults to the driver's current schema |
| `tables` | Optional exact, case-sensitive table allowlist; omitted means all tables in the schema; `[]` exposes none |
| `fetch_size` | JDBC fetch window; defaults to `batch_size` |
| `query_timeout_seconds` | Statement timeout, default 60 seconds; positive |

URI-prefix authorization applies to both adapters. A dataset prefix grants listing and reads; a table-only prefix permits direct table reads without granting dataset listing. SQL tables are flat Items, with `format: arrow` and Arrow schema metadata. Clients cannot submit arbitrary SQL, filters, joins or writes. Table names must match discovered metadata and are quoted as identifiers; views and system schemas are not automatically exposed outside the configured schema.

## Types and stream semantics

Supported SQL types map to Arrow as follows:

| SQL | Arrow |
|---|---|
| Integer types | signed int64 |
| REAL / FLOAT / DOUBLE | float64 |
| BOOLEAN / BIT | bool |
| Character types | utf8 |
| Binary types | binary |
| DECIMAL / NUMERIC with declared precision 1–38 and scale 0–precision | decimal128 |
| DATE | date32 |
| TIME | time64, microseconds |
| TIMESTAMP | timestamp, microseconds, no timezone |
| TIMESTAMP WITH TIME ZONE | timestamp, microseconds, UTC |

Nulls and JDBC column nullability are preserved. Sub-microsecond temporal precision is truncated. Arrays, LOBs, arbitrary vendor-specific types, unconstrained decimals, and time-with-time-zone are currently rejected explicitly; no silent type coercion is performed. Other JDBC engines need their driver added to `build.sbt` and validation of schema metadata, type mapping and cursor behavior before use.

`as_records()` streams table records directly. A table has no original file representation, so `as_bytes()` streams an **Arrow IPC serialization** in the standard single-binary-column Flight view. It can be consumed with `pyarrow.ipc.open_stream(ds.open("ORDERS").as_bytes())`; it is not CSV, SQL text or a database backup. `format_convert@1.0` still accepts CSV/JSON only; SQL table conversion is outside this adapter change.

Each request owns its JDBC connection, statement and result set. Reads request a read-only transaction with forward-only cursors. PostgreSQL uses autocommit=false and a positive fetch size to avoid buffering the entire result ([driver documentation](https://jdbc.postgresql.org/documentation/query/)). Arrow allocation is bounded by batch size; JDBC buffering depends on the driver, and individual large values still occupy memory. SQL results have no implicit row order. Listing/FlightInfo and DoGet use separate connections, so metadata and data are not a shared snapshot. Each reopened stream starts a new read, without retries, checkpoints or deduplication. Cancellation is checked between fetches/batches; an in-flight database operation may run until the driver/query timeout.

## Validation

```bash
cd dacp-server
sbt test
# From repository root: real mTLS Python/Flight/H2 and file reads
bash scripts/test-sql.sh
```
