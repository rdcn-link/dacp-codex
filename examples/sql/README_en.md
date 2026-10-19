# Serve files and SQL tables from one DACP node

[中文](README.md)

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

This starts one node serving logical datasets `dacp://localhost:8815/files` and `dacp://localhost:8815/sales`. H2 is an embedded Java database, stored at `examples/sql/sales`. Its private JDBC source `warehouse` initializes the demonstration tables using `init.sql`. Clients access aliases `/reports/orders` and `/clients`; physical table names and file paths remain server-side.

```python
from dacp import Source

with Source.open("dacp://localhost:8815/sales") as ds:
    for item in ds.list("/"):
        print(item.path, item.kind)
    print(ds.list("/reports"))
    for batch in ds.open("/reports/orders").as_records():
        print(batch.to_pydict())
```

`list()` returns all Items recursively; `list(path)` returns immediate children of a folder. All paths start with `/`. Files and SQL tables both become `data` Items; parent groups are `folder` Items. Empty folders can be declared with `kind: folder`. The `sales` Dataset uses `provider: sqldb` with `source.connection: warehouse`; its `tables` mappings need only `table`. The `files` Dataset uses `mixed`. A mixed Dataset may combine file and SQL mappings.

## PostgreSQL

The PostgreSQL and H2 JDBC drivers are bundled. Replace the private `warehouse` source under `sources` and update its explicit public mappings:

```yaml
datasets:
  sales:
    provider: sqldb
    source:
      connection: warehouse
    tables:
      /reports/orders: {table: orders}
      /clients: {table: customers}
sources:
  warehouse:
    provider: jdbc
    settings:
      url: jdbc:postgresql://db-host:5432/sales?sslmode=verify-full
      user_env: DACP_DB_USER
      password_env: DACP_DB_PASSWORD
      schema: public
      tables: [orders, customers]
      fetch_size: 256
      query_timeout_seconds: 60
```

Set those environment variables before starting the node, and configure the PostgreSQL driver's trusted server CA for `verify-full`. Use a database account with SELECT privileges on exposed tables and schema USAGE. Credentials remain on the server and are never sent to DACP clients. Plain `user` / `password` fields are also accepted; environment fields take precedence and missing referenced variables fail configuration loading. Each private SQL source describes one database schema. Use `provider: mixed` with Item-level `sql` bindings to combine multiple sources if desired.

Use `provider: directory` with `source.root` for automatic file discovery, or configure `sqldb` / `mixed` public Item mappings and private `sources` explicitly. Private sources and tables are not published unless mapped. File mappings use `{file: /private/path.csv}`; SQL mappings use `{sql: warehouse, table: private_table}`. Names and paths visible to the client are independently chosen aliases. Each private SQL source supports:

| Field | Meaning |
|---|---|
| `url` | JDBC connection URL |
| `user`, `password` | Optional server-side credentials |
| `user_env`, `password_env` | Environment variable names for credentials |
| `schema` | Exact database schema; defaults to the driver's current schema |
| `tables` | Optional exact, case-sensitive table allowlist; omitted permits mapping any table in the schema; `[]` permits none; tables still require explicit public aliases |
| `fetch_size` | JDBC fetch window; defaults to `batch_size` |
| `query_timeout_seconds` | Statement timeout, default 60 seconds; positive |

URI-prefix authorization applies to public logical addresses. A dataset prefix grants listing and reads; a data-Item prefix permits direct reads without granting dataset listing. SQL mappings have `kind: data`, `format: sql`, `views: [records, bytes]`, and Arrow schema metadata. Clients cannot submit arbitrary SQL, filters, joins, writes, or physical table names. Server-side table mappings must match JDBC metadata and are quoted as identifiers. Source bindings do not appear in listings or tickets, and data-service errors do not disclose underlying paths or identifiers. Column names and data values remain part of the returned content.

SQL-backed data Items leave `record_count` unknown during normal listing and opening. Explicit `ds.count("/reports/orders")` executes `COUNT(*)`, returning an exact count including zero for empty tables. `last_modified_time_ms` and `total_size_bytes` remain absent because JDBC does not provide portable content modification times or serialized byte sizes. Counts may scan large tables and respect `query_timeout_seconds`; metadata and later reads are not a shared snapshot. All folder properties remain unknown, including for empty folders, without visiting child Items.

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

`as_records()` streams table records directly. A table has no original file representation, so `as_bytes()` streams an **Arrow IPC serialization** in the standard single-binary-column Flight view. It can be consumed with `pyarrow.ipc.open_stream(ds.open("/reports/orders").as_bytes())`; it is not CSV, SQL text or a database backup. `format_convert@1.0` still accepts CSV/NDJSON only; SQL table conversion is outside this adapter change.

Each request owns its JDBC connection, statement and result set. Reads request a read-only transaction with forward-only cursors. PostgreSQL uses autocommit=false and a positive fetch size to avoid buffering the entire result ([driver documentation](https://jdbc.postgresql.org/documentation/query/)). Arrow allocation is bounded by batch size; JDBC buffering depends on the driver, and individual large values still occupy memory. SQL results have no implicit row order. Listing/FlightInfo and DoGet use separate connections, so metadata and data are not a shared snapshot. Each reopened stream starts a new read, without retries, checkpoints or deduplication. Cancellation is checked between fetches/batches; an in-flight database operation may run until the driver/query timeout.

## Validation

```bash
cd dacp-server
sbt test
# From repository root: real mTLS Python/Flight/H2 and file reads
bash scripts/test-sql.sh
```
