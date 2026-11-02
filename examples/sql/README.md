# Serve files and SQL tables from one DACP node

[中文](README_zh.md)

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

## Configure the database backend

For PostgreSQL configuration, JDBC options, SQL-to-Arrow type mapping, cursors and streaming behavior, see the [XStream JDBC dataset guide](https://github.com/rdcn-link/xstream-codex/blob/main/docs/jdbc-datasets.md). Combine its catalog settings with DACP listener, TLS and credential configuration to publish data. Database credentials remain on the node; see [DACP authentication and access control](../../docs/security.md) for remote permissions.

## Validation

```bash
cd dacp-server
sbt test
# From repository root: real mTLS Python/Flight/H2 and file reads
bash scripts/test-sql.sh
```
