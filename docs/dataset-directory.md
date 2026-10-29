# Node dataset directories

[中文](dataset-directory_zh.md)

DACP nodes publish local catalogs; Union reads approved members online and exposes its own DACP namespace using short names. SDKs use the `dacp.catalog` Action to obtain a `Directory` with completeness fields. `ListFlights` fails for incomplete catalogs. See [Union proxy service](union-proxy.md).

The Union website has three independent pages:

- `/`: operations dashboard with jobs, execution states, DAGs, policies and attempt records.
- `/nodes`: graphical member nodes with health, capacity, startup time, uptime and node website links. Status updates every five seconds. Expanding a node reads its local dataset names and URIs inline with a five-second timeout. Cards expand independently and support manual refresh and retry; dataset lists are not polled.
- `/datasets`: the data catalog merges approved member catalogs online and displays Union-assigned public `name` values such as `ds-k7m2-9wqr`, JSON descriptions and Union URIs. This is the same namespace returned to clients by `dacp.catalog`. Filtering and manual refresh are available. Unreachable members produce an explicit incomplete-directory warning rather than presenting partial results as complete.

Local names and Union public names belong to different namespaces. Datasets have no separate `id` attribute. Union persists name mappings for stable public names, not dataset lists. Loaded local directories stay only in the current browser page; refreshing the node list clears them.

## Access boundaries

Node directory reads authenticate with an ordinary token or Union service credential and require no additional data-access grant. See [Union proxy service](union-proxy.md) for management Web access boundaries. Union uses its own certificate to query the selected configured member and verifies the member identity. Arbitrary remote addresses are not accepted. Directories show names, URIs and JSON descriptions without reading Items or data contents. Published datasets are readable by default; execution still requires its corresponding authorization.

## Administrators: static descriptions

```yaml
datasets:
  orders:
    provider: mixed
    description: |-
      {"summary":"Order data","owner":{"team":"sales"},"fields":["id","amount"],"updated":"daily"}
    items:
      /records:
        file: /srv/data/orders.csv
```

`description` is a string containing one valid JSON object. Its internal keys and nested structures are user-defined; DACP imposes no description schema. It is separate from provider `source` and `items` settings. Omission publishes `"{}"`. The string is limited to 32768 characters. Plain text, arrays, scalars, malformed JSON, multiple JSON values and YAML mappings are rejected. Public descriptions must not contain credentials or private storage settings. A complete node configuration still requires TLS and other fields.

## Developers: dynamic descriptions

Catalog plugins return the same JSON string through `DatasetInfo.description`:

```scala
import dacp.DatasetInfo

override def list(): Seq[DatasetInfo] = Seq(
  DatasetInfo("orders", description =
    """{"summary":"Order data","owner":{"team":"sales"},"fields":["id","amount"],"updated":"daily"}"""
  )
)
```

The string is validated at the shared plugin contract and preserved through Flight, SDKs, the shell and web views. Directory reads do not call `open()` or scan data contents. Backend discovery failures must throw rather than return empty lists. See [dynamic catalog plugins](dataset-catalogs.md) for registration, configuration and lease implementation.

## Shell

The shell connects to DACP nodes or a Union providing DACP services. The startup argument must be a server address, not a Dataset URI:

```bash
dacp-shell dacp://localhost:18861
```

Supply certificates through `DACP_CA`, `DACP_CERT`, `DACP_KEY`, or `--ca`, `--cert`, `--key`.

```text
dacp-shell> datasets
dacp-shell> use orders
dacp-shell> ls /
```

`datasets` accepts no arguments and shows row numbers, dataset names, and descriptions in the columns `#`, `Name`, and `Description (JSON)`, without URIs. On startup or `connect dacp://host:port`, the shell automatically selects and prints the sole Dataset when the complete catalog contains exactly one Dataset. With zero or multiple Datasets, select one with `use NAME` or `use NUMBER` before `ls`, `count`, `cat`, `SELECT`, or `run`. `use` selects a Dataset on the current server; published datasets are readable by default. Union uses automatically assigned public short names, selected directly with `use NAME`. Incomplete catalogs never trigger automatic selection. Row numbers start at 1 and refer to the latest `datasets` output, for example `use 1`. Running `datasets` updates the mapping; reconnecting clears it. Numeric arguments are interpreted as row numbers, with out-of-range values rejected.

## Python

```python
import json
from pathlib import Path
from dacp import Source

credentials = dict(
    tls_root_certs=Path("examples/certs/ca.crt").read_bytes(),
    cert_chain=Path("examples/certs/client.crt").read_bytes(),
    private_key=Path("examples/certs/client.key").read_bytes(),
)
# Connect directly to dacp-server.
for dataset in Source.datasets("dacp://localhost:18861", **credentials):
    print(dataset.name, dataset.description, dataset.uri)
    print(json.loads(dataset.description).get("owner", {}).get("team"))
```

The Scala client uses `Source.datasets(origin, tls)` to query nodes directly. Node `FlightInfo.app_metadata` contains the `DataSet` protobuf; descriptors use `datasets/<name>`.

## Validation

```bash
(cd dacp-server && sbt 'testOnly dacp.DataCatalogSpec dacp.DacpServerSpec')
(cd dacp-union-server && sbt test)
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```
