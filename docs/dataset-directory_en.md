# Node dataset directories

[中文](dataset-directory.md)

DACP nodes publish local `DatasetCatalog.list()` results through Flight `ListFlights`. Union exposes no directory Flight API, client directory methods, or cross-node aggregate.

Click “查看数据集” in the Union website member list to open one member’s directory. Opening or refreshing the page queries only that member over Flight, with a 5-second timeout. Directories are never saved, cached, or polled. Failed reads report an error instead of showing old entries.

## Access boundaries

Node directory reads retain mTLS trust and require no data-access or execution grant. Web pages are public; Union uses its own certificate to query the selected invited member and verifies the member identity. Arbitrary remote addresses are not accepted. Directories show names, URIs, titles, descriptions, and tags, without reading Items or data contents. Data access and execution still require authorization.

## Administrators: static descriptions

```yaml
datasets:
  orders:
    provider: mixed
    metadata:
      title: 订单数据
      description: 用于跨中心销售统计的订单数据集
      tags: [sales, orders]
    items:
      /records:
        file: /srv/data/orders.csv
```

`metadata` contains public Dataset information, separately from provider `source` and `items` settings. Titles are limited to 1024 characters, descriptions to 32768 characters, and tags to 64 entries of 1–256 characters each. Omission publishes the name and URI with empty descriptions. A complete node configuration still requires the normal TLS and other fields.

## Developers: dynamic descriptions

Existing `DatasetCatalog` plugins return public descriptions from `list()` without introducing another registration mechanism:

```scala
import dacp.{DatasetInfo, DatasetMetadata}

override def list(): Seq[DatasetInfo] = Seq(
  DatasetInfo("orders", DatasetMetadata(
    title = "订单数据",
    description = "最近一年的订单；每日更新",
    tags = Seq("sales", "daily")
  ))
)
```

See [dynamic catalog plugins](dataset-catalogs_en.md) for complete registration, configuration, and lease implementation. Directory reads do not call `open()` or scan data contents. Backend discovery failures must throw rather than return empty lists.

## Shell

The shell connects only to dacp-server, not to Union. The startup argument must be a server address, not a Dataset URI:

```bash
dacp-shell dacp://localhost:18861
```

Supply certificates through `DACP_CA`, `DACP_CERT`, `DACP_KEY`, or `--ca`, `--cert`, `--key`.

```text
dacp-shell> datasets
dacp-shell> use orders
dacp-shell> ls /
```

`datasets` accepts no arguments and lists names, titles, descriptions, and URIs on the current server. Startup and `connect dacp://host:port` leave no Dataset selected. Run `use NAME` before `ls`, `count`, `show`, `SELECT`, or `run`. `use` selects a Dataset on the current server; data access still requires authorization. Union provides only a per-member directory page on its website.

## When `use` is denied

`datasets` reads a public directory, while `use` requests an Item listing and therefore still requires data-access authorization. Administrators should allow the published Dataset URI in node configuration, for example:

```yaml
allowed_uri_prefixes:
  - dacp://localhost:18861/data
```

The URI host and port must match the node's `public_host` and `port`. Restart the node after changing configuration, then run `use data`. This prefix allowlist applies to clients verified by the node's CA; it does not authorize one particular client certificate separately.

## Python

```python
from pathlib import Path
from dacp import Source

credentials = dict(
    tls_root_certs=Path("examples/certs/ca.crt").read_bytes(),
    cert_chain=Path("examples/certs/client.crt").read_bytes(),
    private_key=Path("examples/certs/client.key").read_bytes(),
)
# The same interface connects to either an ordinary node or a Union.
for dataset in Source.datasets("dacp://localhost:18861", **credentials):
    print(dataset.name, dataset.title, dataset.description, dataset.uri)
```

The Scala client uses `Source.datasets(origin, tls)` to query nodes directly. Node `FlightInfo.app_metadata` contains the `DataSet` protobuf; descriptors use `datasets/<name>`.

## Validation

```bash
(cd dacp-server && sbt 'testOnly dacp.DatasetCatalogSpec dacp.DacpServerSpec')
(cd dacp-union-server && sbt test)
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```
