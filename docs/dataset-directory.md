# 节点数据集目录

[English](dataset-directory_en.md)

DACP 节点通过 Flight `ListFlights` 发布本地 `DatasetCatalog.list()` 的结果。Union 不提供目录 Flight API、客户端目录方法或跨节点合集。

在 Union Web 成员列表点击“查看数据集”，可打开指定成员的目录页面。每次打开或刷新仅通过 Flight 查询该成员，超时为 5 秒；不保存、不缓存、不定时采集目录。失败页面明确报错，不展示旧目录。

## 访问边界

节点目录读取沿用 mTLS 信任，不需要数据访问或计算授权。Web 页面公开可读，Union 使用自身证书查询已邀请的指定成员，并校验成员身份；不接受任意远程地址。目录仅展示名称、URI、标题、描述和标签，不读取 Items 或数据内容。实际数据访问及执行仍需授权。

## 管理员：静态目录描述

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

`metadata` 是 Dataset 的公开元信息，与 provider 的 `source`、`items` 参数分开。标题最多 1024 字符，描述最多 32768 字符；标签最多 64 项，每项 1–256 字符。省略时发布名称与 URI，描述为空。完整节点配置仍需要 TLS 等常规字段。

## 开发者：动态目录描述

现有 `DatasetCatalog` 插件无需新增注册机制，在 `list()` 中返回公开描述即可：

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

完整注册、配置和租约实现见[动态目录插件](dataset-catalogs.md)。目录读取不会调用 `open()` 或扫描数据内容；后端目录查询失败必须抛出异常，而不是返回空列表。

## Shell

Shell 只连接 dacp-server，不连接 Union。启动参数必须是服务器地址，不接受 Dataset URI：

```bash
dacp-shell dacp://localhost:18861
```

证书通过 `DACP_CA`、`DACP_CERT`、`DACP_KEY`，或 `--ca`、`--cert`、`--key` 提供。

```text
dacp-shell> datasets
dacp-shell> use orders
dacp-shell> ls /
```

`datasets` 不接受参数，列出当前服务器上的数据集名称、标题、描述和 URI。启动及 `connect dacp://host:port` 后均未选择数据集，必须先 `use NAME`，才能执行 `ls`、`count`、`show`、`SELECT` 和 `run`。`use` 只选择当前服务器的数据集，数据访问仍需授权。Union Web 仅提供按成员查看目录的页面。

## `use` 被拒绝时

`datasets` 公开目录，而 `use` 会请求 Item 列表，因此仍需要数据访问授权。管理员应在节点配置中授权实际发布的数据集 URI，例如：

```yaml
allowed_uri_prefixes:
  - dacp://localhost:18861/data
```

URI 的主机和端口必须与节点 `public_host`、`port` 一致。修改后重启节点，客户端执行 `use data`。该前缀白名单适用于通过本节点 CA 验证的客户端，并不是针对某一个客户端证书单独授权。

## Python

```python
from pathlib import Path
from dacp import Source

credentials = dict(
    tls_root_certs=Path("examples/certs/ca.crt").read_bytes(),
    cert_chain=Path("examples/certs/client.crt").read_bytes(),
    private_key=Path("examples/certs/client.key").read_bytes(),
)
# 直接连接 dacp-server。
for dataset in Source.datasets("dacp://localhost:18861", **credentials):
    print(dataset.name, dataset.title, dataset.description, dataset.uri)
```

Scala 客户端使用 `Source.datasets(origin, tls)` 直接查询节点。节点的 `FlightInfo.app_metadata` 为 `DataSet` protobuf，descriptor 为 `datasets/<name>`。

## 验证

```bash
(cd dacp-server && sbt 'testOnly dacp.DatasetCatalogSpec dacp.DacpServerSpec')
(cd dacp-union-server && sbt test)
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```
