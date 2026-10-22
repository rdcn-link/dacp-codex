# 节点数据集目录

[English](dataset-directory_en.md)

DACP 节点通过 Flight `ListFlights` 发布本地 `DatasetCatalog.list()` 的结果。Union 不提供目录 Flight API、客户端目录方法或跨节点合集。

在 Union Web 成员列表点击“查看数据集”，可打开指定成员的目录页面。每次打开或刷新仅通过 Flight 查询该成员，超时为 5 秒；不保存、不缓存、不定时采集目录。失败页面明确报错，不展示旧目录。

## 访问边界

节点目录读取沿用 mTLS 信任，不需要数据访问或计算授权。Web 页面公开可读，Union 使用自身证书查询已邀请的指定成员，并校验成员身份；不接受任意远程地址。目录仅展示名称、URI 和 JSON 描述，不读取 Items 或数据内容。已发布的数据集默认可读取；计算执行仍需相应授权。

## 管理员：静态目录描述

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

`description` 是包含一个合法 JSON 对象的字符串。内部字段及嵌套结构由用户自行定义，DACP 不约定描述字段的 schema。它与 provider 的 `source`、`items` 参数分开。省略时发布 `"{}"`。字符串最多 32768 字符；普通文本、数组、标量、非法 JSON、多个 JSON 值和 YAML 映射均会被拒绝。公开描述中不要包含凭据或私有存储配置。完整节点配置仍需要 TLS 等常规字段。

## 开发者：动态目录描述

目录插件通过 `DatasetInfo.description` 返回相同的 JSON 字符串：

```scala
import dacp.DatasetInfo

override def list(): Seq[DatasetInfo] = Seq(
  DatasetInfo("orders", description =
    """{"summary":"Order data","owner":{"team":"sales"},"fields":["id","amount"],"updated":"daily"}"""
  )
)
```

字符串在公共插件契约处校验，并经 Flight、SDK、Shell 和 Web 原样传递。目录读取不会调用 `open()` 或扫描数据内容；后端目录查询失败必须抛出异常，而不是返回空列表。注册、配置和租约实现见[动态目录插件](dataset-catalogs.md)。

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

`datasets` 不接受参数，列出当前服务器上的数据集名称、JSON 描述和 URI。启动或 `connect dacp://host:port` 后，若目录中只有一个数据集，Shell 自动选择并打印名称；没有或有多个数据集时先执行 `use NAME`，选中后才能执行 `ls`、`count`、`show`、`SELECT` 和 `run`。`use` 只选择当前服务器的数据集，已发布的数据集默认可读取。Union Web 仅提供按成员查看目录的页面。

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
# 直接连接 dacp-server。
for dataset in Source.datasets("dacp://localhost:18861", **credentials):
    print(dataset.name, dataset.description, dataset.uri)
    print(json.loads(dataset.description).get("owner", {}).get("team"))
```

Scala 客户端使用 `Source.datasets(origin, tls)` 直接查询节点。节点的 `FlightInfo.app_metadata` 为 `DataSet` protobuf，descriptor 为 `datasets/<name>`。

## 验证

```bash
(cd dacp-server && sbt 'testOnly dacp.DatasetCatalogSpec dacp.DacpServerSpec')
(cd dacp-union-server && sbt test)
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```
