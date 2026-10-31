# 节点数据集目录

[English](dataset-directory.md)

DACP 节点发布本地目录；Union 在线合并已批准成员目录，以短名称 暴露自己的 DACP 命名空间。SDK 使用 `dacp.catalog` Action 获取包含完整性标记的 `Directory`，`ListFlights` 遇到不完整目录返回错误。详见 [Union 代理服务](union-proxy_zh.md)。

Union Web 分为三个独立页面：

- `/` 运行看板：任务、执行状态、DAG、策略与尝试记录。
- `/nodes` 节点列表：图形化展示成员节点、健康状态、容量、启动时间、运行时长和节点 Web 地址，状态每 5 秒更新。点击节点原位读取本地数据集，展示节点的本地 `name` 与 URI；目录读取超时为 5 秒，支持独立展开、重新读取和失败重试，不定时采集数据集列表。
- `/datasets` 数据目录：在线合并已批准成员的目录，展示 Union 分配的公开 `name`（如 `ds-k7m2-9wqr`）、JSON 描述及 Union URI，与客户端通过 `dacp.catalog` 看到的命名空间一致。支持页面内筛选和手动刷新。成员不可达时明确标记目录不完整，不将部分结果当作完整列表。

节点本地 `name` 与 Union 公开 `name` 是不同命名空间中的名称；数据集没有额外的 `id` 属性。目录列表不保存在 Union，名称映射持久化以保持公开名称稳定。节点页已加载的目录只在当前浏览器页面中保留；刷新节点列表会清除这些展示内容。

## 访问边界

目录、Item 列表及元数据允许通过 TLS 匿名访问。数据流及查询需要主体具有 `reader` 或 `runner` 角色。管理 Web 的访问边界见 [Union 代理服务](union-proxy_zh.md)。Union 使用自身证书查询已配置的指定成员，并校验成员身份；不接受任意远程地址。目录仅展示名称、URI 和 JSON 描述，不读取 Items 或数据内容。已发布数据的读取需要 reader 或 runner，执行需要 runner。

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
import xstream.data.DatasetInfo

override def list(): Seq[DatasetInfo] = Seq(
  DatasetInfo("orders", description =
    """{"summary":"Order data","owner":{"team":"sales"},"fields":["id","amount"],"updated":"daily"}"""
  )
)
```

字符串在公共插件契约处校验，并经 Flight、SDK、Shell 和 Web 原样传递。目录读取不会调用 `open()` 或扫描数据内容；后端目录查询失败必须抛出异常，而不是返回空列表。注册、配置和租约实现见[动态目录插件](dataset-catalogs_zh.md)。

## Shell

Shell 可连接 DACP 节点或提供 DACP 服务的 Union。启动参数必须是服务器地址，不接受 Dataset URI：

```bash
dacp-shell dacp://localhost:18861
```

证书通过 `DACP_CA`、`DACP_CERT`、`DACP_KEY`，或 `--ca`、`--cert`、`--key` 提供。

```text
dacp-shell> datasets
dacp-shell> use orders
dacp-shell> ls /
```

`datasets` 不接受参数，按 `#`、`Name`、`Description (JSON)` 三列显示行号、数据集名称和描述，不显示 URI。启动或 `connect dacp://host:port` 后，若完整目录中只有一个数据集，Shell 自动选择并打印名称；没有或有多个数据集时先执行 `use NAME` 或 `use NUMBER`，选中后才能执行 `ls`、`count`、`cat`、`SELECT` 和 `run`。`use` 只选择当前服务器的数据集，元信息匿名可看，数据读取需要 reader 或 runner。Union 使用自动分配的公开短名称，直接通过 `use NAME` 选择；不完整目录不会触发自动选择。行号从 1 开始，对应最近一次 `datasets` 输出，例如 `use 1`；再次执行 `datasets` 会更新行号映射，重新连接服务器会清空映射。纯数字参数按行号解释，越界会报错。

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
(cd dacp-server && sbt 'testOnly dacp.DataCatalogSpec dacp.DacpServerSpec')
(cd dacp-union-server && sbt test)
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```
