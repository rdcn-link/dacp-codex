# DACP

<img src="assets/dacp-logo.png" alt="DACP 标志：连接的节点组成 Collaborative 的首字母 C" width="480">

[English](README_en.md)

DACP 是基于 Apache Arrow Flight 的数据协作协议与原型实现。各节点通过统一接口发布数据，客户端按逻辑名称浏览文件、SQL 表和 归档与压缩文件，也可以提交 YAML 任务，让计算在数据所在节点执行。

项目提供 Scala 2.13 服务端、Python 和 Scala 客户端 SDK，以及交互式终端 `dacp-shell`。节点通过 URI 直接寻址，以 mTLS 建立连接，无需中心注册服务。

## 数据模型

DACP 用 Dataset、Item 和 Stream 描述数据访问：

| 概念 | 含义 | 示例 |
|---|---|---|
| Dataset | 节点发布的命名逻辑空间，可组合不同来源的数据 | `dacp://localhost:8815/data1` |
| Item | Dataset 内具有绝对逻辑路径的资源 | `/reports/orders` |
| `folder` Item | 包含子 Item 的集合；归档和压缩集合还提供原始文件的 bytes 视图 | `/reports`、`/archive` |
| `data` Item | 可读取的数据，具有格式和支持的视图 | CSV 文件、SQL 表、图片 |
| Stream | 打开 Item 后取得的读取接口 | Arrow 记录批次或字节流 |

Dataset 是逻辑空间，可以同时包含文件和 SQL 表。公开路径由管理员配置，不暴露服务器文件路径或数据库表名。所有 Item 路径从 `/` 开始；根集合 `/` 是隐式的，不出现在清单中。

**kind** 表示 Item 是否可包含子项，**format** 表示数据格式，**views** 表示读取能力。客户端应检查 views，而不是根据文件名判断能否读取记录。

## 快速开始

运行数据服务需要 JDK 17、sbt 1.10.x 和 Python 3.10+。普通数据浏览无需 Docker；执行容器任务时再配置 Docker。

在项目目录中安装 Python SDK、生成本地示例证书并启动服务：

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt run
```

默认服务地址为 `localhost:8815`，配置见 [examples/server.yaml](examples/server.yaml)。启动日志会列出已发布的 Dataset 名称和 URI；当前没有数据集发现接口，连接时需要使用已知 URI。

在另一个终端中，从项目根目录连接：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/dacp-shell dacp://localhost:8815/data1
```

进入 shell 后：

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

示例数据集包含 `/events`、`/reports/orders`、`/image` 和空集合 `/empty`，另有 ZIP 集合 `/archive` 与 `/images`、TAR 集合 `/bundle`、压缩 TAR 集合 `/bundle_gzip` 和 GZIP 集合 `/orders.csv.gz`。`/archive` 包含 CSV、NDJSON 和空集合；`/images` 包含 PNG，示范大写 `.ZIP` 扩展名。也可运行 `.venv/bin/python examples/client.py` 验证 SDK 读取。生成的证书仅用于本地示例；连接远端时替换 URI 和证书。

## 浏览与读取

### Python SDK

沿用快速开始中的证书环境变量：

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

| 操作 | 行为 |
|---|---|
| `ds.list()` | 递归列出所有 Item |
| `ds.list("/")` | 列出根集合的直接子项 |
| `ds.list("/reports")` | 列出指定集合的直接子项 |
| `ds.open(path)` | 打开可读取的 Item，返回 Stream |
| `ds.count(path)` | 显式请求精确记录数；非记录数据和集合返回未知 |

路径必须以 `/` 开始，不接受 `.`、`..`、重复分隔符或末尾 `/`（根路径除外）。`records` 视图返回 Arrow RecordBatch；`bytes` 视图提供增量字节读取。文件的 bytes 是原始内容，SQL 的 bytes 是 Arrow IPC。

Stream 必须在所属 Dataset 关闭前使用。提前停止记录读取时关闭迭代器，字节读取器使用 context manager；失败后重新打开并从头读取，不支持断点续传。Python 也支持通过完整 Item URI 直接打开 Stream。

### dacp-shell

`ls` 以带外边框的表格显示 Path、Format、Bytes、Records 和 Modified (UTC)。未知值显示为 `-`，时间格式为 `2026-10-16 06:22:02`；终端中集合路径以加粗青色高亮。

`show` 接受路径或最近一次列表中的编号；默认优先显示记录，否则显示十六进制。记录使用带列名的表格，`text` 显示文本，`hex` 显示偏移量、十六进制及 ASCII。默认最多预览 20 行或 4096 字节，可通过 `--rows` 和 `--bytes` 调整。

使用 `use DATASET` 切换同一服务器上的数据集，`connect URI` 连接其他位置，`help` 查看命令。证书也可通过 `--ca`、`--cert`、`--key` 指定。

### Scala SDK

独立的 [Scala 2.13 SDK](dacp-client-scala/README.md) 提供相同的数据访问模型，不依赖服务端工程。先发布到本地 Ivy 仓库：

```bash
cd dacp-client-scala
sbt test publishLocal
```

在 Scala 应用的 `build.sbt` 中添加：

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

使用 `Source.open`、`Dataset.list`、`Dataset.open`、`asRecords()` 和 `asBytes()`，通过 `scala.util.Using` 管理资源。SDK 文档提供完整示例、TLS 配置、超时及 Arrow JVM 参数；构件尚未发布到 Maven Central。

## 构建与发布 Dataset

Dataset 由可扩展的 provider 构建。内置三种方式，对客户端提供相同的 `list`、`open` 和 `count`：

| provider | 构建方式 | 私有来源 |
|---|---|---|
| `directory` | 自动发现根目录中的文件、子目录和归档 | `source.root` |
| `sqldb` | 从一个 SQL 连接发布具有公开别名的表 | `source.connection` |
| `custom` | 逐个定义 Item，可组合文件、SQL 表及逻辑集合 | Item 的 `file`、`sql`、`table` |

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

这是配置片段，需与监听地址、TLS 和 URI 白名单合并，并替换示例路径及数据库连接。相对路径以服务进程工作目录为基准。

目录 Dataset 将根目录内的相对路径作为公开逻辑路径，例如 `/reports/orders.csv`；空目录也是 folder，受支持的归档可继续浏览。后续列举会反映文件新增、删除和修改。根目录保持私有，所有符号链接均忽略；不会访问根目录之外的数据。目录的 `reader_overrides` 是可选读取选项覆盖，支持格式、工作表、表头行和 schema，不用于重命名或额外添加来源。

SQL Dataset 的 `tables` 将公开路径绑定到选定连接的私有表名，不自动暴露所有表。`custom` 保留逐项映射和混合来源的灵活性。每个 Dataset 必须显式指定 `provider`。

默认示例同时提供 `data1`、`csv_data`（custom）和 `documents`（directory）。在 shell 中执行 `use documents`、`ls /`，或读取 `/nested/orders.csv`。SQLDB 的完整初始化示例见 [SQL 示例](examples/sql/README.md)。添加 MongoDB 等新来源时，可通过 provider JAR 扩展，无需修改客户端；开发接口见 [Dataset provider 指南](docs/dataset-providers.md)。

### 自定义 Dataset provider

用户定义的 provider 是服务端 Scala/JVM 插件。`provider` 选择插件，`source` 传入该 Dataset 实例的私有参数；同一个 provider 可用不同参数构建多个 Dataset。插件负责校验参数，客户端只看到公开路径和数据。

下面是一个可发布单个 CSV 文件的完整 provider 示例，固定公开路径为 `/records`：

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

将代码编译为 JAR，仅依赖独立的 `dacp-plugin-api`（使用与服务器匹配的版本和 Scala 版本，以 `Provided` 方式依赖）。在 JAR 内创建以下服务注册文件：

```text
META-INF/services/dacp.DatasetProvider
```

文件内容为实现类全名：

```text
example.CsvFileProvider
```

将插件及其所需依赖 JAR 放入 `plugin_directory`（不要复制服务器已提供的 API 和依赖），重启时使用以下配置片段，并合并监听地址、TLS 和 URI 白名单：

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  measurements:
    provider: csv-file
    source:
      file: /private/measurements.csv
```

客户端读取 `/records`，不会获得 `source.file`。需要逐项映射的插件默认通过 `items` 接收参数，也可覆盖 `DatasetProvider.itemSettingsKey` 声明自己的配置键；解析后的映射统一由 `definition.items` 提供。`source` 可包含插件自定义的嵌套参数，插件必须自行校验类型、必填项和未知参数。

目录 provider 使用 `reader_overrides`，SQLDB 使用 `tables`，custom 使用 `items`。目录和 SQLDB 不接受 `items`。原生后端（例如未来的 MongoDB provider）可直接实现 `PublishedDataset` 和 `DatasetAccess`，提供列举、元数据、计数与流式读取，无需映射成文件或 SQL。完整接口和取消、资源释放要求见 [provider 开发指南](docs/dataset-providers.md)。MongoDB 尚未实现。

### 文件与结构化内容插件

文件插件实现 `FormatHandler`，在 `META-INF/services/dacp.FormatHandler` 注册。处理器自行校验私有 `options` 参数；例如 Excel 的设置为 `options: {sheet: Orders, header_row: 2}`。通用 `format` 和 `schema` 保持在读取配置顶层。

需要浏览文件内部逻辑对象时，实现 `StructuredFileHandler`，通过同一个格式服务文件注册。其 `openStructure` 返回 `DatasetAccess`，可直接流式读取内部对象，不要求提取为临时文件。文件根表现为带原始 bytes 视图的 folder，子对象显式声明 `records` 或 `bytes`。归档读取器可实现 `ContainerReader` 并在 `META-INF/services/dacp.ContainerReader` 注册，无需再修改核心格式注册表。

独立插件 API 位于 [dacp-plugin-api](dacp-plugin-api)，可运行的贡献示例和构建、安装步骤位于 [examples/plugins](examples/plugins/README.md)。`PluginChecks.listing` 提供可复用的元数据契约检查。服务端会关闭每次请求的访问句柄，并在关闭服务时释放 provider 资源。NetCDF、FITS 的具体解析插件及数组、图像新视图尚未实现；现有接口允许贡献者定义文件内部对象，不能静默把数组展开为记录。

### 格式与视图

| 来源 | format | kind | views |
|---|---|---|---|
| CSV | `csv` | `data` | `records`, `bytes` |
| NDJSON（`.jsonl`、`.ndjson`） | `ndjson` | `data` | `records`, `bytes` |
| 普通 JSON | `json` | `data` | `bytes` |
| Excel（`.xls`、`.xlsx`） | `excel` | `data` | `records`, `bytes` |
| 带 DACP Arrow schema 元数据的 Parquet | `parquet` | `data` | `records`, `bytes` |
| SQL 表 | `sql` | `data` | `records`, `bytes` |
| ZIP | `zip` | `folder` | `bytes` |
| TAR | `tar` | `folder` | `bytes` |
| GZIP（`.gz`） | `gzip` | `folder` | `bytes` |
| 压缩 TAR（`.tar.gz`、`.tgz`） | `tar-gzip` | `folder` | `bytes` |
| 其他文件 | `bin` | `data` | `bytes` |

格式检测支持显式 `format`、文件签名和不区分大小写的扩展名，优先识别 `.tar.gz` 等组合后缀；未知格式回退为二进制。按行存储对象的 `.json` 文件可用 `format: ndjson` 启用记录读取。Excel 支持指定工作表、表头行及显式 schema。

ZIP、TAR、压缩 TAR 的子项可按 `/archive/reports/orders.csv` 浏览和读取；打开集合的 bytes 视图读取原始归档。普通 GZIP 提供一个解压后的子项，例如 `/orders.csv.gz/orders.csv`，其名称来自公开别名，不使用私有文件名。别名不以 `.gz` 结尾时，子项名为 `content`。

ZIP 列举只读取目录索引；TAR 需要顺序扫描，`.tar.gz` / `.tgz` 还需要解压，因此大归档的首次列举和读取可能较慢。读取条目时只将所需条目暂存到临时文件。嵌套归档暂不展开；`.bz2`、`.xz` 和 `.zst` 目前仍作为普通二进制数据读取。

格式细节、Excel 行为、归档限制和 schema 规则见 [技术参考](docs/reference.md)，第三方处理器开发见 [格式扩展](docs/format-handlers.md)。

### 元数据与计数

Item 可携带修改时间、字节大小和精确记录数。未知值与 `0` 不同；所有 folder 的这三个属性始终为未知，不遍历子项来计算。

示例服务启用了 `count_file_records_on_startup: true`：在发布时对 custom 文件映射及目录初次发现的 CSV、NDJSON、Excel 等记录文件计算一次精确记录数，因此 `/events` 和 `/reports/orders` 的 Records 显示为 `2`。此选项默认关闭；启用后会增加启动时间，计数失败会使启动失败。SQL、归档子项和所有 folder 不参与启动计数。目录中新发现的文件不会在浏览时触发计数，可显式查询或重启后填充。

普通浏览不扫描文件计数，也不执行 SQL `COUNT(*)`。已保存的文件计数在大小或修改时间变化后回到未知；重启可重新填充，也可用 `count` 显式查询最新数量，但显式查询不更新已保存的列表计数。Parquet 仍可从 footer 获取记录数。元数据与之后读取的内容不构成原子快照。

## 数据协作与任务

DACP 通过 YAML DAG 组织任务。协调节点解析依赖，默认将任务放在第一个输入所在节点，也可显式指定执行节点；各节点通过 mTLS Flight action 协作。每个子任务使用独立 Docker 容器，输出在执行节点发布为临时 Dataset，由 TTL 管理。

运行任务还需要 Linux、`mkfifo` 和可访问的 Docker Engine。从项目根目录构建转换算子，再启动示例任务；若快速开始的服务仍在运行，先将其停止：

```bash
docker build -f operators/format_convert/1.0/Dockerfile -t dacp/format-convert:1.0 .
cd dacp-server
sbt 'run ../examples/server.yaml ../examples/convert.yaml'
```

成功后日志会输出 `s1`、`s2` 的 URI。沿用证书环境变量，在项目根目录运行 `.venv/bin/python examples/verify_job.py` 检查结果。

更多说明：

- [双节点示例](examples/multi-node/README.md)：任务放置、远端输入及输出消费。
- [Scala 算子库 HTTP 服务](dacp-operator-library/README.md)：发布不可变算子版本，下载、校验和缓存镜像。
- [任务与算子参考](docs/reference.md#任务执行与算子)：DAG、容器输入输出契约和转换参数。

## 安全与运行边界

所有连接强制 mTLS，并通过 URI 前缀白名单授权。公开元数据、ticket 和数据服务错误隐藏私有存储绑定；数据内容仍包含源数据自身的字段和值。Python 的 `DacpError` 和 Scala 的 `DacpException` 提供错误码和可读消息，shell 显示简洁提示。

当前白名单在节点级生效，不按证书主体划分权限。读取失败后从头重开，不支持流内重试、checkpoint 或恢复。任务输出注册表仅驻内存，输出在 TTL 到期或服务关闭时清理；重启不恢复任务。完整配置和协议说明见 [技术参考](docs/reference.md#协议与配置)。

## 开发与验证

DACP 优先采用插件机制扩展。新的数据集后端、文件格式和处理算子应通过实现公开扩展契约并安装插件加入，避免修改核心分发逻辑。内置实现与社区实现应使用相同契约，插件负责自己的参数校验与第三方依赖，客户端通过统一路径和能力访问数据。贡献方式、现有机制与科学数据扩展边界见 [插件贡献指南](docs/plugins.md)。

从项目根目录运行：

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
```

服务启动并设置证书环境变量后，运行 SDK 与安全检查：

```bash
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
```

跨节点容器联调使用 `bash scripts/test-collaboration.sh`，需要 Docker。测试覆盖命名空间、格式读取、归档浏览、SQL 流、schema 校验、授权、输出过期和任务协作。

协议定义位于 [proto/dacp.proto](proto/dacp.proto)，服务端位于 [dacp-server](dacp-server)，Python SDK 位于 [dacp-client](dacp-client)，Scala SDK 位于 [dacp-client-scala](dacp-client-scala)。
