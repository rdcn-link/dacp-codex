# 用户指南

[English](README_en.md) · [文档目录](../README.md) · [开发者指南](../developers/README.md)

面向浏览数据、使用 SDK、执行算子，以及使用已有插件配置 DACP 节点的用户。除非另有说明，命令均从项目根目录执行。实现新后端、格式或算子请阅读开发者指南。

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

默认服务地址为 `localhost:8815`，配置见 [examples/server.yaml](../../examples/server.yaml)。启动日志会列出已发布的 Dataset 名称和 URI；当前没有数据集发现接口，连接时需要使用已知 URI。

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

### 记录过滤、列投影与行数限制

两个 SDK 都支持请求服务端执行记录查询：

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

执行顺序为过滤 → 列投影 → 行数限制。谓词使用公开列名和带类型的字面量，支持比较、布尔逻辑、成员判断和空值检查；过滤条件可引用输出中未包含的列。SQL 和 Parquet 将支持的操作下推到读取器，其他处理器使用有界的服务端批次过滤。达到行数限制后停止读取；bytes 视图不接受这些选项。Item 元数据和精确计数仍描述完整数据。

参见[记录查询语义与插件契约](../../docs/records-scans.md)，了解 Python/Scala 示例、空值和类型规则、校验以及各来源的优化范围。

### dacp-shell

`ls` 以带外边框的表格显示 #、Path、Format、Bytes、Records 和 Modified (UTC)，首列为从 1 开始的条目编号。未知值显示为 `-`，时间格式为 `2026-10-16 06:22:02`；终端中集合路径以加粗青色高亮。

`show` 接受路径或最近一次列表中的编号；默认优先显示记录，否则显示十六进制。记录使用带列名的表格，`text` 显示文本，`hex` 显示偏移量、十六进制及 ASCII。默认最多预览 20 行或 4096 字节，可通过 `--rows` 和 `--bytes` 调整。

使用 `use DATASET` 切换同一服务器上的数据集，`connect URI` 连接其他位置，`help` 查看命令。证书也可通过 `--ca`、`--cert`、`--key` 指定。

在 shell 提示符下直接输入单行 SQL 查询：

```sql
SELECT amount FROM "/reports/orders" WHERE id > 1 LIMIT 10
SELECT amount FROM 2 WHERE id > 1 LIMIT 10
SELECT * FROM "/reports/orders" WHERE id IN (1, 2) AND amount IS NOT NULL
```

shell 使用 SQLGlot 将支持的 SQL 子集转换为与 SDK 相同的远程扫描请求。`FROM` 可以是当前 Dataset 内用双引号包围的绝对 Item 路径，也可以是最近一次 `ls` 结果中的条目编号（例如 `FROM 2`）。编号跟随最新列表的顺序，包括 `ls PATH`；编号超出范围时会报错。支持 `SELECT *` 或公开列名列表，以及可选的 `WHERE` 和 `LIMIT`；SQL 关键字不区分大小写。结果以带外边框的表格显示。`--rows` 预览上限仍然有效：发送给服务器的行数限制取 SQL `LIMIT` 和预览上限中的较小值；省略 `LIMIT` 时使用预览上限。输入 `help select` 查看语法。不支持的连接、排序、聚合、别名和子查询会明确报错。参见 [SQL shell 说明](../../docs/records-scans.md#sql-in-dacp-shell)。


### 帮助与命令示例

输入 `help` 查看命令列表及简短示例；输入 `help show`、`help select`、`help run`、`help submit` 查看具体语法和用法。帮助中的路径为示例，请使用 Dataset 实际公布的路径。条目编号对应最近一次 `ls` 的结果。

### Scala SDK

独立的 [Scala 2.13 SDK](../../dacp-client-scala/README.md) 提供相同的数据访问模型，不依赖服务端工程。先发布到本地 Ivy 仓库：

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

默认示例同时提供 `data1`、`csv_data`（custom）和 `documents`（directory）。在 shell 中执行 `use documents`、`ls /`，或读取 `/nested/orders.csv`。SQLDB 的完整初始化示例见 [SQL 示例](../../examples/sql/README.md)。添加 MongoDB 等新来源时，可通过 provider JAR 扩展，无需修改客户端；开发接口见 [Dataset provider 指南](../../docs/dataset-providers.md)。

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

格式细节、Excel 行为、归档限制和 schema 规则见 [技术参考](../../docs/reference.md)，第三方处理器开发见 [格式扩展](../../docs/format-handlers.md)。

### 元数据与计数

Item 可携带修改时间、字节大小和精确记录数。未知值与 `0` 不同；所有 folder 的这三个属性始终为未知，不遍历子项来计算。

示例服务启用了 `count_file_records_on_startup: true`：在发布时对 custom 文件映射及目录初次发现的 CSV、NDJSON、Excel 等记录文件计算一次精确记录数，因此 `/events` 和 `/reports/orders` 的 Records 显示为 `2`。此选项默认关闭；启用后会增加启动时间，计数失败会使启动失败。SQL、归档子项和所有 folder 不参与启动计数。目录中新发现的文件不会在浏览时触发计数，可显式查询或重启后填充。

普通浏览不扫描文件计数，也不执行 SQL `COUNT(*)`。已保存的文件计数在大小或修改时间变化后回到未知；重启可重新填充，也可用 `count` 显式查询最新数量，但显式查询不更新已保存的列表计数。Parquet 仍可从 footer 获取记录数。元数据与之后读取的内容不构成原子快照。

## 数据协作与任务

DACP 通过 YAML DAG 组织任务。协调节点解析依赖，默认将任务放在第一个输入所在节点，也可显式指定执行节点；各节点通过 mTLS Flight DoExchange 协作。每个子任务使用独立 Docker 容器，命名端口消费数据项流，输出直接流向客户端或下游任务，不保存输出数据集。

运行任务还需要 Linux、`mkfifo` 和可访问的 Docker Engine。从项目根目录构建转换算子，再启动服务；若快速开始的服务仍在运行，先将其停止：

```bash
scripts/build-operator-image.sh
cd dacp-server
sbt 'run ../examples/server.yaml'
```

连接 shell 后运行 `submit examples/convert.yaml`（在项目根目录启动 shell），或 `run format_convert@1.1 --input source=/reports/orders`。shell 等待并逐块显示结果；Ctrl-C 取消任务。

更多说明：

- [双节点示例](../../examples/multi-node/README.md)：任务放置、远端输入及输出消费。
- [Scala 算子仓库与网站](../../dacp-operator-repository/README.md)：公共搜索、版本详情、下载与不可变算子发布。
- [自定义算子编写与上传](../../docs/operator-authoring.md)：Scala/Python 完整源码、构建、发布、远程上传和任务执行。
- [任务与算子参考](../../docs/reference.md#任务执行与算子)：DAG、容器输入输出契约和转换参数。

使用 `scripts/setup-operator-repository.sh --host 0.0.0.0 --port 8820` 启动包含初始算子的仓库，然后打开 `http://localhost:8820/`，无需登录即可搜索、查看和下载算子。HTTPS 部署参见仓库指南。

算子可共享依赖环境和代码包。初始算子通过薄 JAR 使用 `dacp/operator-jvm:1.1`；也支持在共享运行时容器中执行 Python 源码和 wheel。运行时与代码按 SHA-256 存储和缓存，每个节点只下载一次相同制品。详见 [算子打包设计](../../docs/operator-packaging.md) 和 [Python 示例](../../examples/operators/python/README.md)。


`run` 和 `submit` 在完成、失败或取消后打印 `Execution time: 1.234 s`。这是 shell 从提交开始到结果流关闭的总耗时，包括连接、运行时准备、执行、传输、结果显示及客户端清理；不包含输入 heredoc 的时间，也不是单个服务器任务的 CPU 时间。多任务作业显示整个作业的一次总耗时。

## 安全与运行边界

所有连接强制 mTLS，并通过 URI 前缀白名单授权。公开元数据、ticket 和数据服务错误隐藏私有存储绑定；数据内容仍包含源数据自身的字段和值。Python 的 `DacpError` 和 Scala 的 `DacpException` 提供错误码和可读消息，shell 显示简洁提示。

当前白名单在节点级生效，不按证书主体划分权限。读取失败后从头重开，不支持流内重试、checkpoint 或恢复。任务输出仅为实时流，客户端须消费或取消；不提供保存的结果 URI，重启不恢复任务。完整配置和协议说明见 [技术参考](../../docs/reference.md#协议与配置)。

DEBUG 模式下可通过 `run --runtime python@1.0 --input source=/orders <<END` 输入 heredoc 源码；不支持 `--source` 或 `--code`。服务端默认为 NORMAL，并在协调与执行节点拒绝源码任务。配置、Python/Scala 示例与插件契约见[源码算子](../../docs/operator-source.md)。

原生可执行程序可通过 `command` 执行器使用 stdin/stdout，无需在程序内实现 DACP 协议。支持单 bytes 输入和输出，仍在容器内隔离执行。见[配置及发布指南](../../docs/native-commands.md)和 [C 示例](../../examples/operators/native/README.md)。

## 启动 DEBUG 日志

启动时，`dacp.Main` 的 DEBUG 日志打印 `remote_operator_library_urls` 的数量及按配置顺序排列的每个 URL；空列表显示数量 0。普通 INFO 日志不打印这些 URL。stage 服务端后，可从项目根目录运行：

```bash
(cd dacp-server && target/universal/stage/bin/dacp-server \
  -J-Dorg.slf4j.simpleLogger.log.dacp.Main=debug ../examples/server.yaml)
```

这是日志级别设置，与允许提交源码的 `mode: DEBUG` 独立。启用 DEBUG 日志不会开放源码执行。

## 查看本机运行时

目前 shell 没有 `runtimes` 命令，服务端也没有运行时发现接口。在服务器宿主机执行以下命令可列出 Docker 中已安装的镜像：

```bash
docker image ls
```

`run --runtime` 可使用的 ID 来自服务端配置中的 `source_runtimes`，例如 `examples/source-debug.yaml` 中的 `python@1.0` 与 `scala@1.0`。Docker 镜像列表不等于已配置的源码运行时列表；普通仓库算子的运行时则由各算子定义或 manifest 指定。

## 算子审查材料

支持发布可选的源码包、仓库修订版本、许可证声明与文本、构建说明和 SBOM。公开页面提供下载并标注缺失信息。参见[源码与审查信息](../operator-provenance.md)。

## Union 协作作业

通过 [Union 服务器](../../dacp-union-server/README.md) 提交显式节点 DAG、订阅执行状态和流量，并读取最终流。节点邀请和审批、Python 与 Scala 用法见模块文档；[双节点示例](../../examples/union/README.md) 演示本地销量汇总与跨节点价格融合。

DACP 与 Union 默认持久化全部执行记录；Union 看板支持重启后的历史查询。本地查询命令及保存目录见 [执行记录](../execution-history.md)。
