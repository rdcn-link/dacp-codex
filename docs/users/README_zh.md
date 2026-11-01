# 用户指南

[English](README.md) · [文档目录](../README_zh.md) · [开发者指南](../developers/README_zh.md)

Job 的输入和最终输出使用 `step[:port]`，端口缺省为 `result`；外部数据通过 `read_item@1.0` 接入，仅通过流连接建立依赖。详见[流式作业](../jobs_zh.md).

面向浏览数据、使用 SDK、执行算子，以及使用已有插件配置 DACP 节点的用户。除非另有说明，命令均从项目根目录执行。实现新后端、格式或算子请阅读开发者指南。

## 访问已发布的数据

用 `datasets` 发现数据集，用 `use` 选择数据集，再以 Item 的逻辑路径列举或读取。路径及可用视图以服务器返回的信息为准；底层文件路径不会公开。数据概念和格式示例见 [XStream 数据模型](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-model_zh.md)。

## 快速开始

运行数据服务需要 JDK 17、sbt 1.10.x 和 Python 3.10+。普通数据浏览无需 Docker；执行容器任务时再配置 Docker。

在项目目录中安装 Python SDK、生成本地示例证书并启动服务：

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt stage
(umask 077
 mkdir -p .dacp-security/8815
 target/universal/stage/bin/dacp-admin create-account .dacp-security/8815 --role runner --subject alice > .dacp-security/8815/alice-issued.json
 python3 -c 'import json,pathlib; p=pathlib.Path(".dacp-security/8815"); (p/"alice.token").write_text(json.loads((p/"alice-issued.json").read_text())["token"])'
)
target/universal/stage/bin/dacp-server
```

默认服务地址为 `localhost:8815`，配置见 [examples/server.yaml](../../examples/server.yaml)。可先连接服务器根地址，在 Shell 中使用无参数 `datasets` 列出目录，再选择数据集；目标服务器只有一个数据集时会自动选中。节点发布本地目录，Union 在线合并公开目录，管理 Web 可按指定成员查看目录；见[数据集目录](../dataset-directory_zh.md)。

在另一个终端中，从项目根目录连接：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_TOKEN_FILE="$PWD/dacp-server/.dacp-security/8815/alice.token"
.venv/bin/dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

进入 shell 后：

```text
ls /
ls /reports
ls /archive
cat /archive/reports/orders.csv records
ls /images
cat /images/images/pixel.png hex
ls /bundle_gzip
cat /bundle_gzip/reports/orders.csv records
ls /orders.csv.gz
cat /orders.csv.gz/orders.csv records
cat /orders.csv.gz hex
cat /reports/orders records
cat /events text
cat /image hex
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

参见[记录查询语义与插件契约](../records-scans_zh.md)，了解 Python/Scala 示例、空值和类型规则、校验以及各来源的优化范围。

### dacp-shell

`ls` 以带外边框的表格显示 #、Path、Format、Views、Bytes、Records 和 Modified (UTC)，首列为从 1 开始的条目编号。未知值显示为 `-`，时间格式为 `2026-10-16 06:22:02`；终端中集合路径以加粗青色高亮。

`cat` 接受路径或最近一次列表中的编号；默认优先显示 records，其次是 bytes（十六进制），再选择第一个已声明的科学视图。记录使用带列名的表格，`text` 显示文本，`hex` 显示偏移量、十六进制及 ASCII。默认最多预览 20 行或 4096 字节，可通过 `--rows` 和 `--bytes` 调整。`cat 2 array`、`cat /sky.fits/hdu0 array`、`cat /genome.fa/sequences sequence` 显示有界的结构描述，以及包含块位置/标识和样例的表格。array 根据声明的数据类型和字节序解码数值。科学视图达到元素/块数量或载荷字节预算后停止并取消读取器；单个传入块可能大于展示预算。未知科学表示或编码展示元数据与十六进制载荷，不假设文件格式。`cat ITEM bytes` 等同于 `hex`。请求 Item 未声明的视图会明确报错。`Views` 列列出全部已声明的表示；空列表显示为 `-`。

`datasets` 显示 `#`、`Name`、`Description (JSON)` 三列，不显示 URI。使用 `use NAME` 按名称切换同一服务器上的数据集，也可用 `use 1` 选择最近一次 `datasets` 列表中的第 1 行。行号从 1 开始，刷新列表会更新映射，重新连接会清空映射；纯数字参数按行号解释。使用 `connect URI` 连接其他位置，`help` 查看命令。证书也可通过 `--ca`、`--cert`、`--key` 指定。

在 shell 提示符下直接输入单行 SQL 查询：

```sql
SELECT amount FROM "/reports/orders" WHERE id > 1 LIMIT 10
SELECT amount FROM 2 WHERE id > 1 LIMIT 10
SELECT * FROM "/reports/orders" WHERE id IN (1, 2) AND amount IS NOT NULL
```

shell 使用 SQLGlot 将支持的 SQL 子集转换为与 SDK 相同的远程扫描请求。`FROM` 可以是当前 Dataset 内用双引号包围的绝对 Item 路径，也可以是最近一次 `ls` 结果中的条目编号（例如 `FROM 2`）。编号跟随最新列表的顺序，包括 `ls PATH`；编号超出范围时会报错。支持 `SELECT *` 或公开列名列表，以及可选的 `WHERE` 和 `LIMIT`；SQL 关键字不区分大小写。结果以带外边框的表格显示。`--rows` 预览上限仍然有效：发送给服务器的行数限制取 SQL `LIMIT` 和预览上限中的较小值；省略 `LIMIT` 时使用预览上限。输入 `help select` 查看语法。不支持的连接、排序、聚合、别名和子查询会明确报错。参见 [SQL shell 说明](../records-scans_zh.md#dacp-shell-中的-sql)。


### 帮助与命令示例

输入 `help` 查看命令列表及简短示例；输入 `help cat`、`help select`、`help run`、`help submit` 查看具体语法和用法。帮助中的路径为示例，请使用 Dataset 实际公布的路径。条目编号对应最近一次 `ls` 的结果。

### Scala SDK

独立的 [Scala 2.13 SDK](../../dacp-client-scala/README_zh.md) 提供相同的数据访问模型，不依赖服务端工程。先发布到本地 Ivy 仓库：

```bash
cd dacp-client-scala
sbt test publishLocal
```

在 Scala 应用的 `build.sbt` 中添加：

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

使用 `Source.open`、`Dataset.list`、`Dataset.open`、`asRecords()` 和 `asBytes()`，通过 `scala.util.Using` 管理资源。SDK 文档提供完整示例、TLS 配置、超时及 Arrow JVM 参数；构件尚未发布到 Maven Central。

## 发布数据

发布节点的监听、TLS、凭据及目录配置见[管理员指南](../administrators/README_zh.md)。Dataset 后端配置和文件读取参数见 [XStream Dataset 指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-providers_zh.md)及[文件格式参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/file-formats_zh.md)。完整 SQL 发布示例见 [SQL 示例](../../examples/sql/README_zh.md)。

## 数据协作与任务

DACP 通过内嵌 XStream 引擎执行 YAML Job。用户描述逻辑流依赖，不指定执行节点；Union 解析 Item 位置并自动规划节点子作业，成员通过双向 TLS 和节点签发的服务凭据协作。符合[合并执行条件](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion_zh.md)的节点子作业共享一个容器和进程，其余任务独立执行；命名端口消费数据项流，输出直接流向客户端或下游任务，不保存输出数据集。

运行任务还需要 Linux、`mkfifo` 和可访问的 Docker Engine。从项目根目录配置提供已审批算子的仓库，再启动服务；若快速开始的服务仍在运行，先将其停止：

```bash
cd dacp-server
sbt 'run ../examples/server.yaml'
```

连接 shell 后运行 [当前算子库](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-catalog_zh.md)中的完整气象工作流，或在选择气象数据集后运行 `run piflow_csv_parser@1.0 --input source=/observations --view bytes`。shell 等待并逐块显示结果；Ctrl-C 取消任务。

更多说明：

- [双节点示例](../../examples/multi-node/README_zh.md)：任务放置、远端输入及输出消费。
- [Scala 算子仓库与网站](https://github.com/rdcn-link/xstream-operator-repository/blob/main/docs/browsing_zh.md)：公共搜索、版本详情、下载与不可变算子发布。
- [自定义算子编写与上传](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring_zh.md)：Scala/Python 完整源码、构建、发布、远程上传和任务执行。
- [任务与算子参考](../jobs_zh.md)：DAG、容器输入输出契约和转换参数。

在独立算子仓库项目目录中，使用 `scripts/setup-operator-repository.sh --host 0.0.0.0 --port 8820` 启动空仓库，通过源码提交与审批发布算子后，打开 `http://localhost:8820/`，无需登录即可搜索、查看和下载算子。HTTPS 部署参见仓库指南。

算子共享依赖环境和小型代码包。独立仓库只接收源码，构建 JVM JAR 或 Python 字节码 ZIP，经审核后公开。环境与程序包按 SHA-256 校验并缓存。算子开发与部署说明位于 XStream 文档；DACP 用户配置仓库地址并使用已批准版本。


`run` 和 `submit` 在完成、失败或取消后打印 `Execution time: 1.234 s`。这是 shell 从提交开始到结果流关闭的总耗时，包括连接、运行时准备、执行、传输、结果显示及客户端清理；不包含输入 heredoc 的时间，也不是单个服务器任务的 CPU 时间。多任务作业显示整个作业的一次总耗时。

## 安全与运行边界

DACP 元信息允许通过 TLS 匿名查看；数据读取需要 Token 对应主体具有 reader 或 runner，执行和推导需要 runner。Union 协作使用双向 TLS 与服务凭据，暂不配置 Dataset/Item ACL。公开元数据、ticket 和数据服务错误隐藏私有存储绑定；数据内容仍包含源数据自身的字段和值。Python 的 `DacpError` 和 Scala 的 `DacpException` 提供错误码和可读消息，shell 显示简洁提示。

未发布的物理文件和数据库表不会自动公开。读取失败后从头重开，不支持流内重试、checkpoint 或恢复。任务输出仅为实时流，客户端须消费或取消；不提供保存的结果 URI，重启不恢复任务。完整配置和协议说明见 [技术参考](../reference_zh.md#协议与配置)。

DEBUG 模式下可通过 `run --runtime python@1.0 --input source=/orders <<END` 输入 heredoc 源码。服务端默认为 NORMAL，并在协调与执行节点拒绝源码任务。配置、Python/Scala 示例与插件契约见[源码算子](https://github.com/rdcn-link/dacp-codex/blob/main/docs/operator-source_zh.md)。

原生可执行程序可通过 `command` 执行器使用 stdin/stdout，无需在程序内实现 DACP 协议。支持单 bytes 输入和输出，仍在容器内隔离执行。见[配置及发布指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands_zh.md)和 [C 示例](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/native/README_zh.md)。

## 启动 DEBUG 日志

启动时，`dacp.Main` 的 DEBUG 日志打印 `remote_operator_library_urls` 的数量及按配置顺序排列的每个 URL；空列表显示数量 0。普通 INFO 日志不打印这些 URL。stage 服务端后，可从项目根目录运行：

```bash
(cd dacp-server && target/universal/stage/bin/dacp-server \
  -J-Dorg.slf4j.simpleLogger.log.dacp.Main=debug ../examples/server.yaml)
```

这是日志级别设置，与允许提交源码的 `mode: DEBUG` 独立。启用 DEBUG 日志不会开放源码执行。



## Union 协作作业

通过 [Union 服务器](../../dacp-union-server/README_zh.md) 提交显式节点 DAG、订阅执行状态和流量，并读取最终流。成员配置与服务凭据、Python 与 Scala 用法见模块文档；[双节点示例](../../examples/union/README_zh.md) 演示跨节点气象观测与气候基准融合。

DACP 与 Union 默认持久化全部执行记录；Union 看板支持重启后的历史查询。本地查询命令及保存目录见 [执行记录](../execution-history_zh.md)。

## 执行策略、重试与重放

单机 DACP Server 的多步骤任务和 Union 协作任务均支持独立的执行策略 YAML：`max_retries`、`timeout_seconds` 和 `materialization`。Shell 用 `submit workflow.yaml --policy policy.yaml` 提交，并可用 `replay EXECUTION_ID` 从有效的物化边界重放。Python 与 Scala 客户端须通过显式重试回调清空上一轮部分结果；关闭结果流可取消任务。配置、完整调用示例与成功确认规则见[执行策略、输出物化与重放](../materialization_zh.md)。

Union 对外提供标准 DACP 目录、Item、Job 与 S3 服务；公开短名称、自动放置及管理权限配置见 [Union 代理服务](../union-proxy_zh.md)。

[在线 Job 设计器](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md)：从浏览器设计单机与跨节点作业，导出 YAML 供 Shell / SDK 提交。

仅浏览元信息时可省略 `--token-file`：`dacp-shell dacp://host:8815`。数据集、Item 列表及元数据可匿名查看；`cat`、查询及计算记录数需要 reader 或 runner，`run`/`submit` 需要 runner。

### 仅凭地址匿名浏览

```bash
.venv/bin/dacp-shell dacp://10.0.82.103:18861
```

未配置 `DACP_TOKEN_FILE`、`DACP_CA` 或客户端证书时，无需 `ca.crt` 即可浏览公开目录和 Item 元信息。此模式保留 TLS 加密，不校验服务器证书。显式传入 `--ca` 会启用校验；携带 Token 的访问仍需可信的服务器证书。匿名访问不能读取数据流或执行任务。
