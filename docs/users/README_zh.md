# Shell 与 SDK 指南

[English](README.md) · [使用指南](../usage_zh.md)

使用 Shell、Python 或 Scala 连接节点与 Union，浏览目录并读取流。首次安装、凭据准备和连接步骤见[快速上手](../quickstart_zh.md)。以下示例连接 `dacp://localhost:8815`，并使用示例服务的数据集 `data1`；远程服务请替换地址、路径与凭据。

使用环境变量配置 SDK 的凭据：

```bash
export DACP_CA=/path/to/ca.crt
export DACP_TOKEN_FILE=/path/to/reader.token
```

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


## 执行与运维

- [提交 Job、变量与结果消费](../jobs_zh.md)
- [重试、超时、取消、物化与重放](../materialization_zh.md)
- [角色、凭据与访问失败](../security_zh.md)
- [发布和部署服务](../operations/README_zh.md)

读取失败后需重新打开流。Job 的执行尝试与普通 Item 读取是不同操作；自动重试和物化的行为以执行策略为准。
