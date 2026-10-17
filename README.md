# dacp MVP

<img src="assets/dacp-logo.png" alt="DACP 标志：以相连节点组成的 C，代表协作" width="480">

[English](README_en.md)

面向自治数据节点的 Arrow Flight 数据协作协议。Scala 2.13 服务端、Python/PyArrow 与 Scala SDK、protobuf 元数据、YAML DAG、一任务一 Docker 容器。节点无需部署集群或中心注册服务。文件与 JDBC SQL 表通过显式逻辑别名发布，folder 和 data Item 统一组织内容。

## 核心概念：Dataset、Item 与 Stream

Dataset 是节点发布的命名逻辑空间。Item 有两种 kind：`folder` 用于组织子 Item，`data` 提供可读取的内容。普通文件和 SQL 表都映射为 `data`，ZIP 归档映射为 `folder`，客户端无需依赖存储类型。Stream 是打开具有读取视图的 Item 后得到的读取接口。

| 概念 | 含义 | 示例 |
|---|---|---|
| Dataset | 节点上的根集合，具有公开名称 | `dacp://localhost:8815/data1` |
| folder Item | 包含子 Item 的逻辑集合；ZIP 集合提供 bytes Stream | `/reports` |
| data Item | 可读取的逻辑资源，具有格式、可选 Arrow schema 和支持的 views | `/reports/orders`、`/image` |
| Stream | 通过记录或字节视图拉取 Item 的内容 | `ds.open("/reports/orders")` |

所有 Item 路径都以 `/` 开头，相对于 Dataset 的逻辑根；它们不是服务器文件路径或 SQL 表名。公开名称由管理员显式配置。集合可以同时包含文件和 SQL 表映射，父集合可从路径自动推导，也可显式配置空集合。`/` 是隐式根集合，不作为 Item 出现在清单中。Dataset 内的 data Item 可以具有不同格式和 schema。

| 调用 | 行为 |
|---|---|
| `ds.list()` | 递归返回所有 Item，包括 folder 和 data |
| `ds.list("/")` | 只返回根集合的直接子 Item |
| `ds.list("/reports")` | 只返回 `/reports` 的直接子 Item，不包含集合自身 |
| `ds.open("/reports/orders")` | 打开 data Item，返回 Stream |

返回路径始终从 Dataset 根开始。空集合返回空列表；不存在的路径、对 data Item 调用 `list(path)`、对不提供视图的 folder 调用 `open(path)` 都会报错。不接受相对路径、`.`、`..`、重复分隔符或末尾 `/`（根 `/` 除外）。URI 将 Dataset 名称与逻辑路径拼接，例如 `dacp://localhost:8815/data1/reports/orders`。列举仅返回元数据，不下载内容。

Stream 支持的视图由 Item 的 `views` 元数据表示：

- `records`：逐批读取带 schema 的 Arrow RecordBatch，适用于 CSV、NDJSON、Excel、带 Arrow schema 的 DACP Parquet 和 SQL 表。
- `bytes`：增量读取二进制内容。文件映射返回原始文件字节，SQL 映射返回 Arrow IPC stream；PNG 等二进制资源只提供 bytes 视图。

配置好快速运行中的 mTLS 证书后：

```python
from dacp import Source

with Source.open("dacp://localhost:8815/data1") as ds:
    for item in ds.list("/"):
        print(item.path, item.kind, item.views)
    print(ds.list("/reports"))
    with ds.open("/reports/orders") as stream:
        for batch in stream.as_records():
            print(batch.to_pydict())
    with ds.open("/image") as stream:
        with stream.as_bytes() as reader:
            print(reader.read(8))
```

Dataset 创建的 Stream 共用连接，须在 Dataset 关闭前使用；直接用 Item URI 打开的 Stream 拥有独立连接。字节 reader 应使用上下文管理器；提前停止记录迭代时关闭迭代器以取消读取。失败后重新打开资源从头读取，没有断点续传。

## 格式与视图

format 标识源数据表示方式，view 标识读取方式。客户端使用服务器公布的 `views`，包括以后新增的格式，不再根据文件名猜测能力。格式名称使用小写。

| 来源 | Format | Views |
|---|---|---|
| `.csv` | `csv` | `records`, `bytes` |
| `.jsonl`, `.ndjson` | `ndjson` | `records`, `bytes` |
| `.json` | `json` | `bytes` |
| `.zip`, `.ZIP` | `zip` | `bytes` |
| `.xls`, `.xlsx` | `excel` | `records`, `bytes` |
| DACP Parquet | `parquet` | `records`, `bytes` |
| SQL 表 | `sql` | `records`, `bytes` |
| 其他文件 | `bin` | `bytes` |

注册表优先使用显式 `format`，否则依次匹配已注册的文件签名和不区分大小写的扩展名，最终回退为 `bin`。检测最多读取八个签名字节，不验证整个文件；结构化内容错误在打开记录视图时报告。已识别文件仍可通过 bytes 原样读取。Parquet 目前要求 DACP Arrow schema footer 元数据。普通 JSON 仅提供 bytes；对于按行存储对象的 `.json` 文件，配置 `format: ndjson` 即可启用记录视图。SQL 格式由 `arrow` 改为 `sql`，bytes 视图仍使用 Arrow IPC。

### ZIP 归档浏览

将 ZIP 文件映射为普通文件别名，服务器自动将其发布为 `kind=folder`、`format=zip`，提供原始归档的 `bytes` 视图。`list("/archive")` 列出归档的直接子项，`list()` 包含全部归档条目；缺失的父集合自动补齐。条目按扩展名选择格式，打开时使用现有格式处理器检测和读取。ZIP 内的 ZIP 暂作为 `bin` data Item，不递归展开。所有 folder 的修改时间、大小及记录数仍为未知。

```yaml
datasets:
  public:
    items:
      /archive: {file: /private/reports.ZIP}
```

```python
ds.list("/archive")
ds.list("/archive/reports")
with ds.open("/archive/reports/orders.csv") as stream:
    records = stream.as_records()
with ds.open("/archive") as stream:
    archive_bytes = stream.as_bytes()
```

列举只读取 ZIP 目录，不解压条目。打开、推断 schema 或计数时，仅将所需条目解压到服务器临时文件，完成、失败或取消后清理；因此大条目的首次响应需要等待解压。每个归档最多 10,000 个条目，单条目解压上限 512 MiB，目录声明的总大小上限 2 GiB。绝对路径、路径穿越、重复条目、文件与父集合冲突均被拒绝；不解压到源目录，也不跟随归档内符号链接。`dacp-shell` 可用 `ls /archive` 浏览、用 `show /archive hex` 预览原始归档字节。

文件映射支持以下私有读取选项：

```yaml
datasets:
  office:
    items:
      /orders:
        file: /private/sales.xlsx
        format: excel
        sheet: Orders
        header_row: 2
        schema:
          - {name: id, type: int64}
          - {name: amount, type: float64}
      /events:
        file: /private/events.json
        format: ndjson
```

`sheet` 和 `header_row` 仅用于 Excel；表头行号从 1 开始，默认值为 1。单工作表文件自动选择唯一工作表；多工作表文件必须显式指定。可用多个逻辑别名分别发布工作表。每个别名的 bytes 返回原始完整工作簿，records 和 count 仅对应所选工作表。工作表绑定及底层文件名不会出现在元数据或 ticket 中。

Excel 使用 Apache POI 5.5.1 的 XLS/XLSX 事件读取接口。空表头生成 `column_N`，重复名称依次添加 `_2`、`_3`，缺失单元格返回 null。空数据行跳过，超出表头宽度的数据报错。公式只读取缓存结果，不重新计算；可识别的缺失缓存返回 null。错误单元格、加密或无效工作簿返回 DACP 格式错误。整数且未越界的数字单元格使用 int64，其余数字使用 float64，布尔值及字符串保留其值。Excel 数字日期保留序列值，不自动推断地区或时区。可选 `schema` 通过有序的 `name`/`type` 配置列，支持 `int64`、`float64`、`bool` 和 `utf8`/`string`。未配置时采样不超过 `batch_size * infer_sample`，后续类型冲突报错，不修改既定 schema。

列举不解析 CSV、NDJSON 或 Excel 记录，其记录数保持未知。打开记录视图时通过有限采样推断 schema；只有显式 `count` 扫描文件，Parquet 可直接读取 footer 中的数量。Excel 解析队列最多保留 16 行，关闭或取消时停止解析；schema、当前 batch、单个单元格及 POI 共享字符串表还会占用内存，大型共享字符串表仍可能需要较多内存。不会在内存中构建完整工作表。

服务端扩展接口为 [`FormatHandler`](dacp-server/src/main/scala/dacp/Formats.scala)。提供者实现检测、可选的低成本元数据、schema、记录读取、显式计数及资源释放；文件 bytes 使用公共读取器。将提供者 JAR 加入类路径，在 `META-INF/services/dacp.FormatHandler` 注册公开的无参类并重启。格式 ID 和扩展名注册必须唯一，`sql` 保留给 SQL adapter。新增格式不需要修改核心服务端或 SDK。参见[处理器开发说明](docs/format-handlers.md)。

## Item 属性

Item 元数据新增以下可选属性；未知值与已知的 `0` 明确区分：

| 属性（Python / Scala） | 含义 |
|---|---|
| `last_modified_time_ms` / `lastModifiedTimeMs` | UTC Unix 毫秒时间戳。Python 的 `last_modified_time` 返回带 UTC 时区的 datetime；Scala 的 `lastModifiedTime` 返回 `Option[Instant]`。 |
| `total_size_bytes` / `totalSizeBytes` | 内容的总字节数；文件映射使用原始文件大小。 |
| `record_count` / `recordCount` | 精确的逻辑记录数。CSV 不包含表头，NDJSON 忽略空行，Excel 统计所选工作表的数据行。这些文件和 SQL 在普通浏览时不计数，使用显式 `ds.count(path)`；Parquet 从 footer 读取。 |

Python 未知值为 `None`，Scala 为 `None`（已知值为 `Some`），protobuf 中对应字段缺省。SQL 映射不推测修改时间或字节数；PNG 等非记录数据的记录数为未知。folder 的三个属性始终为未知，包括空集合；计算集合属性时不会访问其子 Item。

这些属性由 `list()`、`list(path)` 和 data Item 的 FlightInfo 返回，交互式客户端 `ls` 也显示它们。显式 CSV/NDJSON/Excel 计数会扫描文件，显式 SQL 计数会执行查询；普通列举不进行这些计数。SQL 计数遵循源查询超时。集合元数据不会触发后代扫描；data Item 的统计不跨请求缓存。属性是观测值，不与后续读取组成原子快照，源数据变化时可能改变。

```python
with Source.open("dacp://localhost:8815/data1") as ds:
    for item in ds.list("/"):
        print(item.path, item.last_modified_time, item.total_size_bytes, item.record_count)
```

## 逻辑命名空间配置与迁移

`datasets` 显式映射每个 Dataset 的公开逻辑路径。`file`、`sql` 和 `table` 是服务器端私有来源绑定。SQL 连接在私有 `sql_sources` 中定义，不自动发布表名：

```yaml
datasets:
  public:
    items:
      /reports/orders: {file: ../examples/data/nested/orders.csv}
      /clients: {sql: warehouse, table: CUSTOMERS}
      /empty: {kind: folder}
sql_sources:
  warehouse:
    url: jdbc:h2:../examples/sql/sales
    user: sa
    password: ""
    schema: PUBLIC
    tables: [CUSTOMERS]
```

此片段需与服务器的监听、TLS、白名单等配置合并；数据库表需预先存在，完整初始化示例见 SQL 配置。父集合 `/reports` 自动创建；不自动发布其余文件或数据库表。旧版 `datasets: {name: /directory}` 和 `sql_datasets` 配置不再支持，须迁移到显式映射并更新客户端路径。元数据、ticket 和数据服务错误不携带底层路径或表名；内容中的字段和值仍按源数据返回。任务输出使用 `/result-1`、`/result-2` 等逻辑别名，避免暴露算子内部文件名。

## 快速运行

需要 JDK 17、sbt 1.10.x、Python 3.10+。执行容器任务还需要 Linux、`mkfifo`、本地 Docker Engine，运行服务的用户须能访问 Docker socket。示例路径按以下工作目录编写。

```bash
cd dacp-codex
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
cd dacp-server
sbt run
```

服务器启动成功后，会按名称排序输出 `datasets` 中配置的公开数据集名称及完整 DACP URI。可将 URI 直接用于交互式客户端；任务运行时产生的临时输出不在此启动清单中。

另一终端，从项目根目录运行：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python examples/client.py
```

输出包含 `/events`、`/reports/orders`、`/image` 等逻辑 Item，CSV 两条记录和 PNG 字节数。TLS 证书仅用于本地示例；脚本生成 CA、服务端和客户端证书，服务端证书也允许 clientAuth，供本节点任务执行器连接其他信任该 CA 的节点。

```python
from dacp import Source

with Source.open("dacp://localhost:8815/data1") as ds:
    print(ds.list())
    for batch in ds.open("/reports/orders").as_records():
        print(batch.to_pydict())
    with ds.open("/image").as_bytes() as stream:
        header = stream.read(8)
```

URI 默认端口为 8815，也支持直接打开 Item：`Source.open("dacp://localhost:8815/data1/reports/orders")`。三份证书也可用 `tls_root_certs`、`cert_chain`、`private_key` 的 bytes 参数传给 `Source.open`。Dataset 和直接打开的 Stream 支持 context manager。

普通 SQL 浏览和 `show` 不执行 `COUNT(*)`。需要精确数量时，在 shell 使用 `count /reports/orders` 或 `count 1`，在 Python/Scala SDK 使用 `ds.count("/reports/orders")`。显式计数可能耗时并遵循 SQL 查询超时；不会更新列表中的缓存元数据。folder 不支持计数，非记录 data 返回未知。

请求不存在的数据集时，服务器返回 `NOT_FOUND: dataset not found; check the dataset name`，提示检查名称；已存在数据集仍按 URI 白名单校验访问权限。

SDK 将底层传输错误转换为 DACP 错误：Python 为 `DacpError`，Scala 为 `DacpException`，两者提供稳定的 `code` 和可读消息。shell 只显示简洁提示，例如 `Dataset not found. Check the dataset name.`；不会显示 Arrow Flight/gRPC 错误包装、远端诊断或私有存储信息。流中途失败同样转换，失败后仍需重新打开。底层异常不作为公开异常原因附带返回。

## 交互式客户端 dacp-shell

`dacp-shell` 是交互式客户端命令，原 `dacp` 命令作为兼容别名保留。`ls` 以带外边框和列名的表格显示 Path、编号、Format、Bytes、Records 和 Modified (UTC)，不显示 Kind 列。时间格式为 `2026-10-16 06:22:02`，未知值显示为 `-`，空列表也显示表头和边框。`show` 的记录预览以字段名为列名显示带边框的表格；text 保持纯文本显示，hex 保持偏移量、十六进制和 ASCII 的原有显示样式。终端中 folder 路径以加粗青色高亮；重定向输出、`NO_COLOR` 或 `TERM=dumb` 时不使用颜色。

安装 Python SDK 后，可连接远端数据集，在终端列举 Item 并预览内容。使用已有的 `DACP_CA`、`DACP_CERT`、`DACP_KEY` 环境变量，或显式提供证书路径：

```bash
.venv/bin/pip install -e './dacp-client[test]'
.venv/bin/dacp-shell dacp://localhost:8815/data1 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

连接远端时，将 `localhost:8815/data1` 替换为实际服务器地址、端口和数据集名称。也可用 `.venv/bin/python -m dacp.cli` 替代 `.venv/bin/dacp-shell`。

```text
ls /
ls /reports
show 1
show "/reports/orders" records
show "/events" text
show "/image" hex
use csv_data
connect dacp://localhost:8815/data1
help
quit
```

`ls` 递归列举所有 Item；`ls /` 或 `ls /reports` 只列举集合的直接子 Item，并显示编号、格式和逻辑路径。folder 使用 `ls` 浏览，data 使用 `show` 预览。`show` 接受编号或带引号的路径；根据服务器公布的 views，支持 records 时默认使用记录视图，否则使用十六进制预览。`text` 按 UTF-8 显示原始字节并转义终端控制字符；SQL 表应使用 `records`，其 bytes 视图为 Arrow IPC。默认最多显示 20 行或 4096 字节，可用 `--rows`、`--bytes` 调整；每行字段文本合计受 `--bytes` 字符预算限制（省略标记和表格边框除外）。记录预览停止时取消上游读取，避免下载整个数据集。二进制文件显示十六进制及 ASCII，不直接打开图片。失败后可重试命令或重新连接；连接切换失败保留旧连接。

服务器目前不提供数据集发现接口，因此启动和 `connect` 必须指定已知数据集 URI；`use` 切换同一服务器上的已知数据集。使用 `quit`、`exit` 或 EOF 退出并关闭连接。

## Scala 客户端 SDK

独立 Scala 2.13 SDK 位于 [dacp-client-scala](dacp-client-scala/README.md)，需要 JDK 17 和 sbt 1.10.x，不依赖服务端项目。它提供 `Source.open(datasetUri)`、`Dataset.list()` / `Dataset.list(path)`、`Dataset.open(itemPath)`、`ItemStream.asRecords()` 和 `ItemStream.asBytes()`；直接访问 Item URI 使用 `Source.openItem(itemUri)`。

```bash
cd dacp-client-scala
sbt test publishLocal
```

在 Scala 2.13 应用的 `build.sbt` 中添加本地发布的依赖：

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

该构件尚未发布到 Maven Central。证书默认读取 `DACP_CA`、`DACP_CERT`、`DACP_KEY`，也可显式传入 `TlsConfig`。记录读取器通过 `next()` 推进并从 `root` 访问 Arrow batch；root 会被复用，须在下一次读取前消费或复制。字节读取器为 Java `InputStream`。使用 `scala.util.Using.resource` 关闭连接、Item 和读取器；默认每次 Flight RPC 的完整生命周期超时为 30 秒，可用 `timeoutSeconds` 调整。JVM 的 Arrow 参数、完整示例及测试方法见 SDK 文档。

## 同时提供目录和 SQL 数据库

通过私有 `sql_sources` 配置数据库，再在 `datasets` 中显式映射公开逻辑路径。SQL 映射与文件映射可位于同一集合，通过相同的 `Source.open / list / open / as_records` API 访问；SQL data Item 的清单包含 Arrow schema，字节视图为 Arrow IPC 流。内置 H2 与 PostgreSQL JDBC 驱动，支持表白名单、环境变量凭据和有界 fetch/batch。详见 [SQL 配置、类型与运行步骤](examples/sql/README.md)。

```bash
cd dacp-server
sbt 'run ../examples/sql/server.yaml'
# 另一终端，项目根目录，设置前述 DACP_CA/CERT/KEY 后：
.venv/bin/python examples/sql/client.py
```

## 运行转换任务

先停止前一个服务实例，再构建算子并启动带任务的服务：

```bash
# 项目根目录
docker build -f operators/format_convert/1.0/Dockerfile -t dacp/format-convert:1.0 .
cd dacp-server
sbt 'run ../examples/server.yaml ../examples/convert.yaml'
```

启动 Flight 后执行 YAML 中的 DAG。`csv_data` 显式发布指向示例 CSV 的逻辑 Item `/orders`；它与演示 PNG 读取的混合数据集 `data1` 共用本地文件。成功日志显示 `s1`、`s2` 的 URI，Parquet 位于 `examples/output/<output>-<uuid>/`。服务继续运行并暴露输出，TTL 到期自动删除；停止服务也会清理输出。

从项目根目录、沿用上面的证书环境变量：

```bash
.venv/bin/python examples/verify_job.py
```

输出清单中的 Parquet Item 必须携带 schema。原始 CSV/NDJSON/Excel 的清单 schema 可为空（枚举不扫描数据）；打开具体 Item 的 FlightInfo 才推断 schema。

算子库支持独立 Web 服务：配置 `operator_library_url` 后，执行节点远程下载任务指定版本的镜像归档、校验并缓存，再使用本地 Docker 隔离执行。启动、发布与配置见 [dacp-operator-library](dacp-operator-library/README.md)。不配置 URL 时仍使用本地 `operators/<name>/<version>/operator.yaml`。版本不可覆盖，无动态版本更新。标准 Dockerfile 在构建阶段执行 `sbt stage`，运行阶段只保留 JRE 和应用。不要把生成的证书或数据挂入算子镜像。

### DAG 与算子契约

`input` 是单个 URI 或上游 `output` 名；`inputs` 是多个此类值组成的列表。上游引用也支持 `s1/result-1`。任务先校验重复名称、未知依赖、环和目标节点，再按拓扑顺序串行执行。默认在第一个输入 URI 所在节点执行，也可用 `node: dacp://host:port` 显式指定。协调节点把上游引用解析为具体 URI，再通过 mTLS Flight action 分发；无中心注册服务。每个子任务新建容器，禁用容器网络，挂载只读 `/input` 和可写 `/output`。

`/input/manifest.json` 包含 `file/path/format`；`file` 指向 Arrow IPC stream，bytes 视图为 `data: binary`，每行一个原始字节块。`/input/params.json` 是参数对象。单输入使用 FIFO，IPC 的 EOS 表示结束，容器读取消费驱动上游；多输入在启动容器前全部落盘。结束或失败均删除输入临时目录；失败的输出目录也删除。仅容器退出码为 0 才注册输出。任务超时上限为 30 分钟。

容器入口接收 `operator /input /output /input/params.json`，环境变量 `DACP_OPERATOR` 指定版本化算子名称。自定义算子遵守相同 Arrow 输入契约；输出 `/output/_output.json` 清单（`file/path/format`），每个 `file` 为单列 binary Arrow IPC stream，`path` 为落地文件名。执行器在成功退出后解码这些流、清除 IPC 临时文件，注册最终目录。内置算子在 Parquet footer 完成后将文件包装为该 Arrow 输出契约，内存有界。内置转换器逐个消费 CSV/JSON Item，生成 `part-N.parquet`，不将不同 schema 的 Item 强行合并；不接受 PNG 等其他格式。

### format_convert 参数

| 参数 | 默认 / 支持值 |
|---|---|
| `from` | `auto`；`csv` / `json` |
| `to` | `parquet` |
| `schema` | `infer` 或有序列声明，例如 `{id: int64, amount: float64}` |
| `infer_sample` | 4；每块为 1024 条解析记录，采样后 schema 固定 |
| `on_conflict` | 仅 `fail` |
| `compression` | `zstd` / `snappy` / `none`，默认 zstd |
| `row_group_size` | `128MB`，也接受 B / KB / GB 或字节数 |

显式 schema 支持 `int64`、`float64`、`bool`、`utf8`（或 `string`），列名及顺序必须匹配。CSV 空字段、JSON null 为 null；JSON 使用 NDJSON，一行一个平坦对象。采样发现不同类型直接 `SCHEMA_CONFLICT`，后续批次也校验固定类型；显式 float64 允许合法整数表示，显式 utf8 保留文本。没有自动类型拓宽。内存由采样块、当前 Arrow batch、解析器缓冲和 Parquet row group 决定，不随完整文件大小增长；超大单条记录仍需要容纳该记录。

转换器用 Arrow Java 解码 IPC/构建向量，Apache Parquet Java 原生写入器通过 Avro 行桥接生成 Parquet（Arrow Java 没有原生 Parquet writer）。输出 Arrow schema 同时写入 Parquet footer 的 `dacp.arrow.schema` 和数据集保留文件 `_schema.json`；保留文件不作为 Item 暴露。

## 协议与配置

唯一元数据定义：[proto/dacp.proto](proto/dacp.proto)。Scala 在 sbt 构建时生成 Java 类，Python 绑定已提交。修改协议后用系统 protoc（3.21+）重新生成：

```bash
protoc -I proto --python_out=dacp-client/dacp proto/dacp.proto
```

| Flight 调用 | 响应 |
|---|---|
| GetFlightInfo `[datasets, dataset]` | `app_metadata` 为 protobuf `DataSet`，包含全部逻辑 Item（folder 和 data），无 DoGet endpoint |
| GetFlightInfo `[datasets, dataset, list, /path]` | 只返回集合的直接子 Item |
| GetFlightInfo `[datasets, dataset, count, /path]` | 显式返回 data Item 的精确记录数（非记录数据为未知），无 DoGet endpoint |
| GetFlightInfo `[datasets, dataset, /logical/item]` | Arrow schema + 含 protobuf `Ticket` 的 FlightEndpoint |
| DoGet `Ticket(dataset,item,view)` | `view=records` 为记录，`view=bytes` 为单列 binary（原始文件字节） |

记录视图支持 CSV、NDJSON、Excel、SQL 和 dacp 生成的 Parquet；任意文件可用 bytes。SDK 将 binary batch 包装成可增量读取的 `io.BufferedReader`。ticket 只描述资源和视图，不是认证 token。GetFlightInfo 和 DoGet 均校验 URI 白名单、规范化路径和符号链接边界。越界链接不会出现在清单中。

`examples/server.yaml` 配置：

| 字段 | 含义 |
|---|---|
| `host` / `port` | 监听地址 / 端口 |
| `public_host` | 对外 URI 主机名，白名单使用该名字及显式端口 |
| `datasets` | Dataset 名称 → `items` 显式逻辑映射；名称为一个 URI 路径段 |
| `allowed_uri_prefixes` | 节点级 URI 前缀白名单，按路径段边界匹配；输出名称也必须授权 |
| `tls_cert` / `tls_key` / `tls_ca` | PEM 证书、私钥、可信客户端 CA；强制 mTLS |
| `batch_size` | 文件记录视图每个 batch 的最大行数 |
| `byte_chunk` | 字节视图每个 binary 行的最大字节数 |
| `infer_sample` | 服务端记录视图推断采样 batch 数 |
| `output_ttl_seconds` | 从成功注册开始计算输出寿命 |
| `sql_sources` | 私有 JDBC 数据源（不自动发布表），url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
| `operator_library_url` / `operator_cache` / `operator_max_bytes` | 远程算子库 URL / 本地不可变版本缓存 / 最大归档字节数（默认 8 GiB） |
| `operator_library` / `output_root` | 本地算子库 / 临时输出根目录 |
| `task_peers` | 允许分发任务的节点 origin；自身始终允许，无中心发现服务 |
| `task_input_prefixes` | 执行节点可拉取的输入 URI 前缀；省略时使用 `allowed_uri_prefixes` |
| `max_concurrent_tasks` | 本地任务及控制请求并发上限（默认 2）；满载立即返回 UNAVAILABLE |

前缀 `dacp://localhost:8815/data1` 不匹配 `data10`。本版白名单适用于所有通过该节点 CA 验证的连接，不提供按证书主体划分的权限组。任务执行器使用节点配置的证书发起 mTLS；多节点须互相信任相应 CA。

服务错误使用 Flight/gRPC 状态和明确的 dacp 错误名称：资源不存在为 `NOT_FOUND`；过期输出为 `NOT_FOUND` 且消息含 `EXPIRED`；禁止访问为 `UNAUTHORIZED` 且消息含 `FORBIDDEN`；schema 冲突为 `INVALID_ARGUMENT` 且消息含 `SCHEMA_CONFLICT`。protobuf 同时定义错误码供协议扩展。任何传输失败使当前 reader 作废，调用方从头 open；没有流内重试。已有 DoGet ticket 在输出过期后同样失败，活跃流在下一个批次边界检查 TTL。

## 验证

```bash
(cd dacp-server && sbt test)
.venv/bin/pytest -q dacp-client/tests
# 服务启动后，证书环境变量已设置
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
# convert.yaml 的 s1/s2 成功注册后
.venv/bin/python examples/verify_job.py
```

本次逻辑命名空间更新已通过 41 项服务端 Scala 测试、10 项 Scala SDK 测试（含真实 mTLS 服务测试）、59 项 Python 测试（含算子库服务测试），以及真实 mTLS 下的分层列举、逻辑别名、CSV/PNG、H2 SQL records/Arrow IPC bytes 和交互式客户端检查。已启动隔离端口的服务验证两种 SDK。本次会话无权访问 Docker socket，未重新运行容器 DAG 联调；此前的容器验证包括 zstd 转换、双输入物化/snappy、输出 schema 和 TTL 过期 DoGet。标准 Dockerfile 构建仍需访问 Docker Hub 下载基础镜像。

单元测试覆盖跨节点放置、上游 URI 解析、worker 权限、容量拒绝、失败不重试、描述符/protobuf、递归枚举、路径越界、TTL 销毁、schema 推断/显式声明/冲突、DAG 校验、Arrow 二进制分块到 Parquet 的转换、容器 Arrow 输出契约往返以及 SDK 部分字节读取。安全检查验证缺少客户端证书时连接失败，合法证书下未授权数据集仍被拒绝。

## 与架构决策的对照表

| 已定案决策 | MVP 对应实现 |
|---|---|
| URI 自带位置、节点自治、无注册中心 | Python `Source` / Scala `Config.datasets`，Flight 自描述 |
| Flight GetFlightInfo / DoGet | `Producer`，两种 descriptor 及 protobuf ticket |
| 下游拉取与背压 | Flight `isReady` / 取消回调；Pekko `mapAsync(1)`、有界 FIFO；gRPC 可有有限预取 |
| RecordBatch 分块 | `batch_size` / `byte_chunk`；不实现应用层推送 |
| 无 chunk 重试 / checkpoint / resume / 去重 | reader 失败终止，任务从源头重开，未添加这些机制 |
| 连接级 mTLS + URI 前缀白名单 | `useMTlsClientVerification`、GetFlightInfo/DoGet 两处授权；无登录/token |
| DataSet → Item → Stream | `DataSetAdapter`、`Item`、SDK `DataSet/Stream` |
| 与存储无关的层级命名空间 | `NamespaceAdapter` 显式映射；folder/data、绝对逻辑路径与分层列举 |
| Records / bytes 双视图 | Arrow schema / 单列 binary + SDK 字节输入流 |
| YAML DAG、名称@版本全局库 | `Jobs` 拓扑排序、独立 HTTP(S) 算子库、执行节点校验镜像归档并缓存；保留本地目录模式，无热更新 |
| 计算跟随数据，节点协作 | `JobCoordinator` 按 URI 放置任务，mTLS Flight action 分发；worker 本地执行且不转发 |
| 每子任务一个容器 | docker-java 创建/启动/检查退出码/删除 |
| 单输入流管道，多输入先物化 | 单输入 Arrow FIFO；多输入临时 IPC 文件，finally 清理 |
| 上游输出本节点 Flight 暴露 + TTL | `OutputRegistry`，定时清理、过期 tombstone、旧 ticket 明确报错 |
| CSV/JSON → Parquet 流式转换 | Scala `FormatConvert`，Arrow Java + Parquet Java，固定 row group |
| infer / 显式 schema，冲突仅 fail | 有界采样、后续校验、显式列映射；不自动拓宽 |
| 输出 schema 写入元数据 | Parquet footer、`_schema.json`、输出 Item protobuf |
| 跨语言 protobuf / YAML | `proto/dacp.proto`、YAML job；Python 绑定无需部署 protoc |
| 不做 S3 / pandas / HF | 未实现，保持基础接口；SQL 适配按后续升级需求新增 |

本版输出注册表仅驻内存，重启后需要重跑任务；不会恢复旧任务。Parquet 记录读取支持本转换器生成且带 Arrow schema footer 的文件，其他 Parquet 文件仍可按字节读取。逻辑清单规模随发布的 Item 数量增长，数据正文始终流式读取。

跨节点协作已实现：任意获授权节点可接收 `Job.run(node, yaml)`，通过 Flight `dacp.run_job` 协调 DAG，并用 `dacp.execute_task` 将已解析任务分发到输入所在节点或 YAML 指定节点。输出保留在执行节点，通过原有 Flight 数据接口拉取。详见 [双节点示例与运行步骤](examples/multi-node/README.md)。

```bash
# 根目录：先构建算子镜像，再运行真实双节点 / 多容器验证
bash scripts/test-collaboration.sh
```

`identity@1.0` 内置算子按 Arrow 流保留原始字节，用于跨节点消费已有输出；不限制上游必须是 CSV/JSON。失败会停止下游分发，已产生的输出仍由各节点 TTL 管理；控制响应丢失后不自动重试，也不保证取消远端已开始的任务。
