# 技术参考

[返回项目说明](../README_zh.md) · [English](reference.md)

本页说明格式读取、元数据、算子契约及 Flight 协议；首次使用请先阅读项目说明。

## 格式与视图

format 标识源数据表示方式，view 标识读取方式。客户端使用服务器公布的 `views`，包括以后新增的格式，不再根据文件名猜测能力。格式名称使用小写。

| 来源 | Format | Views |
|---|---|---|
| `.csv` | `csv` | `records`, `bytes` |
| `.jsonl`, `.ndjson` | `ndjson` | `records`, `bytes` |
| `.json` | `json` | `bytes` |
| `.zip`, `.ZIP` | `zip` | `bytes` |
| `.tar` | `tar` | `bytes` |
| `.gz` | `gzip` | `bytes` |
| `.tar.gz`, `.tgz` | `tar-gzip` | `bytes` |
| `.xls`, `.xlsx` | `excel` | `records`, `bytes` |
| DACP Parquet | `parquet` | `records`, `bytes` |
| SQL 表 | `sql` | `records`, `bytes` |
| 其他文件 | `bin` | `bytes` |

注册表优先使用显式 `format`，否则依次匹配归档组合后缀、已注册的文件签名和不区分大小写的扩展名，最终回退为 `bin`。检测最多读取八个签名字节，不验证整个文件；结构化内容错误在打开记录视图时报告。已识别文件仍可通过 bytes 原样读取。Parquet 目前要求 DACP Arrow schema footer 元数据。普通 JSON 仅提供 bytes；对于按行存储对象的 `.json` 文件，配置 `format: ndjson` 即可启用记录视图。SQL 的格式为 `sql`，bytes 视图使用 Arrow IPC。

### ZIP 归档浏览

将 ZIP 文件映射为普通文件别名，服务器自动将其发布为 `kind=folder`、`format=zip`，提供原始归档的 `bytes` 视图。`list("/archive")` 列出归档的直接子项，`list()` 包含全部归档条目；缺失的父集合自动补齐。条目按扩展名选择格式，打开时使用现有格式处理器检测和读取。ZIP 内的 ZIP 暂作为 `bin` data Item，不递归展开。所有 folder 的修改时间、大小及记录数仍为未知。

```yaml
datasets:
  public:
    provider: mixed
    items:
      /archive: {file: /private/reports.ZIP}
```

```python
ds.list("/archive")
ds.list("/archive/reports")
with ds.open("/archive/reports/orders.csv") as stream:
    records = stream.as_records()
```

列举只读取 ZIP 目录，不解压条目。打开、推断 schema 或计数时，仅将所需条目解压到服务器临时文件，完成、失败或取消后清理；因此大条目的首次响应需要等待解压。每个归档最多 10,000 个条目，单条目解压上限 512 MiB，目录声明的总大小上限 2 GiB。绝对路径、路径穿越、重复条目、文件与父集合冲突均被拒绝；不解压到源目录，也不跟随归档内符号链接。`dacp-shell` 可用 `ls /archive` 浏览、用 `cat /archive hex` 预览原始归档。Python 和 Scala SDK 均按 advertised views 打开可读集合；普通集合没有读取视图。

### TAR、GZIP 与压缩 TAR

`tar`、`gzip` 和 `tar-gzip` 与 `zip` 一样发布为 folder，仅提供原始文件的 bytes 视图。`.tar.gz` 和 `.tgz` 识别为单个压缩 TAR，直接列出 TAR 条目，没有中间 `.tar` 子项。扩展名不区分大小写；显式 `format` 可覆盖后缀检测。

普通 `.gz` 提供一个解压后的 data 子项。公开别名 `/orders.csv.gz` 对应 `/orders.csv.gz/orders.csv`；别名不以 `.gz` 结尾时使用 `content`。GZIP 头中的私有文件名不参与命名，子项的修改时间和列举大小保持未知；按内容格式提供 records 和 bytes。连续 GZIP members 被合并为同一个解压流。

```python
with Source.open("dacp://localhost:8815/data1") as ds:
    print(ds.list("/bundle_gzip"))
    with ds.open("/orders.csv.gz") as stream:
        with stream.as_bytes() as reader:
            print(reader.read(2))
    with ds.open("/orders.csv.gz/orders.csv") as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()
```

TAR 列举顺序扫描归档；压缩 TAR 需要解压扫描。为验证整个目录及校验和，读取选定条目也会扫描完整归档，再只暂存所需文件供现有读取器使用。GZIP 列举不解压数据正文，读取时解压到临时文件。完成、异常或取消均清理临时文件；扫描及解压过程检查取消。归档大小没有跨请求索引缓存。

与 ZIP 一样，最多 10,000 个条目、单条目解压最多 512 MiB、声明总大小最多 2 GiB；TAR 的解压流另外最多允许 16 MiB 的头、填充和尾部开销。TAR 支持普通文件及目录，拒绝符号链接、硬链接、稀疏文件、设备及其他特殊条目，校验头与 GZIP CRC。常见的条目前缀 `./` 及根目录条目会被规范化，其余路径穿越、重复名称及文件/父目录冲突均被拒绝。所有 folder 属性仍为未知；嵌套归档保持二进制 data，不递归展开。BZ2、XZ 和 Zstandard 解压尚未实现。

### 读取选项与 Excel

文件映射支持以下私有读取选项：

```yaml
datasets:
  office:
    provider: mixed
    items:
      /orders:
        file: /private/sales.xlsx
        format: excel
        options: {sheet: Orders, header_row: 2}
        schema:
          - {name: id, type: int64}
          - {name: amount, type: float64}
      /events:
        file: /private/events.json
        format: ndjson
```

`options.sheet` 和 `options.header_row` 仅用于 Excel；表头行号从 1 开始，默认值为 1。单工作表文件自动选择唯一工作表；多工作表文件必须显式指定。可用多个逻辑别名分别发布工作表。每个别名的 bytes 返回原始完整工作簿，records 和 count 仅对应所选工作表。工作表绑定及底层文件名不会出现在元数据或 ticket 中。

Excel 使用 Apache POI 5.5.1 的 XLS/XLSX 事件读取接口。空表头生成 `column_N`，重复名称依次添加 `_2`、`_3`，缺失单元格返回 null。空数据行跳过，超出表头宽度的数据报错。公式只读取缓存结果，不重新计算；可识别的缺失缓存返回 null。错误单元格、加密或无效工作簿返回 DACP 格式错误。整数且未越界的数字单元格使用 int64，其余数字使用 float64，布尔值及字符串保留其值。Excel 数字日期保留序列值，不自动推断地区或时区。可选 `schema` 通过有序的 `name`/`type` 配置列，支持 `int64`、`float64`、`bool` 和 `utf8`/`string`。未配置时采样不超过 `batch_size * infer_sample`，后续类型冲突报错，不修改既定 schema。

列举不解析 CSV、NDJSON 或 Excel 记录；除发布时已填充的记录数外，其记录数保持未知。打开记录视图时通过有限采样推断 schema；除可选的启动计数外，只有显式 `count` 扫描文件，Parquet 可直接读取 footer 中的数量。Excel 解析队列最多保留 16 行，关闭或取消时停止解析；schema、当前 batch、单个单元格及 POI 共享字符串表还会占用内存，大型共享字符串表仍可能需要较多内存。不会在内存中构建完整工作表。

服务端扩展接口为 [`FormatHandler`](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-core/src/main/scala/xstream/data/FormatHandler.scala)。提供者实现检测、可选的低成本元数据、schema、记录读取、显式计数及资源释放；文件 bytes 使用公共读取器。将提供者 JAR 加入类路径，在 `META-INF/services/xstream.data.FormatHandler` 注册公开的无参类并重启。格式 ID 和扩展名注册必须唯一，`sql` 保留给 SQL adapter。新增格式不需要修改核心服务端或 SDK。参见[处理器开发说明](format-handlers_zh.md)。

## Item 属性

Item 元数据新增以下可选属性；未知值与已知的 `0` 明确区分：

| 属性（Python / Scala） | 含义 |
|---|---|
| `last_modified_time_ms` / `lastModifiedTimeMs` | UTC Unix 毫秒时间戳。Python 的 `last_modified_time` 返回带 UTC 时区的 datetime；Scala 的 `lastModifiedTime` 返回 `Option[Instant]`。 |
| `total_size_bytes` / `totalSizeBytes` | 内容的总字节数；文件映射使用原始文件大小。 |
| `record_count` / `recordCount` | 精确的逻辑记录数。CSV 不包含表头，NDJSON 忽略空行，Excel 统计所选工作表的数据行。这些文件可使用发布时保存的数量；未填充时及 SQL 在普通浏览时数量为未知，使用显式 `ds.count(path)`；Parquet 从 footer 读取。 |

Python 未知值为 `None`，Scala 为 `None`（已知值为 `Some`），protobuf 中对应字段缺省。SQL 映射不推测修改时间或字节数；PNG 等非记录数据的记录数为未知。folder 的三个属性始终为未知，包括空集合；计算集合属性时不会访问其子 Item。

这些属性由 `list()`、`list(path)` 和 data Item 的 FlightInfo 返回，交互式客户端 `ls` 也显示它们。显式 CSV/NDJSON/Excel 计数会扫描文件，显式 SQL 计数会执行查询；普通列举不进行这些计数。SQL 计数遵循源查询超时。集合元数据不会触发后代扫描；data Item 的统计不跨请求缓存。属性是观测值，不与后续读取组成原子快照，源数据变化时可能改变。

```python
with Source.open("dacp://localhost:8815/data1") as ds:
    for item in ds.list("/"):
        print(item.path, item.last_modified_time, item.total_size_bytes, item.record_count)
```

### 发布时填充记录数

配置 `count_file_records_on_startup: true` 后，服务器在加载配置时对 mixed 文件映射和目录初次发现的记录文件执行一次精确计数并保存结果，普通浏览复用该结果。默认关闭。计数与文件大小和修改时间绑定；计数期间或之后检测到文件变化时，结果为未知。重启会重新计数，显式 `count` 仍按当前内容计算且不修改保存的元数据。该选项不计数 SQL、归档子项或 folder；启用时无效记录文件会导致启动失败。

`plugin_directory` 可选指定插件及依赖 JAR 所在目录，在注册表初始化前加载，修改后需要重启。读取器专用参数放入 `options`，由处理器自行校验。独立 API 与结构化文件契约见 [插件指南](plugins_zh.md)。

## Dataset 构建

目录使用可选 `reader_overrides`，SQLDB 使用公开路径到私有表的 `tables` 映射，mixed 使用 `items`。目录和 SQLDB 不接受 `items`。插件默认使用 `items`，可通过 `DatasetProvider.itemSettingsKey` 自定义映射键。

内置 `directory`、`sqldb` 和 `mixed` provider。每个 Dataset 选择一个 provider，由它解释私有 `source` 和各 provider 的映射配置，并提供统一的数据访问接口。目录 provider 动态发现相对路径，SQLDB provider 使用显式公开表别名，mixed provider 可混合来源。每个 Dataset 必须显式指定 `provider`。

目录 Dataset 不跟随符号链接。集合属性和直接子项列举不递归扫描普通子目录；只有 `list()` 才递归发现所有文件和归档条目。配置加载时启用启动计数会扫描初始文件，后续新文件只被发现、不自动计数。读取选项覆盖按公开路径匹配，不改变文件命名。目录中的新增 SQL 绑定或绝对路径映射被拒绝；这类组合应使用 mixed。

Dataset provider、容器 reader、格式 handler 分别负责构建数据集、浏览归档、解释内容。MongoDB 等未来 provider 可自行实现列举、元数据及流式读取，无需伪装成文件或 SQL 表，也不要求修改协议或 SDK。参见 [provider 开发指南](dataset-providers_zh.md)。

## 任务执行与算子

任务输入只引用上游步骤的输出，使用 `task[:port]`，省略端口时为 `result`。外部数据项通过 `read_item@1.0` 的 `params.uri/view` 读取，详见[作业格式](jobs_zh.md)。输出为命名流，不保存数据集。shell 支持 `run OP --input source=/item` 和 `submit job.yaml`，并逐块打印结果。普通任务通过 TLS 与 Token 的 Flight DoExchange 协作；流水线任务使用有界队列并发执行。每个输出端口最多有一个下游消费者；多个输入按端口名称字典序消费。

算子启动参数为 `operator /input /input/params.json`。输入为 Arrow IPC FIFO，由 `/input/manifest.json` 描述。stdout 写 start/data/end 协议帧，stderr 写诊断。无输出目录、输出注册、TTL 或结果数据集 URI。Parquet 转换使用私有临时空间完成 footer 后流式发送字节。

源码示例、端口、帧格式、运行时白名单、取消与限制见[编写契约](operator-authoring_zh.md)、[流契约](operator-streams_zh.md)与 [DEBUG heredoc 源码](operator-source_zh.md)。

算子首先从本地目录及已安装镜像解析，然后检查已完成的下载缓存，最后按配置顺序查询远程 URL。清单返回 HTTP 404 时继续查询下一个库；其他下载或验证错误会终止执行。配置示例：`remote_operator_library_urls: [http://library-a:8820, https://library-b:8820]`；空列表仅使用本地算子。

运行时镜像与算子代码分别作为按内容摘要寻址的制品。内置启动插件支持完整容器、Java/Scala JAR、Python 源码和 wheel，均在隔离的运行时容器中执行。定义、共享缓存、执行器注册与当前限制参见 [算子打包](operator-packaging_zh.md)。


## 协议与配置

唯一元数据定义：[proto/dacp.proto](../proto/dacp.proto)。Scala 在 sbt 构建时生成 Java 类，Python 绑定已提交。修改协议后用系统 protoc（3.21+）重新生成：

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

记录视图支持 CSV、NDJSON、Excel、SQL 和 dacp 生成的 Parquet；任意文件可用 bytes。SDK 将 binary batch 包装成可增量读取的 `io.BufferedReader`。ticket 只描述资源和视图，不是认证 token。GetFlightInfo 和 DoGet 均校验已发布命名空间、规范化路径和符号链接边界。越界链接不会出现在清单中。

`examples/server.yaml` 配置：

| 字段 | 含义 |
|---|---|
| `host` / `port` | 监听地址 / 端口 |
| `public_host` | 对外公布的 URI 主机名 |
| `public_port` | 对外公布的端口，默认使用监听端口；代理部署时可与 `port` 不同 |
| `datasets` | Dataset 名称 → provider、source 及 items；名称为一个 URI 路径段 |
| `tls_cert` / `tls_key` / `tls_ca` | PEM 服务器证书、私钥与信任 CA；普通客户端使用 TLS / Token，协作使用双向 TLS |
| `count_file_records_on_startup` | 启动时填充直接发布的记录文件数量，默认 false；增加启动扫描成本 |
| `batch_size` | 文件记录视图每个 batch 的最大行数 |
| `byte_chunk` | 字节视图每个 binary 行的最大字节数 |
| `infer_sample` | 服务端记录视图推断采样 batch 数 |
| `sources` | 私有 JDBC 数据源（不自动发布表），url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
| `remote_operator_library_urls` / `operator_cache` / `operator_max_bytes` | 有序的远程算子库 URL 列表 / 本地不可变版本缓存 / 单个制品最大字节数（默认 8 GiB） |
| `local_operator_library` | 本地算子库 |
| `task_peers` | 允许分发任务的节点 origin；自身始终允许，无中心发现服务 |
| `task_input_prefixes` | 可选的执行节点输入 URI 前缀限制；省略或为空时不增加前缀限制 |
| `max_concurrent_tasks` | 本地任务及控制请求并发上限（默认 2）；满载立即返回 UNAVAILABLE |

节点发布的数据集默认对通过 TLS / Token 认证的客户端开放；不另设数据读取白名单。Union 协作使用节点签发的服务凭据和双向 TLS；节点间流还须匹配精确执行计划。

服务错误使用 Flight/gRPC 状态和明确的 dacp 错误名称：资源不存在为 `NOT_FOUND`；禁止访问为 `UNAUTHORIZED` 且消息含 `FORBIDDEN`；schema 冲突为 `INVALID_ARGUMENT` 且消息含 `SCHEMA_CONFLICT`。protobuf 同时定义错误码供协议扩展。任何传输失败使当前 reader 作废，调用方从头 open；没有流内重试。算子结果只通过流返回，不注册输出数据集或使用输出 TTL。

服务端 `mode` 默认为 `NORMAL`。内嵌源码要求协调与执行节点均为 `DEBUG`；`source_runtimes` 将运行时引用映射为管理员选择的 `image` 与 `executor` 配置。

## DataCatalog

`data_catalog` 选择目录 provider，通过 `source` 传入插件私有参数；不能同时配置非空 `datasets`。未配置时使用静态目录。具体契约、JDBC 示例、发现和授权见[动态目录](dataset-catalogs_zh.md)。标准 Flight `ListFlights` 返回公开 Dataset URI 及描述，不额外要求数据授权，不返回 Item 内容。

## S3 兼容端口

节点默认启动SigV4 认证的只读 HTTPS S3 服务：dataset 为 bucket，支持 `bytes` 的 Item 为 object。配置 `s3.enabled`（默认 true）、`s3.host`（默认 `0.0.0.0`）与 `s3.port`（默认实际 Flight 端口加 2000）。使用 path-style 与 SigV4 签名请求。操作范围、客户端与插件示例见 [S3 兼容服务](s3-service_zh.md)。
