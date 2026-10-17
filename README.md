# dacp MVP

<img src="assets/dacp-logo.png" alt="DACP 标志：以相连节点组成的 C，代表协作" width="480">

[English](README_en.md)

面向自治数据节点的 Arrow Flight 数据协作协议。Scala 2.13 服务端、Python/PyArrow SDK、protobuf 元数据、YAML DAG、一任务一 Docker 容器。节点无需部署集群或中心注册服务。目录与 JDBC SQL 数据库可同时作为数据集暴露，SQL 表作为单层 Item。

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

另一终端，从项目根目录运行：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python examples/client.py
```

输出包含 `events.json`、`nested/orders.csv`、`pixel.png`，CSV 两条记录和 PNG 字节数。TLS 证书仅用于本地示例；脚本生成 CA、服务端和客户端证书，服务端证书也允许 clientAuth，供本节点任务执行器连接其他信任该 CA 的节点。

```python
from dacp import Source

with Source.open("dacp://localhost:8815/data1") as ds:
    print(ds.list())
    for batch in ds.open("nested/orders.csv").as_records():
        print(batch.to_pydict())
    with ds.open("pixel.png").as_bytes() as stream:
        header = stream.read(8)
```

URI 默认端口为 8815，也支持直接打开 Item：`Source.open("dacp://localhost:8815/data1/nested/orders.csv")`。三份证书也可用 `tls_root_certs`、`cert_chain`、`private_key` 的 bytes 参数传给 `Source.open`。Dataset 和直接打开的 Stream 支持 context manager。

## 交互式客户端

安装 Python SDK 后，可连接远端数据集，在终端列举 Item 并预览内容。使用已有的 `DACP_CA`、`DACP_CERT`、`DACP_KEY` 环境变量，或显式提供证书路径：

```bash
.venv/bin/pip install -e './dacp-client[test]'
.venv/bin/dacp dacp://localhost:8815/data1 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

连接远端时，将 `localhost:8815/data1` 替换为实际服务器地址、端口和数据集名称。也可用 `.venv/bin/python -m dacp.cli` 替代 `.venv/bin/dacp`。

```text
ls
show 1
show "nested/orders.csv" records
show "events.json" text
show "pixel.png" hex
use csv_data
connect dacp://localhost:8815/data1
help
quit
```

`ls` 刷新清单并显示编号、格式和路径。`show` 接受编号或带引号的路径；默认对 CSV/JSON/NDJSON/Parquet 使用记录视图，其他格式使用十六进制预览。`text` 按 UTF-8 显示原始字节并转义终端控制字符；SQL 表应使用 `records`，其 bytes 视图为 Arrow IPC。默认最多显示 20 行或 4096 字节，可用 `--rows`、`--bytes` 调整；每行渲染文本也受 `--bytes` 字符数限制。记录预览停止时取消上游读取，避免下载整个数据集。二进制文件显示十六进制及 ASCII，不直接打开图片。失败后可重试命令或重新连接；连接切换失败保留旧连接。

服务器目前不提供数据集发现接口，因此启动和 `connect` 必须指定已知数据集 URI；`use` 切换同一服务器上的已知数据集。使用 `quit`、`exit` 或 EOF 退出并关闭连接。

## 同时提供目录和 SQL 数据库

新增 `sql_datasets` 配置，现有 `datasets` 目录配置保持兼容。SQL 表通过同样的 `Source.open / list / open / as_records` API 访问；表清单包含 Arrow schema，字节视图为 Arrow IPC 流。内置 H2 与 PostgreSQL JDBC 驱动，支持表白名单、环境变量凭据和有界 fetch/batch。详见 [SQL 配置、类型与运行步骤](examples/sql/README.md)。

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

启动 Flight 后执行 YAML 中的 DAG。`csv_data` 是指向 `examples/data/nested` 的 CSV 数据集；它与演示 PNG 读取的混合数据集 `data1` 共用本地文件。成功日志显示 `s1`、`s2` 的 URI，Parquet 位于 `examples/output/<output>-<uuid>/`。服务继续运行并暴露输出，TTL 到期自动删除；停止服务也会清理输出。

从项目根目录、沿用上面的证书环境变量：

```bash
.venv/bin/python examples/verify_job.py
```

输出清单中的 Parquet Item 必须携带 schema。原始 CSV/JSON 的清单 schema 可为空（枚举不扫描数据）；打开具体 Item 的 FlightInfo 才推断 schema。

算子库支持独立 Web 服务：配置 `operator_library_url` 后，执行节点远程下载任务指定版本的镜像归档、校验并缓存，再使用本地 Docker 隔离执行。启动、发布与配置见 [dacp-operator-library](dacp-operator-library/README.md)。不配置 URL 时仍使用本地 `operators/<name>/<version>/operator.yaml`。版本不可覆盖，无动态版本更新。标准 Dockerfile 在构建阶段执行 `sbt stage`，运行阶段只保留 JRE 和应用。不要把生成的证书或数据挂入算子镜像。

### DAG 与算子契约

`input` 是单个 URI 或上游 `output` 名；`inputs` 是多个此类值组成的列表。上游引用也支持 `s1/part-0.parquet`。任务先校验重复名称、未知依赖、环和目标节点，再按拓扑顺序串行执行。默认在第一个输入 URI 所在节点执行，也可用 `node: dacp://host:port` 显式指定。协调节点把上游引用解析为具体 URI，再通过 mTLS Flight action 分发；无中心注册服务。每个子任务新建容器，禁用容器网络，挂载只读 `/input` 和可写 `/output`。

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
| GetFlightInfo `[datasets, dataset]` | `app_metadata` 为 protobuf `DataSet`，包含递归打平的 Item 清单，无 DoGet endpoint |
| GetFlightInfo `[datasets, dataset, relative/item]` | Arrow schema + 含 protobuf `Ticket` 的 FlightEndpoint |
| DoGet `Ticket(dataset,item,view)` | `view=records` 为记录，`view=bytes` 为单列 binary（原始文件字节） |

记录视图支持 CSV、NDJSON、dacp 生成的 Parquet；任意文件可用 bytes。SDK 将 binary batch 包装成可增量读取的 `io.BufferedReader`。ticket 只描述资源和视图，不是认证 token。GetFlightInfo 和 DoGet 均校验 URI 白名单、规范化路径和符号链接边界。越界链接不会出现在清单中。

`examples/server.yaml` 配置：

| 字段 | 含义 |
|---|---|
| `host` / `port` | 监听地址 / 端口 |
| `public_host` | 对外 URI 主机名，白名单使用该名字及显式端口 |
| `datasets` | 数据集名称 → 本地目录；MVP 名称为一个 URI 路径段 |
| `allowed_uri_prefixes` | 节点级 URI 前缀白名单，按路径段边界匹配；输出名称也必须授权 |
| `tls_cert` / `tls_key` / `tls_ca` | PEM 证书、私钥、可信客户端 CA；强制 mTLS |
| `batch_size` | CSV/JSON 记录视图每个 batch 的最大行数 |
| `byte_chunk` | 字节视图每个 binary 行的最大字节数 |
| `infer_sample` | 服务端记录视图推断采样 batch 数 |
| `output_ttl_seconds` | 从成功注册开始计算输出寿命 |
| `sql_datasets` | JDBC 数据集，url/user_env/password_env/schema/tables/fetch_size/query_timeout_seconds |
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

当前环境已通过 20 项 Scala 测试、15 项 Python 测试（含算子库服务测试），以及真实 mTLS 同节点目录/H2 SQL 表（records 与 Arrow IPC 字节视图）、枚举/CSV/PNG、容器内 zstd 转换、双输入物化/snappy、输出 schema 和 TTL 过期 DoGet 检查。`sbt run` 已实际启动验证。当前网络无法拉取 Docker Hub 的 Temurin 基础镜像，容器联调使用本机 JRE 与同一 `sbt stage` 产物构建的临时测试镜像；仓库内标准多阶段 Dockerfile 的基础镜像下载仍需可访问 Docker Hub 的环境。

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
| 目录递归打平、数据库单层同构 | `DirectoryAdapter` 递归文件；`SqlAdapter` JDBC 单层表与 Arrow schema |
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
| 不做 S3 / 下钻 / pandas / HF | 未实现，保持基础接口；SQL 适配按后续升级需求新增 |

本版输出注册表仅驻内存，重启后需要重跑任务；不会恢复旧任务。Parquet 记录读取支持本转换器生成且带 Arrow schema footer 的文件，其他 Parquet 文件仍可按字节读取。目录清单规模随文件数量增长，数据正文始终流式读取。

跨节点协作已实现：任意获授权节点可接收 `Job.run(node, yaml)`，通过 Flight `dacp.run_job` 协调 DAG，并用 `dacp.execute_task` 将已解析任务分发到输入所在节点或 YAML 指定节点。输出保留在执行节点，通过原有 Flight 数据接口拉取。详见 [双节点示例与运行步骤](examples/multi-node/README.md)。

```bash
# 根目录：先构建算子镜像，再运行真实双节点 / 多容器验证
bash scripts/test-collaboration.sh
```

`identity@1.0` 内置算子按 Arrow 流保留原始字节，用于跨节点消费已有输出；不限制上游必须是 CSV/JSON。失败会停止下游分发，已产生的输出仍由各节点 TTL 管理；控制响应丢失后不自动重试，也不保证取消远端已开始的任务。
