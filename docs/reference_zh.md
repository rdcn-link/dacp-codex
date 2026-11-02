# DACP 服务参考

[中文](reference_zh.md) · [English](reference.md)

DACP 发布 Dataset，提供远程数据访问和 Job 执行。数据集配置、格式读取及本地执行由 XStream 提供。

- [数据模型与 Item](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-model_zh.md)
- [文件格式、压缩包和读取参数](https://github.com/rdcn-link/xstream-codex/blob/main/docs/file-formats_zh.md)
- [Dataset provider 配置与扩展](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-providers_zh.md)
- [Job 语言](https://github.com/rdcn-link/xstream-codex/blob/main/docs/jobs_zh.md) · [提交远程 Job](jobs_zh.md)

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

可用视图与格式由配置的 XStream 插件提供；打开视图前请查看 Item 元信息。SDK 将 binary batch 包装成可增量读取的 `io.BufferedReader`。ticket 只描述资源和视图，不是认证 token。GetFlightInfo 和 DoGet 均校验已发布命名空间、规范化路径和符号链接边界。越界链接不会出现在清单中。

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

节点发布的目录、Item 列表及元信息允许通过 TLS 匿名访问；数据读取需要 reader 或 runner，执行和结构推导需要 runner。Union 协作使用节点签发的服务凭据和双向 TLS；节点间流还须匹配精确执行计划。

服务错误使用 Flight/gRPC 状态和明确的 dacp 错误名称：资源不存在为 `NOT_FOUND`；禁止访问为 `UNAUTHORIZED` 且消息含 `FORBIDDEN`；schema 冲突为 `INVALID_ARGUMENT` 且消息含 `SCHEMA_CONFLICT`。protobuf 同时定义错误码供协议扩展。任何传输失败使当前 reader 作废，调用方从头 open；没有流内重试。算子结果只通过流返回，不注册输出数据集或使用输出 TTL。

服务端 `mode` 默认为 `NORMAL`。内嵌源码要求协调与执行节点均为 `DEBUG`；`source_runtimes` 将运行时引用映射为管理员选择的 `image` 与 `executor` 配置。

## DataCatalog

`data_catalog` 选择目录 provider，通过 `source` 传入插件私有参数；不能同时配置非空 `datasets`。未配置时使用静态目录。具体契约、JDBC 示例、发现和授权见[动态目录](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-catalogs_zh.md)。标准 Flight `ListFlights` 返回公开 Dataset URI 及描述，不额外要求数据授权，不返回 Item 内容。

## S3 兼容端口

节点默认启动SigV4 认证的只读 HTTPS S3 服务：dataset 为 bucket，支持 `bytes` 的 Item 为 object。配置 `s3.enabled`（默认 true）、`s3.host`（默认 `0.0.0.0`）与 `s3.port`（默认实际 Flight 端口加 2000）。使用 path-style 与 SigV4 签名请求。操作范围、客户端与插件示例见 [S3 兼容服务](s3-service_zh.md)。
