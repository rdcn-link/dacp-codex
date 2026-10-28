# DACP

<img src="assets/dacp-logo.png" alt="DACP 标志：连接的节点组成 Collaborative 的首字母 C" width="240">

[English](README_en.md)

DACP 提供数据访问与跨节点协作计算。服务器以 Dataset / Item 发布文件、SQL 表和结构化文件内容；客户端浏览数据、读取记录或字节流，并提交由算子组成的任务。计算在数据所在节点执行，输出以实时流返回。

DACP（Data Access and Collaboration Protocol，数据访问与协作协议）的数据模型、传输、信任和协作约定见[协议说明](docs/protocol.md)。

Dataset 列表可由 [DatasetCatalog 插件](docs/dataset-catalogs.md)动态提供；Scala/JVM 应用可通过 [DacpServer API](docs/embedded-server.md)直接启动节点。

节点默认提供 [S3 兼容只读服务](docs/s3-service.md)，可使用 S3 客户端列举 dataset bucket 并下载 Item 字节流。

官方网站模块：[dacp-website](dacp-website/README.md)，提供英文项目介绍、按角色分类的在线文档和软件下载。

作业定义、节点子作业、执行策略和变量文件统一采用 YAML。每个 Job 必须提供可读的 `name`，系统另行生成执行 ID；节点与 Union 看板统一展示名称和执行信息。Shell 可同时提交：`submit workflow.yaml --policy policy.yaml --vars variables.yaml`。详见[作业编写](docs/jobs.md)。

## 按角色阅读

| 读者 | 入口 | 主要内容 |
|---|---|---|
| 用户 | [用户指南](docs/users/README.md) | dacp-shell、Python / Scala SDK、查询、作业、结果流和看板 |
| 管理员 | [管理员指南](docs/administrators/README.md) | 服务部署、数据发布、证书与授权、Union 成员、算子仓库、日志和历史记录 |
| 二次开发者 | [开发者指南](docs/developers/README.md) | 算子开发、插件注册、SDK 集成、协议、源码构建和测试 |

[完整文档目录](docs/README.md) · [English documentation](docs/README_en.md)

## 组件与工具

| 组件 | 用途 |
|---|---|
| `dacp-server` | 发布数据，处理数据访问请求，执行本地及跨节点任务 |
| `dacp-job-designer` | 浏览器拖拽设计、跨节点编排、schema 预览与 YAML 导入导出 |
| `dacp-shell` | 交互式浏览、SQL 查询、运行算子和提交任务文件；随 Python SDK 安装 |
| `dacp-client` / `dacp-client-scala` | Python / Scala 应用访问数据、执行 Job 和提交 Union 作业 |
| [dacp-union-server](dacp-union-server/README.md) | 管理一个 Union，按配置连接节点、轮询健康与容量、调度跨节点 DAG、转发最终结果流 |
| [Union Web 看板](dacp-union-server/README.md#执行与接口) | 管理界面展示节点、任务脚本、可缩放 DAG、流量与执行历史 |
| [dacp-operator-repository](dacp-operator-repository/README.md) | HTTP/HTTPS 算子仓库；公开搜索、版本浏览和下载，管理员通过 CLI 发布 |
| `dacp-operator-runtime` / `plugins/dacp-plugin-operator-executors` | 共享算子运行环境、JVM / Python / 容器启动器及原生命令流桥接 |
| `dacp-operator-contract` | 共享的静态算子参数类型、默认值与约束验证 |
| `dacp-plugin-api` / `dacp-union-protocol` | 插件公开契约和 Union 共享传输工具 |
| [plugins](plugins/README.md) | 内置 Dataset、文件格式、归档、Catalog 和算子启动插件 |

服务端使用 JDK 17 和 Scala 2.13；Python SDK 要求 Python 3.10+。数据浏览不需要 Docker；算子执行需要 Linux 和可访问的 Docker Engine。构建使用 sbt 1.10.x。

## 开始使用

- 已有服务器：从[连接与浏览](docs/users/README.md#浏览与读取)开始。
- 本机体验：按[单节点部署](docs/administrators/README.md#单节点部署)启动服务。
- 跨节点演示：运行[双节点 Union 示例](examples/union/README.md)。
- 自定义计算：阅读[算子编写与发布](docs/operator-authoring.md)。

普通数据访问与任务执行使用 TLS 与 Token；Union 协作使用双向 TLS 和节点签发的服务凭据。S3 使用独立 SigV4 认证，算子仓库下载公开、发布要求 Token，内网看板公开只读。见[认证与访问控制](docs/security.md)。算子使用隔离的运行环境；符合条件的节点子作业会合并到同一进程执行，见[算子合并执行](docs/operator-fusion.md)。输出不注册为 Dataset；服务器持久化执行元数据，结果内容只通过一次性流交付。

[在线 Job 设计器](dacp-job-designer/README.md)：拖拽编排、Union 节点选择、实时算子仓库、结构预览、变量、聊天草案和 YAML 导入导出。
