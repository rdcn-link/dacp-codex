# DACP

<img src="assets/dacp-logo.png" alt="DACP 标志：连接的节点组成 Collaborative 的首字母 C" width="240">

[English](README.md)

DACP 提供数据访问与跨节点协作计算。服务器以 Dataset / Item 发布文件、SQL 表和结构化文件内容；客户端浏览数据、读取记录或字节流，并提交由算子组成的任务。计算在数据所在节点执行，输出以实时流返回。

DACP 基于内嵌的 **XStream 流式执行引擎**，与独立 XStream 共享类型化数据视图、流连接算子、取消和执行策略。DACP 提供认证数据服务，以及由 Union 协调的独立节点协作。参见[项目职责](docs/ecosystem_zh.md)。

DACP（Data Access and Collaboration Protocol，数据访问与协作协议）的数据模型、传输、信任和协作约定见[协议说明](docs/protocol_zh.md)。

Dataset 列表可由 [DataCatalog 插件](docs/dataset-catalogs_zh.md)动态提供；Scala/JVM 应用可通过 [DacpServer API](docs/embedded-server_zh.md)直接启动节点。

节点默认提供 [S3 兼容只读服务](docs/s3-service_zh.md)，可使用 S3 客户端列举 dataset bucket 并下载 Item 字节流。

独立官方网站项目：[dacp-website](https://github.com/rdcn-link/dacp-website/blob/main/README_zh.md)，提供英文项目介绍、按角色分类的在线文档和软件下载。

作业定义、节点子作业、执行策略和变量文件统一采用 YAML。每个 Job 必须提供可读的 `name`，系统另行生成执行 ID；节点与 Union 看板统一展示名称和执行信息。Shell 可同时提交：`submit workflow.yaml --policy policy.yaml --vars variables.yaml`。详见[作业编写](docs/jobs_zh.md)。

## 按角色阅读

| 读者 | 入口 | 主要内容 |
|---|---|---|
| 用户 | [用户指南](docs/users/README_zh.md) | dacp-shell、Python / Scala SDK、查询、作业、结果流和看板 |
| 管理员 | [管理员指南](docs/administrators/README_zh.md) | 服务部署、数据发布、证书与授权、Union 成员、算子仓库、日志和历史记录 |
| 二次开发者 | [开发者指南](docs/developers/README_zh.md) | 算子开发、插件注册、SDK 集成、协议、源码构建和测试 |

[完整文档目录](docs/README_zh.md) · [English documentation](docs/README.md)

## 组件与工具

| 组件 | 用途 |
|---|---|
| `dacp-server` | 发布数据，处理数据访问请求，执行本地及跨节点任务 |
| [dacp-proxy-server](dacp-proxy-server/README_zh.md) | 单上游内网 DACP 公网代理，端到端保留 TLS 和认证 |
| `dacp-shell` | 交互式浏览、SQL 查询、运行算子和提交任务文件；随 Python SDK 安装 |
| `dacp-client` / `dacp-client-scala` | Python / Scala 应用访问数据、执行 Job 和提交 Union 作业 |
| [dacp-union-server](dacp-union-server/README_zh.md) | 管理一个 Union，按配置连接节点、轮询健康与容量、调度跨节点 DAG、转发最终结果流 |
| [Union Web 看板](dacp-union-server/README_zh.md#执行与接口) | 管理界面展示节点、任务脚本、可缩放 DAG、流量与执行历史 |
| `dacp-plugin-api` / `dacp-union-protocol` | 插件公开契约和 Union 共享传输工具 |
| [plugins](plugins/README_zh.md) | 内置 Dataset、文件格式、归档、Catalog 和算子启动插件 |

服务端使用 JDK 17 和 Scala 2.13；Python SDK 要求 Python 3.10+。数据浏览不需要 Docker；算子执行需要 Linux 和可访问的 Docker Engine。构建使用 sbt 1.10.x。

## 开始使用

- 已有服务器：从[连接与浏览](docs/users/README_zh.md#浏览与读取)开始。
- 本机体验：按[单节点部署](docs/administrators/README_zh.md#单节点部署)启动服务。
- 跨节点演示：运行[双节点 Union 示例](examples/union/README_zh.md)。
- 自定义计算：阅读[算子编写与发布](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring_zh.md)。

目录及 Item 元信息允许通过 TLS 匿名访问；数据读取需要 reader 或 runner，执行需要 runner，角色分配给 Token 对应的主体；Union 协作使用双向 TLS 和节点签发的服务凭据。S3 与 DACP 共享主体和角色，使用独立 SigV4 密钥，算子仓库下载公开、发布要求 Token，内网看板公开只读。见[认证与访问控制](docs/security_zh.md)。算子使用隔离的运行环境；符合条件的节点子作业会合并到同一进程执行，见[算子合并执行](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion_zh.md)。输出不注册为 Dataset；服务器持久化执行元数据，结果内容通过一次性流交付。显式执行策略可对指定边界进行物化与重放。

[在线 Job 设计器](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md)：拖拽编排、本地及远程目录浏览、实时算子仓库、结构预览、变量、聊天草案和 YAML 导入导出。

算子仅源码发布：[提交、隔离构建、审核和可信度](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。管理员指定多个分类，开发者选择已注册环境。

独立的 [XStream 引擎](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-engine/README_zh.md)提供本地 `xstream-runner` 程序，并由 DACP Server 内嵌使用。[XStream Designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md)按需浏览本地及配置的远程目录。

[xstream-data：类型化数据流与科学格式插件](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README_zh.md)。

## XStream 集成

XStream 数据、引擎和设计器位于独立的 [xstream-codex 仓库](https://github.com/rdcn-link/xstream-codex)。DACP 依赖版本化 XStream 制品，不直接依赖相邻源码目录。尚未发布的开发环境应先在 XStream 项目执行 `scripts/bootstrap-local.sh /path/to/dacp-codex` 初始化本地制品，再构建 DACP。详见[构建边界](https://github.com/rdcn-link/xstream-codex/blob/main/docs/repositories_zh.md)。

## 独立关联项目

| 项目 | 集成方式 |
|---|---|
| [xstream-engine](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-engine/README_zh.md) | 可独立运行或嵌入的流式作业引擎，提供 xstream-runner 程序 |
| [xstream-designer](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md) | 浏览器拖拽设计、目录浏览、schema 预览与 YAML 导入导出 |
| [xstream-operator-repository](https://github.com/rdcn-link/xstream-operator-repository/blob/main/README_zh.md) | HTTP/HTTPS 算子仓库；公开搜索、版本浏览和下载，源码提交经构建和管理员审核后公开 |
| [xstream-operator-sdk](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-operator-sdk/README_zh.md) | 轻量元数据契约（`contract`）、JVM 模板（`jvm`）与 Python 模板（`python`） |

文档归属与官网发布方式：[维护规则](docs/documentation_zh.md)。
