# DACP

<img src="assets/dacp-logo.png" alt="DACP 标志：连接的节点组成 Collaborative 的首字母 C" width="240">

[English](README_en.md)

DACP 基于 Apache Arrow Flight 提供数据访问与跨节点协作计算。服务器以 Dataset / Item 发布文件、SQL 表和结构化文件内容；客户端浏览数据、读取记录或字节流，并提交由算子组成的任务。计算在数据所在节点执行，输出以实时流返回。

DACP（Data Access and Collaboration Protocol，数据访问与协作协议）的数据模型、传输、信任和协作约定见[协议说明](docs/protocol.md)。

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
| `dacp-server` | 发布数据，处理 Flight 请求，执行本地及跨节点任务 |
| `dacp-shell` | 交互式浏览、SQL 查询、运行算子和提交任务文件；随 Python SDK 安装 |
| `dacp-client` / `dacp-client-scala` | Python / Scala 应用访问数据、执行 Job 和提交 Union 作业 |
| [dacp-union-server](dacp-union-server/README.md) | 管理一个 Union，邀请节点、轮询健康与容量、调度跨节点 DAG、转发最终结果流 |
| [Union Web 看板](dacp-union-server/README.md#http-运行看板) | 公开只读展示节点、任务脚本、可缩放 DAG、流量与执行历史 |
| [dacp-operator-repository](dacp-operator-repository/README.md) | HTTP/HTTPS 算子仓库；公开搜索、版本浏览和下载，管理员通过 CLI 发布 |
| `dacp-operator-runtime` / `dacp-operator-executors` | 共享算子运行环境、JVM / Python / 容器启动器及原生命令流桥接 |
| `dacp-plugin-api` / `dacp-union-protocol` | 插件公开契约和 Union 共享 Flight 传输工具 |

服务端使用 JDK 17 和 Scala 2.13；Python SDK 要求 Python 3.10+。数据浏览不需要 Docker；算子执行需要 Linux 和可访问的 Docker Engine。构建使用 sbt 1.10.x。

## 开始使用

- 已有服务器：从[连接与浏览](docs/users/README.md#连接与浏览)开始。
- 本机体验：按[单节点部署](docs/administrators/README.md#单节点部署)启动服务。
- 跨节点演示：运行[双节点 Union 示例](examples/union/README.md)。
- 自定义计算：阅读[算子编写与发布](docs/operator-authoring.md)。

数据访问与任务执行使用 Flight mTLS，算子仓库与只读看板使用 HTTP/HTTPS。Union 的 CA 信任、客户端白名单和节点计算审批分别配置。每个算子在独立容器内运行，输出不注册为 Dataset；服务器持久化执行元数据，结果内容只通过一次性流交付。
