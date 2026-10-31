# DACP 与 XStream 项目职责

[English](ecosystem.md)

## 基于 XStream 引擎

DACP Server 内嵌 XStream 流式执行引擎，并共享 JVM 内的 DataCatalog。DataCatalog → Dataset → Item 提供类型化数据视图，Job 步骤通过流连接。引擎负责本地执行、取消、执行策略、可选物化/重放，以及符合条件的同进程算子合并。XStream 另有独立 Runner，因此引擎可在 DACP 服务之外复用。

DACP 提供认证数据发布与访问、客户端 SDK 和 Shell、独立 S3 访问、执行历史及服务看板。Union 负责成员管理、健康轮询、自动放置、子作业分发，以及成员间直接流传输。客户端向服务提交逻辑 Job，无需知道对方是普通节点还是 Union。

## 仓库与职责

| 项目 | 负责内容 | DACP 如何使用 |
|---|---|---|
| [dacp-codex](https://github.com/rdcn-link/dacp-codex) | 数据服务、Union、代理、客户端、公开集成契约和服务插件 | 主服务项目 |
| [xstream-codex](https://github.com/rdcn-link/xstream-codex) | 数据 SDK 和格式插件、引擎/Runner、Designer、算子 SDK、推导、执行器和源码分析插件 | 版本化库依赖与独立设计工具 |
| [xstream-operator-repository](https://github.com/rdcn-link/xstream-operator-repository) | 公开算子/运行环境目录、仅源码提交、隔离构建和管理员审核 | 配置的公共程序包来源，独立部署 |
| [dacp-website](https://github.com/rdcn-link/dacp-website) | DACP 介绍、服务文档与下载 | 独立官方网站 |
| [xstream-website](https://github.com/rdcn-link/xstream-website) | 引擎/Designer 文档、算子开发与部署指导、仓库目录和集成 Designer 演示 | 独立生态文档与设计体验 |

## 构建与部署

普通 DACP 构建解析版本化 XStream 制品，不引用相邻源码目录。未发布开发制品需按 XStream 仓库说明显式初始化。仓库服务通过版本化制品依赖算子契约和源码分析插件，不需要 DACP 服务。官网各自携带静态资源与文档；XStream 官网可选的集成演示还部署自己的一套 Designer 发行包和推导环境。

DACP 数据节点、Union 与仓库、Designer、官网独立部署。执行服务通过 `remote_operator_library_urls` 配置仓库。仓库源码提交需要认证并经审核，与数据节点 DEBUG 模式下直接执行源码是两件事。批准的算子程序包公开下载。这些职责边界不意味着协作项目之间没有 API 或制品依赖。

## 文档入口

数据访问、服务授权、协作、Job 提交和运维阅读 DACP 文档；引擎嵌入、类型化流、Designer、算子接口和部署阅读 XStream 文档；目录下载、环境记录和管理员审核使用仓库网站。官网构建提供可直接阅读的站内说明，源码仓库链接仍用于实现与贡献。
