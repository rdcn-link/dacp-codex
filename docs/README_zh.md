# DACP 文档

[English](README.md) · [项目首页](../README_zh.md)

作业格式及流依赖见[作业说明](jobs_zh.md).

协议总览：[DACP（Data Access and Collaboration Protocol）](protocol_zh.md)。

动态数据集目录与程序内启动：[DataCatalog](dataset-catalogs_zh.md)、[嵌入式服务器](embedded-server_zh.md)。

当前协作部署的[算子库清单](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-catalog_zh.md)：6 个已发布算子的端口、参数、输入输出和 Shell / Job 示例。

## 按角色阅读

| 入口 | 适用对象 | 阅读路线 |
|---|---|---|
| [用户指南](users/README_zh.md) | 使用已部署服务的数据使用者、分析人员和应用使用者 | 连接 → 浏览与查询 → 执行作业 → 看板和历史 |
| [管理员指南](administrators/README_zh.md) | 部署节点、Union 或算子仓库的运维人员 | 环境与证书 → 发布数据 → 算子与授权 → 监控、存储和停机 |
| [开发者指南](developers/README_zh.md) | 算子作者、应用集成者及插件和协议开发者 | 模块架构 → 扩展契约 → 实现与注册 → 测试 |

## 工具与模块手册

| 工具 | 文档 |
|---|---|
| dacp-shell、Python SDK | [用户指南](users/README_zh.md)、[查询语义](records-scans_zh.md)（英文） |
| DACP 代理服务器 | [单上游 TCP 代理](../dacp-proxy-server/README_zh.md) |
| Scala SDK | [Scala 客户端](../dacp-client-scala/README_zh.md) |
| DACP Server | [管理员指南](administrators/README_zh.md)、[配置和协议参考](reference_zh.md) |
| Union Server 与 Web 看板 | [Union 手册](../dacp-union-server/README_zh.md)、[协作协议](union_zh.md)、[双节点示例](../examples/union/README_zh.md) |
| 算子仓库 CLI 与网站 | [仓库手册](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-operator-repository/README_zh.md) |
| 插件 API 与 Union 传输工具 | [插件 API](../dacp-plugin-api/README_zh.md)、[Union 协议模块](../dacp-union-protocol/README_zh.md) |

## 专题参考

| 主题 | 文档 |
|---|---|
| 算子编写、发布、代码示例 | [Scala / Java / Python 算子](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring_zh.md)、[原生命令](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands_zh.md) |
| DEBUG 源码提交 | [源码运行时与 heredoc](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-source_zh.md) |
| 运行时、制品、缓存和帧协议 | [算子打包](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging_zh.md)、[流契约](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams_zh.md)（英文） |
| 源码、许可证、构建说明和 SBOM | [算子审查材料](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-provenance_zh.md) |
| 执行记录、历史查询和重启语义 | [执行记录](execution-history_zh.md) |
| 插件发现与贡献要求 | [插件体系](plugins_zh.md)（英文） |
| Dataset 后端、文件格式和结构化浏览 | [Dataset provider](dataset-providers_zh.md)、[格式处理器](format-handlers_zh.md)（英文） |
| 过滤、列投影、SQL shell 与优化契约 | [记录查询](records-scans_zh.md)（英文） |
| 数据模型、读取能力和节点配置字段 | [技术参考](reference_zh.md) |

## 可运行示例

- [普通节点间 Job](../examples/multi-node/README_zh.md)和 [Union 跨中心分析](../examples/union/README_zh.md)。
- [Scala 算子](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/scala/README_zh.md)、[Python 算子](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/python/README_zh.md)、[原生可执行算子](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/native/README_zh.md)。
- [插件工程](../examples/plugins/README_zh.md)和 [SQL 数据发布](../examples/sql/README_zh.md)。

除非文档另行指定，命令从项目根目录执行。服务配置中的相对路径以服务进程工作目录为基准。示例证书、主机名、端口及文件路径用于本地演示，部署时按实际环境配置。文档默认英文：`README.md` 及不带语言后缀的主题文件为英文；完整中文翻译使用 `README_zh.md` 及 `*_zh.md`。

[用户访问日志](access-logs_zh.md)：JSON Lines 文件、大小轮转、字段及采集。

[数据集目录](dataset-directory_zh.md)：节点公开描述、Union Web 按节点查看目录和 Shell `datasets`。
[同进程执行实测](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion-benchmark_zh.md)：100 万与 400 万行的策略对比。

[节点 Web 看板](node-web_zh.md)：默认 HTTP 服务、配置开关及 G6 任务 DAG。

官方网站的构建、部署和软件下载发布：[dacp-website](https://github.com/bluejoe2008/dacp-website/blob/main/README_zh.md)。网站仅提供英文内容。

[S3 兼容读取](s3-service_zh.md)：节点启动时公开 dataset bucket 与 Item object，支持 SigV4 认证的只读下载。

Union 对外提供标准 DACP 目录、Item、Job 与 S3 服务；公开短名称、自动放置及管理权限配置见 [Union 代理服务](union-proxy_zh.md)。

[认证与访问控制](security_zh.md)：Flight Token、Union 服务凭据、S3 SigV4、撤销与审计。[结构推导](https://github.com/rdcn-link/xstream-codex/blob/main/docs/structure-inference_zh.md)：算子必选输出推导与工作流校验。

[流传递与逐步物化实测](materialization-benchmark_zh.md)：50 万条气象记录、三轮耗时和实际落盘量。

本地与跨节点流式作业的[效率对比实测](local-vs-union-benchmark_zh.md)。

项目交流材料：[英文会议海报与 A4 PDF](../assets/posters/README_zh.md)。

[在线 Job 设计器](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md)：从浏览器设计单机与跨节点作业，导出 YAML 供 Shell / SDK 提交。

- [算子参数类型及设计器表单](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-parameters_zh.md)

算子仅源码发布：[提交、隔离构建、审核和可信度](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。管理员指定多个分类，开发者选择已注册环境。
