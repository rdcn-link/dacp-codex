# 开发者指南

[English](README.md) · [文档目录](../README_zh.md) · [用户指南](../users/README_zh.md)

引擎内置 `read_item@1.0` 只负责传输，后端和格式继续由 Dataset/Item 插件提供；流依赖契约见[作业说明](../jobs_zh.md).

面向实现算子、Dataset provider、格式处理器、执行器、SDK 或服务端的贡献者。连接节点、浏览数据、配置内置 provider 和运行作业请先阅读用户指南。除非另有说明，命令均从项目根目录执行。

目录级扩展和程序内启动见 [DataCatalog](https://github.com/rdcn-link/xstream-codex/blob/main/docs/dataset-catalogs_zh.md)与[嵌入式服务器](../embedded-server_zh.md)。

## 架构与扩展点

核心负责契约、发现与生命周期、传输、授权、命名空间校验、取消和资源管理。插件负责领域行为、私有参数和依赖。新增已有扩展点的实现不应修改核心注册表或客户端；内置与外部插件使用相同的注册契约。

| 模块 | 职责 |
|---|---|
| `xstream-execution-api` | 服务集成、执行及执行器公开契约；数据契约来自 XStream |
| `dacp-server` | Flight 服务、命名空间、协调与资源生命周期 |
| `dacp-client`、`dacp-client-scala` | 客户端 API 与 shell |
| XStream 的 `plugins/xstream-plugin-operator-executors` | 独立维护的容器启动器与原生命令桥接器 |
| 独立项目 `xstream-operator-repository` | 源码提交、隔离构建、审核、公开目录与下载 |
| `proto/dacp.proto` | 共享线协议消息 |

## 编写与发布算子

- [算子编写指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring_zh.md)：Scala/Python 完整源码、构建及上传。
- [原生命令](https://github.com/rdcn-link/xstream-codex/blob/main/docs/native-commands_zh.md)：stdin/stdout 适配、可执行制品与插件配置。
- [打包与执行器](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-packaging_zh.md)：共享依赖、不可变制品、仓库 schema 与注册。
- [流契约](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams_zh.md)：命名端口、帧、背压、取消和调度边界。
- [DEBUG 源码](https://github.com/rdcn-link/dacp-codex/blob/main/docs/operator-source_zh.md)：Python/Scala 编译启动契约；不支持 Java 原始源码。

算子输出为临时流，不注册结果 Dataset，不提供保存结果模式或输出 TTL。bytes、records、array 和 sequence 采用统一 Arrow 流契约，科学维度保持显式表达。通用流水线调度与 fan-out 尚未实现。

## 接入 XStream 数据扩展

Dataset provider、格式与容器处理器、记录查询语义由 XStream 定义。开发与注册请阅读 [XStream 扩展开发文档](http://xstream.conoai.cn/docs.html)。DACP 管理员负责将插件及依赖安装到 `plugin_directory`，并在节点配置中选择 provider；详见[插件接入](../plugins_zh.md)。

## 开发与验证

DACP 优先采用插件机制扩展。新的数据集后端、文件格式和处理算子应通过实现公开扩展契约并安装插件加入，避免修改核心分发逻辑。内置实现与社区实现应使用相同契约，插件负责自己的参数校验与第三方依赖，客户端通过统一路径和能力访问数据。贡献方式、现有机制与科学数据扩展边界见 [插件贡献指南](../plugins_zh.md)。

从项目根目录运行：

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
```

服务启动并设置证书环境变量后，运行 SDK 与安全检查：

```bash
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
```

跨节点容器联调使用 `bash scripts/test-collaboration.sh`，需要 Docker。测试覆盖命名空间、格式读取、归档浏览、SQL 流、schema 校验、授权、流生命周期和任务协作。

协议定义位于 [proto/dacp.proto](../../proto/dacp.proto)，服务端位于 [dacp-server](../../dacp-server)，Python SDK 位于 [dacp-client](../../dacp-client)，Scala SDK 位于 [dacp-client-scala](../../dacp-client-scala)。


## 补充契约与联调验证

```bash
(cd /path/to/xstream-codex/plugins/xstream-plugin-operator-executors && sbt test)
(cd /path/to/xstream-operator-repository && sbt test)
```

stage 服务端、构建所需运行时并准备示例证书后，使用 Docker 权限运行 `.venv/bin/python scripts/test-native-execution.py`，验证原生流、部分失败和取消。`scripts/test-source-submission.py` 需要启动 `examples/source-debug.yaml` 中的 DEBUG 服务，验证 Python/Scala heredoc。前置条件与测试端口见脚本及对应指南。

[技术参考](../reference_zh.md)说明传输及配置。测试、示例与中英文文档须同步。公共契约、调用方与示例须使用一致的名称和字段定义。

## 算子审查材料

支持发布可选的源码包、仓库修订版本、许可证声明与文本、构建说明和 SBOM。公开页面提供下载并标注缺失信息。参见[源码与审查信息](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-provenance_zh.md)。

## Union 开发

[Union 协议](../union_zh.md) 定义证书身份、计算授权和直接流。`dacp-union-protocol` 提供共享传输工具，`dacp-server/UnionWorker` 复用插件数据访问及算子执行器，`dacp-union-server` 负责成员轮询、子任务准备和结果转发。运行 `cd dacp-union-server && sbt test` 验证真实 mTLS 的双节点契约。领域分析算子放在 `examples/union/operators`，不修改核心注册表。

普通 Flight 请求、Union 节点子任务和全局作业共用持久化执行记录。生命周期、原子写入及测试见 [执行记录](../execution-history_zh.md)。

## 中英文文档检查

在仓库根运行 `python3 scripts/check-doc-translations.py`。发布的 Markdown 以 `README.md`/`name.md` 为中文，`README.md`/`name.md` 为英文。完整译文、命令、配置和链接应一起更新。脚本检查配对、基础语言内容和本地文件/章节链接，不能替代翻译语义审查。工具指令与构建产物不计入发布文档。

Union 对外提供标准 DACP 目录、Item、Job 与 S3 服务；公开短名称、自动放置及管理权限配置见 [Union 代理服务](../union-proxy_zh.md)。

算子仅源码发布：[提交、隔离构建、审核和可信度](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。管理员指定多个分类，开发者选择已注册环境。

## XStream 集成测试

引擎、数据和算子 SDK、推导与运行环境工具位于 [xstream-codex](https://github.com/rdcn-link/xstream-codex)，仓库服务及源码审核集成测试位于独立[算子仓库](https://github.com/rdcn-link/xstream-operator-repository)。DACP 构建依赖已发布的 XStream 制品。需要算子示例源码的开发集成测试通过显式 `XSTREAM_SOURCE_ROOT=/path/to/xstream-codex` 选择测试素材；该路径不是生产构建依赖。气象算子示例需要在测试 Python 环境安装 `xstream-operator-sdk/python`。

## 远程执行适配与协议同步

XStream 定义协议无关的执行与数据访问契约。DACP 连接器通过 `ExecutionTargetProvider` 和 `RemoteItemProvider` 提供计算与数据访问。Union 将成员和数据位置转换为公共规划输入，通用步骤排名复用 `GraphPlacement`。Designer 仅编制 Job。

`proto/dacp.proto` 是元数据协议的权威来源。导出连接器快照：`python3 scripts/export-xstream-protocol.py /path/to/dacp.proto`；加 `--check` 检查一致性。该命令用于显式集成验证，不是 XStream 独立构建的前提。
