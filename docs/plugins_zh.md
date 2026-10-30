# 编写插件

DACP 通过独立贡献的插件扩展。添加已有扩展点的数据集后端、文件格式或其他实现，只需安装和配置插件，不应修改核心注册表、重新构建服务器或修改客户端。

核心负责公共契约、插件发现与生命周期、Flight 传输、授权、命名空间校验、取消和资源管理。插件负责领域行为、私有配置，以及后端或解析器的第三方依赖。内置实现与外部插件使用相同契约。

## 随发行包提供的实现

内置实现位于 [plugins/](../plugins/README_zh.md)：Dataset 后端、基本格式、Parquet、Excel、归档、静态目录和算子启动器。它们针对独立 API 编译，通过 ServiceLoader 注册；服务器只在运行时和集成测试中引入它们。第三方 JAR 使用同一发现机制。源码目录 `plugins/` 与本地安装目录 `.dacp-plugins/` 不同。

## 已提供的扩展点

| 扩展内容 | 当前机制 | 指南 |
|---|---|---|
| Dataset 目录 | 实现 `DataCatalogProvider` 并通过 Java `ServiceLoader` 注册，或向 `DacpServer` 注入 `DataCatalog` | [动态目录](dataset-catalogs_zh.md)、[嵌入启动](embedded-server_zh.md) |
| Dataset 提供者 | 实现 `DatasetProvider`，通过 Java `ServiceLoader` 注册 | [Dataset 提供者](dataset-providers_zh.md) |
| 文件格式处理器 | 实现 `FormatHandler` 或 `StructuredFileHandler`，通过 Java `ServiceLoader` 注册 | [格式处理器](format-handlers_zh.md) |
| 归档读取器 | 实现 `ContainerReader`，通过 Java `ServiceLoader` 注册 | [归档读取器](format-handlers_zh.md#归档读取器) |
| 数据处理算子 | 发布完整容器、精简 JAR、Python 源码或 wheel，复用运行环境 | [算子打包](operator-packaging_zh.md) |
| 算子启动执行器 | 实现 `OperatorExecutor`，通过 Java `ServiceLoader` 注册 | [执行器插件](operator-packaging_zh.md#执行器插件) |
| 输出物化存储 | 实现 `MaterializationStoreProvider`，通过 Java `ServiceLoader` 注册；核心管理授权与流协议 | [物化与重放](materialization_zh.md) |

Dataset 提供者和格式处理器运行在服务器 JVM 内。管理员将插件及私有依赖 JAR 安装到 `plugin_directory`，重启后加载。它们不在沙箱中执行，也不由客户端上传。算子在独立容器中执行。

逐 Item 对象和游标访问、资源所有权、配置以及 MongoDB/S3 扩展边界见 [Dataset/Item 访问契约](dataset-access_zh.md)。

Dataset 提供者构建逻辑命名空间，实现列表、元数据、计数和流式读取。格式处理器解释内容，算子执行转换。新格式应同时适用于 directory 和 mixed Dataset，无需为格式单独添加 Dataset 提供者。

## 贡献契约

每个插件应提供：

- 唯一标识、注册元数据、安装说明和最小配置示例。
- 对必需设置、类型和未知参数的校验。物理路径、凭据和后端标识保持私有。
- 准确的 schema 和能力声明。原文件字节可直接读取且不修改源；记录数含义明确，不可用时保持未知。
- 有界流处理、背压和取消处理，以及成功、失败和中断时的清理。普通浏览不得触发昂贵的记录扫描。
- 按扩展类型测试发现、配置、命名空间、流、计数、取消、清理和不泄露私有诊断的公共错误。

公开 Item 路径和视图构成客户端契约。后端解析和配置留在服务器端。新的内容表示可能需要修改公共协议和 SDK；为已有表示添加另一种实现不应要求这些修改。

## 插件基础设施

独立的 [dacp-plugin-api](../dacp-plugin-api/README_zh.md) 包含共享契约，不依赖服务器实现。[示例项目](../examples/plugins/README_zh.md) 展示独立编译、服务注册、原生 Dataset 构建和结构化文件子对象访问。`PluginChecks.listing` 提供可复用的元数据契约检查；服务器测试覆盖 Flight 集成、取消和清理。

配置 `plugin_directory`，在注册表初始化前加载插件及私有依赖 JAR。内置与外部实现使用相同接口。Dataset 提供者接收 `DatasetContext`，不依赖服务器的 `Config`。每个处理器自行校验私有 YAML `options`；`format` 和 `schema` 是通用读取设置。每次请求结束关闭访问句柄，服务器关闭时释放提供者资源。插件是可信服务器代码，不支持热加载。

结构化文件实现 `StructuredFileHandler.openStructure`，返回逻辑子对象并直接输出流，无需提取文件。ZIP/TAR/GZIP 使用独立的 `ContainerReader` 暂存条目；注册归档读取器会自动提供对应格式条目。

## 科学数据和未来扩展

科学格式解析插件、科学视图及网络编码见 [xstream-data](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README_zh.md)。

当前公共视图是 `records` 和 `bytes`。数组和图像视图仍需明确共享 schema、元数据和选择语义。不得静默把科学数组展开为记录，也不得虚构记录数。插件基础设施的存在不表示这些表示已经实现。

新的扩展点同样应定义聚焦的公共契约，通过注册发现实现，并避免核心分发中的领域分支。没有具体贡献或用例时不引入抽象。未发布阶段直接改进契约并同步调用方，不累积兼容层。

## 记录扫描优化

Dataset 提供者和格式处理器可通过 `Scan` 优化过滤、投影和限制。所有 records 实现都有有界的服务器端回退；优化必须保留相同的空值、类型语义和全局限制。见 [记录扫描语义和插件契约](records-scans_zh.md)。

## 编写和发布算子

[算子编写指南](operator-authoring_zh.md) 提供完整 Scala/Python 实现、精简 JAR 和 wheel 构建、管理员发布、远程传输及任务提交。公共仓库支持浏览下载；认证发布通过 API 和管理员 CLI 提供。

算子启动插件可实现 `OperatorExecutor.sourceCommand`，用于仅 DEBUG 模式允许的内联源码。默认拒绝源码执行。管理员在 `source_runtimes` 中将运行环境映射到已安装镜像和执行器 ID；协调端和工作端都要求 `mode: DEBUG`。编译在隔离容器中执行。注册、配置和示例见 [源码算子](operator-source_zh.md)。

`command` 执行器将原生 stdin/stdout 程序适配到一对 bytes 输入输出。配置来自本地 `executor_config` 或仓库 `execution.config`，通过 `OperatorProgram.configuration` 传入；程序同时暴露声明的端口表示供插件校验。见 [原生命令注册、配置和契约测试](native-commands_zh.md)。

## 后续自定义能力审查

剩余耦合、优先级和建议接口见 [扩展边界审查](plugin-roadmap_zh.md)。其中的建议与上面已实现的扩展点明确区分。

## 实例环境和执行存储

`PluginEnvironment` 按服务器管理发现和生命周期。通过 `DacpServer.builder().plugins(environment, owned = false)` 注入；应用拥有的环境不会随服务器关闭。插件可覆盖 `initialize(environment)` 和 `close()`。`ExecutionStoreProvider` 在 `META-INF/services/dacp.ExecutionStoreProvider` 注册，配置为 `execution_store: {provider: file, settings: {directory: /var/lib/dacp/executions}}`。存储必须提供原子唯一 `create`、原子替换 `put`、独立副本的 `get`/`records` 及关闭。核心保留状态转换、恢复和稳定的按所有者过滤分页。内置 file 位于 `plugins/dacp-plugin-execution-file`；其他实现不需要修改核心注册表。

## 同进程算子合并

`OperatorExecutor.groupCommand` 是已实现的可选扩展点；JVM SDK 算子和显式提供 `process(ctx)` 的 Python 算子可在同节点、同环境下合并运行。资格、注册契约、示例和限制见 [同进程合并执行](operator-fusion_zh.md)。

S3 兼容读取是复用 DatasetAccess 的核心传输层，不是新的存储插件。Dataset 插件无需独立注册 S3 实现；原生 bytes 直接传递，仅声明 records 的 Item 则通过通用适配层以 UTF-8 JSONL 提供 bytes（不落盘），同样适用于 Flight 和任务输入。见 [S3 服务](s3-service_zh.md)。

显式结构推导通过 `OperatorExecutor.inferenceWorkerCommand` / `sourceInferenceWorkerCommand` 启动隔离 worker，支持按需启动和空闲回收；核心管理生命周期、主体隔离、并发容量、超时和有界元数据通道，执行器负责语言启动与 SDK 适配。外部插件的注册和管道协议见[结构推导](structure-inference_zh.md#执行器插件的-worker-协议)。

## 设计器聊天编排插件

`xstream-designer` 独立提供 `xstream.designer.ChatPlanner` 扩展契约，通过 `META-INF/services/xstream.designer.ChatPlanner` 发现实现。内置命令助手、HTTP 模型适配器和兼容聊天补全服务均使用这一契约；第三方 JAR 无需修改注册表或重建设计器。配置、类路径和 JSON 草案契约见[设计器手册](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-designer/README_zh.md#聊天编排)。插件只产生候选草案，设计器统一校验连接、算子来源及逻辑节点，不能通过该接口执行作业。

第三方算子定义必须包含语言无关的 `parameters` 契约，由 `dacp-operator-sdk/contract` 验证。仓库和设计器读取这些定义时不加载算子代码。注册、发布示例和校验规则见[参数契约](operator-parameters_zh.md)。

算子还必须提供 `icon`；公开 `categories` 由管理员指定，规则和发布示例见[分类与图标](operator-presentation_zh.md)。

[仓库源码分析/构建扩展](../plugins/dacp-plugin-source-analysis/README_zh.md)：`SourceAnalyzer`、`SourceArchive`、`SourceBuildRecipe` 为 `dacp-operator-sdk/contract` 中已实现的 ServiceLoader 契约。

## 类型化数据视图

科学格式解析插件、科学视图及网络编码见 [xstream-data](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README_zh.md)。
