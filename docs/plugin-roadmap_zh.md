# 用户自定义能力与扩展边界审查

[English](plugin-roadmap.md)

本文基于当前源码的访问、配置、执行、协作和持久化调用链审查。下面先列出已落地的契约，再区分仍未实现的建议；已经可用的扩展点和安装方法见 [plugins/](../plugins/README_zh.md) 与[插件指南](plugins_zh.md)。

判断标准：新增某类用户实现，不需要修改核心注册表、重编译服务器或修改客户端；实现专有配置和依赖由插件拥有；公共协议、授权边界、取消、背压和资源语义仍保持一致。插件本身是受信任的服务器代码，插件化不等于沙箱隔离。

## 已实现的基础契约

- `PluginEnvironment`：每个服务器独立发现和管理插件，支持代码注入及资源所有权，关闭不修改 JVM 全局注册表或线程上下文类加载器。
- `ByteSource`：格式读取接收可重新打开的字节源，可声明范围读取、大小、版本及可选本地路径；本地文件适配、远程顺序读取和范围读取使用同一格式接口。
- `mounted` Dataset 提供者：通过嵌套 Dataset 定义组合任意后端命名空间；文件/JDBC 的 PublishedItem 和 SqlConfig 位于后端插件，核心 sources 仅保存 provider/settings。
- `ExecutionStoreProvider`：核心维护执行状态与恢复，文件插件负责原子写入和单写者锁，节点及 Union 通过 execution_store 选择实现。

注册、配置和资源契约见[插件指南](plugins_zh.md)及[Dataset 提供者](dataset-providers_zh.md)。MongoDB/S3 的具体后端尚未捆绑，不应从这些扩展契约推断其已实现。

## 应优先处理的边界

| 优先级 | 现有耦合与位置 | 建议与实际收益 |
|---|---|---|
| 中 | 核心验证 Token 主体、Union 服务凭据、任务所有权和精确节点间流计划 | 可扩展可发现的机构授权策略。插件必须保留核心认证、凭据撤销和执行计划边界 |
| 中 | [TaskExecutor](../xstream-engine/src/main/scala/dacp/TaskExecutor.scala) 直接创建 Docker 客户端和容器；[OperatorLibrary.resolve](../dacp-operator-inference/src/main/scala/dacp/OperatorLibrary.scala) 也接受 DockerClient | 将制品解析与运行环境分开，再定义完整 `ExecutionBackend` 生命周期。现有 `OperatorExecutor` 继续生成语言/入口启动命令；Docker、未来 Podman/Kubernetes 后端负责运行、取消、资源和清理。拆分不表示允许直接在宿主机执行不受约束的代码 |

当前原生 Dataset Provider 已可直接提供 MongoDB/S3 风格的 Item 和流；格式字节源、通用挂载解决的是**复用格式插件和跨后端组合**，不是接入单一后端的前置条件。`mixed` 目前明确仍是文件/JDBC 的组合。

实例级插件环境明确创建、验证配置、启动、失败回滚、请求级资源、停止与类加载器关闭的所有权。应用注入的共享连接池不应因为一个请求或一台嵌入式服务器关闭而被意外关闭。无需为了这些目标同时实现热加载。

## 可以按需求继续扩展

- **观测事件出口**：任务状态、耗时、流量已在执行和历史逻辑中产生，可抽取 `ExecutionObserver`/`MetricsSink`，接入 OpenTelemetry 或机构审计系统。可选观测器不能意外阻塞结果流；持久审计的可靠性不能依赖一个可丢失的回调。
- **仓库存储**：[RepositoryServer](../dacp-operator-repository/src/main/scala/dacp/repository/RepositoryServer.scala) 和 [Publishing](../dacp-operator-repository/src/main/scala/dacp/repository/Publishing.scala) 直接使用本地目录，可为 manifests/blobs 定义存储接口，让 S3 等作为仓库后台。HTTP 发布与下载协议保持一致，摘要校验和版本不可变规则保持一致。
- **调度策略**：Coordinator 当前要求任务显式指定节点，并内置等待容量、准备和启动逻辑。需要自动选点、数据就近、配额或优先级时再抽取 `PlacementPolicy`/`SchedulingPolicy`；核心继续验证计划、授权和数据流连接。此项不意味着已实现一般 DAG 调度、重试或断点恢复。
- **凭据获取**：当多个后端需要共同使用 Vault、STS 或可刷新的机构凭据时，再引入凭据解析接口。现在后端可以自行解释私有 source 配置，不必为每种认证机制增加核心配置分支。

## 应改进契约，而不是直接开放任意插件

[Producer.fail](../dacp-server/src/main/scala/dacp/Server.scala) 仍识别 `java.sql.SQLException`，并依赖部分异常消息字符串。应改成统一的类型化失败契约，由插件把内部错误转换成安全的公共错误类别，核心统一映射 Flight 状态；不要允许任意插件错误消息直接成为客户端响应。

[Item.views](../dacp-plugin-api/src/main/scala/dacp/Models.scala)、流端口和 Flight 读取目前明确支持 records/bytes。数组、图像、多维变量和 selection 需要先定义跨客户端的描述、schema、能力与选择语义，再让插件实现；不能仅让插件注册任意字符串，也不能将科学结构静默拉平成 records。

[OperatorMain](../dacp-operator-runtime/src/main/scala/dacp/OperatorMain.scala) 根据算子名选择 identity/format_convert，这些是具体示例算子行为。可以将其拆成普通算子 JAR，保持共享运行时通用；现有薄 JAR、Python 与独立入口已经支持用户自定义算子，因此不需要为了每个算子再发明服务器插件接口。FormatConvert 对 CSV/NDJSON/Parquet 的业务限制也不能直接认定为核心协议限制。

Flight、URI/票据解析、命名空间安全验证、基本流协议、取消、背压和资源上限继续属于核心职责。优先抽取具体用户场景需要替换的策略与实现，不为所有分支建立扩展点。

后续优先考虑有实际需求的授权策略、执行后端和调度策略。每次提供一个真实实现、注册和配置示例以及发现、错误、取消、资源所有权的契约测试。
