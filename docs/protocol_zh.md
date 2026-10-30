# DACP 协议

[English](protocol.md) · [文档目录](README_zh.md)

DACP（Data Access and Collaboration Protocol，数据访问与协作协议）定义客户端、数据节点和协作调度节点之间的数据访问与计算协作约定。本项目实现建立在 Apache Arrow Flight 之上：Flight 提供传输机制，DACP 规定数据命名、元数据、读取语义、算子任务、身份授权和跨节点协作行为。DACP 是本项目的协议设计，并非 Apache Arrow 的独立标准。

## 数据与计算模型

服务器通过 Dataset 组织数据，通过 Item 描述可访问的对象。客户端先浏览元数据与服务器公布的能力，再选择读取视图。`bytes` 提供字节内容，`records` 提供具有 Arrow schema 的记录批次。文件格式与读取视图分别描述存储表示和访问能力；客户端不能仅凭文件后缀推断读取方式。组、表和多维变量等科学数据结构应由插件显式描述，不应隐式展开为记录。

作业包含 task；task 的 `id` 用于引用依赖，`title` 用于向用户说明用途。算子通过命名输入、输出端口消费和产生流。输入可以引用数据项或上游输出；结果不自动发布为新的 Dataset。普通节点与 Union 均接受标准 Job 提交；Union 使用自己的公开 URI 和可选逻辑节点 ID，并保留专用管理 API，见 [Union 代理服务](union-proxy_zh.md)。

## 传输分工

| 用途 | 当前传输 |
|---|---|
| Dataset 目录发现 | Flight `ListFlights`，返回公开 Dataset URI 及描述，不额外要求数据授权；目录可由插件动态提供 |
| 浏览、元数据与数据读取 | Arrow Flight；具体消息见技术参考和 protobuf 定义 |
| S3 兼容对象读取 | 独立 HTTPS 端口，SigV4 认证，只读；dataset 映射 bucket、bytes Item 映射 object，见 [S3 服务](s3-service_zh.md) |
| 普通节点作业执行 | Flight `DoExchange`，交换控制消息与结果流 |
| Union 成员凭据验证、状态和作业控制 | Flight `DoAction`，使用 `dacp.union.` 动作前缀及 JSON 消息 |
| Union 最终结果与节点间流读取 | Flight `DoGet`，使用相应 ticket |
| 算子仓库搜索、版本与制品下载 | HTTP/HTTPS |
| Union 管理只读看板 | HTTP；查询运行状态及历史，不能据此提交计算任务 |

Union 的共享传输模块对 JSON 消息实施 1 MiB 上限。具体动作、ticket 和字段以 [Union 协议](union_zh.md)、[技术参考](reference_zh.md)、`proto/dacp.proto` 和 `dacp-union-protocol` 实现为准。算子进程的端口帧协议属于执行器接口，见[算子流契约](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-streams_zh.md)，不等同于客户端 Flight 接口。

## 身份、信任和授权

普通 Flight 客户端使用 TLS 和 Bearer Token。Union 访问成员使用双向 TLS 与节点签发的服务凭据。节点间流由精确执行计划授权，仅信任 CA 不授予访问权限。S3 使用独立的 SigV4 密钥。授权由服务器执行，与 SDK 类型无关。详见[安全方案](security_zh.md)。

## 跨中心协作

一个 Union Server 维护一个 Union。数据节点独立运行；Union 使用节点签发的凭据连接配置中的成员。Union 主动轮询节点状态，并按任务声明的依赖调度步骤。

例如，A 上的任务产生 `oa`，B 基于 `oa` 和本地数据计算 `ob`：Union 调度 A、B，A 到 B 的中间流直接在节点间传输，最终 `ob` 由 Union 转发给客户端。客户端不必直接连接 B；参与节点仍须满足证书验证及相应业务授权。

当前支持显式声明的协作 DAG、可配置物化边界与重放；自动恢复需要执行策略和支持重试的客户端，不提供任意通用流水线优化。状态、流量和任务标题可在看板及执行记录中查看。

## 流与持久化

调用方应逐批消费结果并及时关闭流。取消、超时或下游失败必须释放上游读取与执行资源。已经收到部分数据不意味着整个作业成功，客户端仍须检查流的最终状态。

节点和 Union 持久化执行元数据；Union 记录任务脚本和步骤状态。结果内容通过实时流交付，不作为历史结果文件保存；重启后不能从执行记录恢复结果流。存储位置、查询和重启处理见[执行记录](execution-history_zh.md)。

## 实现与扩展

公开契约、传输、授权、命名空间与资源管理由核心负责。数据后端、格式、结构化浏览和执行器通过插件扩展。增加表示类型或改变线协议时，应同步更新共享契约、服务端、Python/Scala 客户端、测试与双语文档。已实现和计划中的扩展边界见[插件体系](plugins_zh.md)。

### 单机任务的执行策略

Flight `DoExchange` 的 `OperatorRequest` 在任务 YAML 之外接收 `execution_policy_yaml`（YAML 字节）、`retry_aware`（布尔值）和 `replay_from`（原执行 ID）。重放时 `job_yaml` 置空，服务器恢复原任务与策略，校验原提交者及有效物化输出。工作步骤请求不接受这些作业级字段。结果流先发 `execution` 元数据帧公布执行 ID；重试前发送 `attempt_failed`，客户端回调清空暂定结果后接收新一轮 `start/data/end`。`complete` 后仍须确认 Flight 成功结束。断开或关闭流即取消执行。详见[执行策略](materialization_zh.md)。
