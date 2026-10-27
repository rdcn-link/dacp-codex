# DACP Union 服务

[English](README_en.md)

Scala 编写的协调与代理服务，一个进程维护一个 Union。成员节点独立运行；Union 读取成员配置，轮询健康与资源状态并调度流式作业。成员无需注册或发送心跳。

Union 提供标准 DACP 数据目录、Item 访问与 Job 执行，公开数据集名称隐藏成员实际地址。中间任务流直接在成员间传输，最终结果经 Union 返回。内网 HTTP 看板无需认证，展示成员、流量、执行策略、尝试记录、DAG 与持久执行历史。

## 构建与配置

需要 JDK 17 和 sbt，在本模块执行：

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

配置路径相对于进程工作目录：

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
credential_directory: ../.union-demo/x/credentials
members:
  - url: dacp://localhost:18861
    credential_file: ../.union-demo/a-union.token
  - url: dacp://localhost:18862
    credential_file: ../.union-demo/b-union.token
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
web: {host: 127.0.0.1, port: 19880}
s3: {enabled: true, host: 127.0.0.1, port: 21860}
```

## 认证与成员关系

按[认证与访问控制](../docs/security.md)在本地签发凭据。每个成员签发绑定稳定 Union ID 和证书指纹的服务凭据，将秘密保存到该成员对应的 `credential_file`。成员仅通过配置文件管理，没有远程邀请、审批或成员管理 API。成员不可用不阻止启动，后续轮询重试；凭据撤销会终止运行中的协作。成员证书变更时，需要本地审查 `state_directory/member-identities` 中保存的身份固定记录，必要时重新签发绑定凭据。保留 Union 状态目录以维持身份。

普通客户端使用 TLS 与 Bearer Token，无需客户端证书。本地创建示例：

```bash
target/universal/stage/bin/dacp-union-server credentials ../.union-demo/x/credentials create --subject alice
```

将返回的 Token 保存到受保护文件，通过 `dacp-shell dacp://localhost:19860 --ca ../examples/certs/ca.crt --token-file /protected/alice.token` 使用。见[客户端示例](../docs/union-proxy.md)。任务使用公开命名空间或逻辑节点名称；状态、取消、重放、结果与事件流检查所有者。

## 执行与接口

Job 通过 `inputs` 连接步骤，不支持控制依赖，允许无输出算子。每个算子必须实现[结构推导](../docs/structure-inference.md)。[执行策略](../docs/materialization.md)控制物化、重放、重试次数与总超时。自动重试要求客户端丢弃失败尝试的部分结果，不强制最终结果物化。流使用有界队列与背压；重启中断的任务不会自动恢复，但已提交的物化结果可重放。

前缀 `dacp.union.` 的 Flight Actions 包括 `submit`、`replay_job`、`job_status` 和 `cancel_job`。结果与事件 Ticket 携带 `job_id`，事件还携带 `after`。普通目录使用 `dacp.catalog`，逻辑节点使用 `dacp.nodes`。请求大小上限为 1 MiB。HTTP 看板是内网只读接口，不管理凭据。S3 使用独立 SigV4 认证，见 [S3](../docs/s3-service.md)。

每个 Job 支持 64 步及 1024 个连接端口，每步最多 32 个输入和 32 个连接输出。Union 最多 64 个活动任务及 4096 个内存会话。事件保留最多 128 条、4 MiB，旧游标收到 gap 通知。备份配置、凭据、Union 身份、成员证书固定记录、命名空间路由、执行历史和物化存储；凭据应与公开数据分别保护。

## 开发

普通 SDK 与协作 SDK 通过 `dacp-union-protocol` 共享传输、证书和流实现，服务端授权不依赖 SDK 类型。数据代理使用插件接口，核心不包含后端专用分支。在本模块运行 `sbt test`，在仓库根目录运行 `scripts/test-union-dashboard.py` 验证网页。
