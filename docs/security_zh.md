# 认证与访问控制

本文说明 DACP 已实现的认证方式、授权边界、凭据管理、撤销行为和审计。下文提供节点、Union、S3 与算子仓库的配置和操作说明。

## 服务边界

| 服务 | 认证与访问范围 |
| --- | --- |
| 节点与 Union 的普通 Flight 服务 | TLS 与 Bearer Token；服务端执行授权 |
| Union 到成员节点的协作调用 | 双向 TLS 与节点签发的 Union 服务凭据 |
| 节点间任务数据流 | 双向 TLS 与精确绑定任务、端口和接收方的执行计划 |
| S3 服务 | 独立的访问密钥；HTTPS、SigV4 请求头认证、预签名 GET |
| 节点、Union 运维看板 | 内网公开只读，不提供登录或访问控制 |
| 算子仓库 | 已批准算子与环境公开；提交源码使用个人 Token，管理员登录审核 |

授权发生在服务端。普通客户端 SDK 与协作 SDK 分开提供，但共享 Flight、证书和流协议实现；选择某个 SDK 本身不会获得权限。

## 普通客户端

客户端使用 Token，不要求每个客户端都申请证书。客户端执行标准 TLS CA 和主机名校验，不增加人工证书固定或额外身份确认步骤。

Token 归属于稳定的主体，一个主体可以持有多个 Token。任务、状态、结果、取消和重放按任务所有者授权。更新或轮换 Token 不应改变主体及其历史任务归属。

管理员通过服务器本地凭据工具创建、列出、撤销和轮换凭据。Token 至少使用 256 位随机数，只在创建时输出一次，服务端保存摘要。凭据变更热加载；服务端在每次 RPC 开始时从内存检查凭据。普通 Token 到期或撤销后，已经接纳的 RPC 和结果流继续执行；同一连接上的后续 RPC 必须重新通过检查。

## Union 与成员节点

Union 启动时从配置读取唯一 Union 的成员列表，不提供远程成员管理接口。节点管理员签发绑定 Union 身份与客户端证书指纹的服务凭据；Union 配置成员地址与凭据文件即可连接，不采用邀请、审批流程或额外算子白名单。

节点接受该凭据，即信任 Union 的协作操作。节点仍可独立服务其允许的其他客户端。Union 保持稳定身份，并记录成员身份；不可用成员不会阻止 Union 启动，后续通过轮询重试。

每次协作调用检查服务凭据。与普通 Token 不同，Union 服务凭据撤销或到期时，应停止相应的运行中协作任务与流。

同一 Union 的成员之间无需额外相互授权，但不能仅凭同一 CA 签发的证书任意访问对方数据。执行计划必须约束执行实例、任务、输出端口及接收方。Union 的服务秘密不得转发给下游节点。

## S3

S3 与 Flight 是独立服务，不要求统一用户或凭据体系。S3 不提供匿名读取，列表、元数据与内容请求均需认证。

支持 SigV4 请求头认证，以及用于临时分享对象的预签名 GET URL。密钥撤销后，后续请求（包括新的 Range 请求）被拒绝；已接纳的下载继续。预签名 URL 是临时访问凭据，应避免写入日志或公开传播。

## 看板与算子仓库

看板按内网只读服务部署，网络暴露范围由部署环境限制。此设计不提供公网登录保护。

已批准算子的浏览和下载公开。开发者使用个人提交 Token 通过接口提交源码，私有源码与待审产物不公开。管理员单独登录，管理 Token 和环境，查看扫描/构建结果，指定多个分类与公开的最终可信度，再批准发布。版本不可覆盖。详细扫描报告保持私有，分数是审核评价而非安全保证。参见[源码提交与审核](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。

## 审计与凭据保管

访问日志应记录主体、操作、资源、结果、耗时、字节数及关联任务；凭据和成员生命周期事件也应记录。不得记录原始 Token、私钥、S3 密钥或带签名的完整 URL。

凭据文件应限制为运行账户可读写。普通 Token 保存摘要；S3 签名验证需要可恢复的密钥材料，因此不能把所有凭据都描述为仅保存摘要。

## 实现与验证状态

已实现本地凭据管理与热加载、普通 Flight Token 认证、Union 服务凭据与证书绑定、任务所有者隔离、节点间精确计划授权、独立协作 SDK、S3 SigV4 和预签名 GET。自动化测试覆盖普通 Token 撤销后已有流继续、后续调用拒绝，服务凭据撤销后运行中任务与流终止，Token 轮换后保留任务归属，以及拒绝请求的审计。真实服务联调验证了三个成员节点的认证、Union 协作调用和 boto3 的签名列表、元数据、内容、Range 及预签名 GET。

相关说明：[访问日志](access-logs_zh.md)、[数据访问](dataset-access_zh.md)、[算子仓库](https://github.com/bluejoe2008/xstream-operator-repository/blob/main/README_zh.md)。普通客户端示例应提供 Token 文件；Union 成员配置应提供节点签发的服务凭据文件。

## 本地凭据操作

节点配置 `credential_directory: /protected/node-credentials`，Union 配置自己的 `credential_directory`。未指定时，节点使用进程工作目录下 `.dacp-security/端口`，Union 使用 `state_directory/credentials`。目录权限为 700，凭据文件为 600。

以下命令在对应模块目录运行；Union 的程序名替换为 `dacp-union-server`，使用它自己的凭据目录：

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials create --subject alice --expires-in 86400
target/universal/stage/bin/dacp-server credentials /protected/node-credentials list
target/universal/stage/bin/dacp-server credentials /protected/node-credentials rotate CREDENTIAL_ID
target/universal/stage/bin/dacp-server credentials /protected/node-credentials revoke CREDENTIAL_ID
```

创建或轮换的 JSON 响应含一次性 `token`；将其值保存为仅运行账户可读的文本文件。列表不返回秘密。Python SDK 通过 `token=` 或 `DACP_TOKEN_FILE` 使用，Scala SDK 通过 `TlsConfig(tokenFile=...)`，Shell 使用 `--token-file`。CA 通过 `--ca`、SDK 参数或 `DACP_CA` 提供。不要将整个 JSON 响应当作 Token 文件。

Union 先执行 `dacp-union-server identity CONFIG` 获得稳定 `union_id` 和 `scheduler_identity`。然后在每个成员本地签发：

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials create --subject union-service --kind union --union-id UNION_ID --fingerprint SCHEDULER_IDENTITY
```

将返回的 `token` 放入 Union 配置的该成员 `credential_file`。文件更换后，后续调用读取新值。配置中的成员如果不可达、凭据无效或凭据文件暂不可读，会显示不可用，后续轮询重试，不阻止 Union 启动。

S3 密钥使用同一管理工具的 `--kind s3` 创建，响应中的 `id` 为 Access Key ID，`secret_key` 为 Secret Access Key；它不能充当 Flight Token。预签名命令和 boto3 示例见 [S3 服务](s3-service_zh.md)。

认证层另生成 `authentication-audit.jsonl`，记录 RPC 方法、主体、状态与耗时，含认证和授权拒绝；`credential-audit.jsonl` 记录凭据管理事件。两者超过 1 MiB 时轮换为各自的 `.previous.jsonl`，业务数据量与资源信息由节点访问日志和任务历史补充。
