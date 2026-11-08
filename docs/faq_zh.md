# 常见问题

[English](faq.md)

## 1. 一个节点如何加入 Union？

Union 使用节点签发的服务凭据连接配置中的节点。节点不主动注册，也不需要发送邀请。

1. 准备 Union 配置，在 Union 主机上查询稳定身份：

   ```bash
   union-admin identity /etc/dacp/union.yaml
   ```

   记录 `union_id` 和 `scheduler_identity`，保留 Union 状态目录及稳定的证书身份。

2. 在节点主机上签发绑定该 Union 身份的凭据。目录使用节点配置的 `credential_directory`：

   ```bash
   dacp-admin grant-union /protected/node-credentials \
     --subject union-service \
     --union-id UNION_ID \
     --fingerprint SCHEDULER_IDENTITY
   ```

   将大写占位符替换为第一步得到的身份值。只保存返回的 `token` 值，不要保存完整 JSON 作为 Token 文件。通过安全方式传至 Union 主机，限制文件权限，例如执行 `chmod 600`。

3. 在 Union 配置中增加节点：

   ```yaml
   members:
     - url: dacp://node.example.org:18861
       credential_file: /protected/union/node.token
   ```

   确保服务之间网络可达，TLS 配置允许建立连接。节点须启用 Union 执行服务；其 `union` 配置见部署指南。

4. 重启 Union 以应用成员列表，然后诊断连接：

   ```bash
   union-admin members /etc/dacp/union.yaml
   ```

   也可在 Union 看板查看成员健康状态。成员不可用不会阻止 Union 启动，服务会持续轮询重试。

参见 [Union 部署](../dacp-union-server/README_zh.md)和[凭据与角色](security_zh.md)。

## 2. 一个节点可以加入多个 Union 吗？

可以。为每个 Union 单独签发服务凭据，分别绑定其 Union ID 与证书身份；在各 Union 的成员配置中填写同一个节点地址及对应凭据。各 Union 独立维护公开数据集名称与 Job。撤销一个 Union 的凭据不会撤销另一个的凭据。

节点本地 Job 和所有 Union 共享节点执行容量，目前没有按 Union 单独预留资源或保证公平调度。一个节点同时加入两个 Union，不代表这两个 Union 的其他成员自动互信。接纳的 Union 服务凭据授权该 Union 的服务操作，目前没有按 Union 配置的数据集允许列表。

## 3. 目前有哪些权限和角色？

| 角色 | 权限 |
|---|---|
| `reader` | 公开内容读取及本地预签名工具权限 |
| `runner` | Reader 能力、Job 执行、推导及本人 Job 管理 |

数据集发现、Item 列表和 Item 元数据公开访问。内容遵循数据集的公开、不公开或定时公开策略；公开内容可匿名读取，普通角色不能绕过不公开或未到期的限制。执行 Job 需要 runner 权限。Union 服务凭据采用独立的证书绑定信任机制。管理员为账号分配预定义角色，不是将各日志操作名单独配置为角色权限。参见[安全与授权](security_zh.md)。

## 4. 步骤之间和跨节点 Job 有时间限制吗？

目前没有独立的步骤间空闲超时。下游可以等待上游数据，下游缓冲区满时上游也会等待；这些等待会消耗执行时间。

默认执行时限为 1,800 秒。服务通过 `execution_timeout_seconds` 配置，提交的执行策略可以用 `timeout_seconds` 缩短。Union 向子 Job 下发剩余时间，子 Job 同时受节点 Union 执行配置的时限约束。控制请求通常另有 30 秒的截止时间，数据流调用也有截止时间。持续收到数据不会延长调用截止时间。参见[执行策略](materialization_zh.md)。

## 5. 30 秒 deadline 错误表示 Job 执行太久了吗？

不一定。`CallOptions deadline exceeded after 29.99…s` 表示某次远程调用超时，不表示整个 Job 已达到执行时限。应在 Union 和节点运行日志中按业务操作名、请求 ID、Job/执行 ID 关联排查。只调大 `execution_timeout_seconds` 不会改变控制调用的截止时间。

节点接纳启动请求后先返回，再在后台解析算子、准备执行单元。接纳请求不表示初始化成功，须继续查看 Job 状态。已取消、到期或完成的子 Job 不能再次启动。“No step snapshots were recorded” 表示看板没有获取到步骤观测，并不能证明节点从未开始工作。

## 6. 请求日志和完整异常栈在哪里？

节点访问日志采用 JSON Lines，目录由 `access_log.directory` 指定；默认是进程工作目录下的 `.dacp-access/<port>/`。请求结束后异步写入，包含请求 ID、可用的 Job ID、操作名、状态和耗时。记录中的开始时间并不意味着收到请求时会单独写一条日志。

操作名采用 `catalog.list`、`item.describe`、`item.read`、`job.submit`、`job.prepare`、`job.start`、`job.status`、`job.cancel` 等业务名称，与授权权限 `data:read` 等不同。独立的传输认证审计仍记录 RPC 方法。

服务端运行日志记录完整异常栈、原因链及关联标识。Job 创建前失败时没有可用的 Job ID。客户端只收到简明错误，访问日志不包含异常栈。参见[访问日志](access-logs_zh.md)。

## 7. 看板会自动刷新吗？

节点和 Union 看板打开时加载，之后通过 **Refresh** 或 **Reload** 手动更新。切换分页、筛选或展开数据集时，可按需加载相应内容。服务后台的成员轮询不依赖浏览器刷新，仍会继续。CONET 大屏保留自动更新。参见[看板](node-web_zh.md)。

## 8. 如何关闭看板 Web 服务？

**DACP 节点：**在节点启动配置中设置以下内容，然后重启节点：

```yaml
web:
  enabled: false
```

节点看板默认开启，因此删除节点配置中的 `web` 段并不能关闭它。

**Union Server：**从启动配置中删除整个顶层 `web` 段，然后重启 Union。例如，删除以下配置：

```yaml
web:
  host: 127.0.0.1
  port: 19880
```

Union 仅在存在该配置段时启动看板。Union 配置不接受 `web.enabled: false`、`web: false` 或 `web: null`。

关闭看板会关闭对应的 HTTP 监听及只读 HTTP 接口，不会关闭 DACP 数据服务、Job 执行、Union 协调、执行历史或独立配置的 S3 服务。修改后须重启对应节点或 Union 进程才能生效。参见[看板](node-web_zh.md)和 [Union 部署](../dacp-union-server/README_zh.md)。

## 9. DACP 支持哪些常见操作，对应的英文名称和权限是什么？

| 操作 | 英文业务名称 | 访问要求 |
| --- | --- | --- |
| 查询服务能力 | `service.capabilities` | 匿名公开 |
| 列出数据集 | `catalog.list` | 匿名公开 |
| 获取目录快照 | `catalog.describe` | 匿名公开 |
| 列出 Item / 展开 Folder | `item.list` | 匿名公开 |
| 查看 Item 元数据 | `item.describe` | 匿名公开 |
| 统计记录数 | `item.count` | 数据集内容发布策略 |
| 读取 Item 视图 | `item.read` | 数据集内容发布策略 |
| 推导步骤输出 | `step.infer` | `job:execute` |
| 推导 Job 输出 | `job.infer` | `job:execute` |
| 提交 Job | `job.submit` | `job:execute` |
| 重放 Job | `job.replay` | `job:execute` |
| 查询 Job 状态 | `job.status` | `job:manage:self`，并校验归属 |
| 取消 Job | `job.cancel` | `job:manage:self`，并校验归属 |
| 读取 Job 结果 | `job.result.read` | `job:manage:self`，并校验归属 |
| 读取 Job 事件 | `job.events.read` | `job:manage:self`，并校验归属 |
| 列出可用节点 | `node.list` | `job:execute` |
| 查询节点状态 | `node.status` | 可信服务身份及执行计划授权 |
| 查询 Union 成员关系 | `union.membership.get` | 可信服务身份及执行计划授权 |
| 准备子 Job | `job.prepare` | 可信服务身份及执行计划授权 |
| 启动子 Job | `job.start` | 可信服务身份及执行计划授权 |
| 读取步骤输出 | `step.output.read` | 可信服务身份及执行计划授权 |
| 查询步骤物化状态 | `step.materialization.status` | 可信服务身份及执行计划授权 |

目录、列表和元数据始终公开；Item 内容读取及实际扫描遵循数据集发布策略。Job 提交权限不绕过数据集内容限制。S3 列表和对象元数据匿名公开，GET 内容同样遵循数据集策略；提供的签名仍须有效。

过滤、投影、限制和科学切片属于 `item.read` 参数。表中英文名是业务日志名称，不是远程方法名或权限标识。看板提供只读信息，其内网边界须通过监听和防火墙配置；管理命令通过本地 `dacp-admin`、`union-admin` 执行。参见[访问日志](access-logs_zh.md)、[认证与访问控制](security_zh.md)和[数据集发布策略](dataset-publication_zh.md)。

## 10. `/etc/dacp/ca.crt` 是什么？

这是示例中的公开 CA 证书文件路径，用于校验远程服务的 TLS 证书。部署时应替换为实际 CA 证书路径。它不是访问 Token，也不是私钥，本身不会授予数据读取或 Job 执行权限。

所有连接均验证服务器证书和主机名。仅当平台信任服务器证书时才可省略 CA 文件；私有 CA 或自签名 CA 必须配置 `ca`。Designer 连接配置中的路径位于 Designer 服务器上，不是访客电脑上的路径。账号 Token 通过独立的 `token_file` 配置。参见[认证与访问控制](security_zh.md)。

## 11. 如何限制 DACP 服务仅在内网内被访问？

分别配置 DACP、看板和 S3 的监听地址。例如，本机内网地址为 `10.0.82.103`：

```yaml
host: 10.0.82.103
port: 19860
public_host: 10.0.82.103
web:
  host: 10.0.82.103
  port: 19880
s3:
  enabled: true
  host: 10.0.82.103
  port: 21860
```

以上是 DACP Server 配置；Union 使用 `public_url: dacp://10.0.82.103:19860` 声明对外地址，而非 `public_host`。仅本机访问时，各监听地址设置为 `127.0.0.1`；此时不能通过内网 IP 访问。未使用的看板和 S3 应关闭，关闭方式见本页相关问题。

绑定内网地址限制监听网卡，但不会校验访问来源。还需在主机防火墙、云安全组和网络边界中，仅允许指定内网网段访问实际配置的 DACP、看板和 S3 端口，并禁止公网映射、转发及代理暴露；同时检查 IPv6 入站规则。修改后重启服务，分别从允许的内网和禁止的网络验证可达性。`public_host` / `public_url` 只声明地址，不是访问控制规则。网络限制不替代 TLS 服务器身份验证和业务权限校验。
