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
| `reader` | `data:read` |
| `runner` | `data:read`、`job:execute`、`job:manage:self` |

数据集发现、Item 列表和 Item 元数据公开访问。读取内容需要读取权限，执行 Job 需要执行权限。Union 服务凭据采用独立的证书绑定信任机制。管理员为账号分配预定义角色，不是将各日志操作名单独配置为角色权限。参见[安全与授权](security_zh.md)。

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
