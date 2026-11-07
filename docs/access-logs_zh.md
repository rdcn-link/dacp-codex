# 用户访问日志

DACP 节点默认将完成的 Flight 请求写入 JSON Lines 文件，方便采集系统按行读取。日志与任务历史独立，不保存流内容。默认目录是服务器工作目录下的 `.dacp-access/<port>/`，单文件上限为 64 MiB。

```yaml
access_log:
  directory: /var/log/dacp/access
  max_bytes: 67108864
```

`max_bytes` 必须为正整数。下一条记录会使当前非空文件超过上限时，先切换新文件再写入，保证一条 JSON 不跨文件。单条记录超过上限时独占一个文件。文件名为 `access-<时间戳>-<UUID>.jsonl`，重启不覆盖旧文件。旧文件保留，不自动删除；采集和保留策略由部署环境管理。请求线程将记录放入最多 1024 条的有界队列后返回，由独立写入线程串行追加；不在每个请求结束时等待磁盘同步。后台每一秒或累计 256 条记录时同步，滚动和正常关闭前也同步。队列满时施加背压，不静默丢弃记录。磁盘写入失败会记录服务端错误，后续追加及关闭会报告失败。异常退出可能丢失尚未写入或同步的记录；访问日志不是逐请求事务提交记录。

记录包含请求 ID、开始/结束时间、经 TLS 验证的身份摘要、操作、逻辑 Dataset/Item/视图（适用时）、状态、耗时、已发送批次行数和字节数。bytes 视图统计原始二进制单元载荷，records 的字节数是 Arrow 向量缓冲大小之和，不等于网络线速流量；任务交换统计外层帧。未开始流的请求流量为零。

状态包括 `completed`、`failed`、`denied` 和 `cancelled`。列表、元数据、数据流、任务交换和 Union action 都记录。TLS 握手失败发生在 Flight 请求进入之前，应由传输层或代理日志采集。记录不包含 ticket 原文、算子源码、参数、数据内容、凭据或后端物理地址，也不记录原始异常消息。

配置目录需要可写。服务器启动初始化日志目录，正常关闭时等待队列写完并同步，再释放文件句柄。查看任务状态和执行事件应使用[任务历史](execution-history_zh.md)，不能把访问完成等同于业务作业成功。

## 业务操作名称

`operation` 使用业务名称，而非底层传输方法名。访问日志仍在请求结束时写入；开始时间不表示接收时已写出记录。独立的传输安全审计 `authentication-audit.jsonl` 仍记录 RPC 方法。

| 操作 | 含义 |
| --- | --- |
| `catalog.list` | 列出数据集 |
| `catalog.describe` | 目录快照 |
| `item.list` | 列出 Item |
| `item.describe` | Item 元数据 |
| `item.count` | 记录数 |
| `item.read` | 读取 Item 视图 |
| `node.list` | 列出可用节点 |
| `job.replay` | 重放 Job |
| `job.events.read` | 读取 Job 事件 |
| `job.result.read` | 读取 Job 结果 |
| `job.submit` | 提交并执行 |
| `step.infer` | 推导步骤输出 |
| `job.infer` | 推导 Job 输出 |
| `service.capabilities` | 服务能力 |
| `union.membership.get` | Union 成员关系 |
| `node.status` | 节点资源与容量 |
| `job.prepare` | 准备子 Job |
| `job.start` | 启动子 Job |
| `job.status` | 执行状态 |
| `job.cancel` | 取消执行 |
| `step.materialization.status` | 物化输出状态 |
| `step.output.read` | 读取跨节点步骤输出 |
| `request.unsupported` | 不支持的控制请求 |

## 异常诊断

服务端运行日志记录完整异常栈及原因链，并携带业务 `operation`、`request_id` 和已知的 `job_id`。任务创建前失败时，Job ID 标记为不可用。后台任务使用关联执行标识追踪；跨节点子 Job 的执行标识可与 Union 的执行历史关联。访问日志只保留结果和关联标识，不存异常栈、请求正文或凭据。Job 执行错误向客户端返回简明说明及可用的请求 ID。运行日志仅供运维人员访问，其中异常栈可能含内部文件路径及第三方库错误细节。
