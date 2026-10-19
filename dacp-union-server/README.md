# DACP Union 调度服务器

[English](README_en.md)

`dacp-union-server` 是独立 Scala 模块。一个进程维护一个持久化 Union，邀请已经独立运行的 DACP 节点，并主动轮询状态和容量。节点本地批准后，X 才能安排计算。节点不需要向 X 注册或发送心跳。

客户端 C 通过 Arrow Flight mTLS 向 X 提交 Job，订阅状态事件并增量读取最终输出。中间流由 B 直接从 A 拉取；最终流经 X 返回 C。不会建立输出 Dataset，也不保存输出文件。

## 构建和运行

需要 JDK 17、sbt 和节点已有的算子运行环境。从本模块目录执行：

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

配置示例，路径相对进程工作目录：

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
client_identities: ["<客户端叶证书 DER 的 SHA-256，小写十六进制 64 位>"]
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
```

所有端点要求客户端证书。TLS 校验证书链和主机名；业务身份为 TLS 验证后的证书指纹。私钥采用未加密 PKCS#8 PEM。X 和节点使用的证书需要同时允许 `serverAuth` 与 `clientAuth`，客户端 C 的证书需要 `clientAuth`；Flight 主机名必须匹配证书 SAN。`client_identities` 是精确白名单，不是“同一 CA 就可以提交”。Job 只对提交它的身份可见。节点间连接额外固定受邀节点证书指纹。更换证书需要重新邀请、批准；X 更换证书时使用新的状态目录。

## 成员管理

以下是 X 本地管理员命令；它们使用相同配置和状态目录，可以在服务运行时执行：

```bash
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server remove ../.union-demo/x.yaml NODE_ID
```

`invite` 在节点创建待批准邀请，返回节点证书指纹和数据服务地址。`members` 主动查询审批和运行状态；网络失败显示 `unknown`，不自动删除节点。`remove` 先请求节点撤销成员授权，再删除 X 本地记录；网络失败会保留记录并报错。移除或撤销取消受影响的运行任务。每个节点至多保留 128 条邀请。

DACP 节点 YAML 增加以下配置，沿用它自己的 TLS 配置：

```yaml
union:
  state_directory: ../.union-demo/a
  execution_timeout_seconds: 120
  retention_seconds: 300
```

节点管理员从 `dacp-server` 目录执行：

```bash
target/universal/stage/bin/dacp-server union list ../.union-demo/a
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union reject ../.union-demo/a UNION_ID
target/universal/stage/bin/dacp-server union leave ../.union-demo/a UNION_ID
```

批准文件必须完整提供：

```yaml
scheduler_identity: "<X 的证书指纹>"
input_prefixes: [dacp://localhost:18861/data]
operators: [quantity_sum@1.0]
max_concurrent_tasks: 4
expires_at_ms: 4102444800000
```

审批绑定 Union ID 和 X 的真实证书身份；限制计算输入前缀、精确算子版本、并发数及有效期。计算权限不会修改普通 Flight 下载权限。审批仅允许待批准邀请；拒绝或撤销后可以再次邀请。状态文件采用文件锁和原子替换，管理员 CLI 与服务可并发运行。

## 作业和客户端

[完整双节点示例](../examples/union/README.md) 包含 A 汇总销量、B 融合本地价格的算子、配置、审批和客户端。任务必须显式指定 `node`、版本化算子及最终 `outputs`；输入为所在节点的 Item 或上游 Task/Port。

Python：

```python
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('examples/union/job.yaml').read_text()) as execution:
        print(execution.job_id)
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

`execution.status()` 查询状态；`execution.cancel()` 取消整张作业。`execution.events(after=0)` 返回独立 Flight 事件迭代器，可在另一个线程运行。事件序号单调递增，保留最近 128 条且总计不超过 4 MiB；游标过旧返回 `gap` 及当前快照。提早结束迭代时显式关闭迭代器；退出执行上下文会取消尚未完成的作业并关闭连接。只关闭状态订阅不取消执行。结果是一次性流，重复领取返回 `ALREADY_EXISTS`。

Scala 客户端的 `dacp.client.Union` 提供同样的 `submit`、`status`、`cancel`、`events`、`results` 接口。通过 `Using.resource` 关闭执行和迭代器：

```scala
val union = new dacp.client.Union("dacp://localhost:19860",
  java.nio.file.Path.of("examples/certs/client.crt"),
  java.nio.file.Path.of("examples/certs/client.key"),
  java.nio.file.Path.of("examples/certs/ca.crt"))
scala.util.Using.resource(union.submit(java.nio.file.Files.readString(
  java.nio.file.Path.of("examples/union/job.yaml")))) { execution =>
  scala.util.Using.resource(execution.results())(_.foreach(println))
}
union.close()
```

## 执行语义与限制

X 校验 DAG 并按显式节点分组。容量不足时状态为 `waiting_resources`；准备所有节点后才启动。若准备期间容量发生竞争，拒绝作业并清理已准备的节点，不自动重试。Union 与普通 Flight 任务共享节点的执行额度。每个节点的所有子任务需要能同时预留；作业合计至多 1024 个输入与连接输出端口，每个任务至多 32 个输入和 32 个连接输出，任务数上限 64，X 活跃作业上限 64，节点和 X 各保留至多 4096 个执行记录。

状态为 `queued`、`waiting_resources`、`preparing`、`running`、`succeeded`、`failed`、`cancelled` 或 `expired`。数据通道使用容量为 2 的帧队列和 Flight 背压。消费节点检查上游的成功 `complete`，不能只看数据 `end`。断开最终结果连接、取消、失败、授权撤销或超时会清理全局作业。客户端尚未领取结果时也受执行超时限制。

状态快照包含各节点 Task 状态与输入、输出端口累计 `payload_bytes`、`remote_payload_bytes`、`last_activity_ms`，records 流另有 `records`。只统计负载，不含 Flight/gRPC/TLS 开销；发送和接收分别计数。状态通道独立于输出队列。

成员关系和执行记录持久化；运行会话及结果仅在内存中。进程重启不恢复作业；X 意外退出后，节点按自己的超时清理。无内嵌源码提交、fan-out、自动重试、检查点、结果重放或自动选址优化。原有 Flight `Job.run` 保留自己的执行路径；Union 使用独立接口。X 的 Flight 执行线程数为 128，部署时应给长连接留出控制请求余量，并按资源设置并发额度。

## 开发接口

所有 Action 类型加前缀 `dacp.union.`，请求与唯一响应均为不超过 1 MiB 的 JSON。地址使用 `dacp://host:port`，节点无需单独的 Union 端口。operator repository 保留 HTTP。

| 接口 | 类型 | JSON 请求或 Ticket |
| --- | --- | --- |
| 提交 | `DoAction submit` | `{"job_yaml":"..."}` |
| 状态 / 取消 | `DoAction job_status` / `cancel_job` | `{"job_id":"ID"}` |
| 成员状态 | `DoAction members` | `{}` |
| 结果 | `DoGet` | `{"type":"dacp.union.results","job_id":"ID"}` |
| 事件 | `DoGet` | `{"type":"dacp.union.events","job_id":"ID","after":0}` |

节点 `DoAction` 提供 `invite`、`membership`、`withdraw`、`node_status`（`union_id`），`prepare`（`union_id`、`execution_id`、`tasks`），`start`、`execution_status`、`cancel_execution`（`execution_id`）。节点输出 Ticket 为 `{"type":"dacp.union.output","execution_id":"ID","task":"ta","port":"result"}`。

结果和中间流的 Arrow schema 为 `data: binary`，每批一行，值为完整 OperatorFrame；事件 schema 为 `event: binary`，每批一行 JSON 事件。成功结果包含 `start/data/end/complete`，还须正常结束 Flight 流；错误使用 Flight 状态。旧 HTTP 接口已移除。

节点协议、mTLS 和 JSON 工具位于 `dacp-union-protocol`；节点执行器位于 `dacp-server` 的 `UnionWorker`，通过现有 `DatasetAccess`、`TaskExecutor` 和算子协议工作，领域分析逻辑放在示例算子中。[协议约定](../docs/union.md) 与 `UnionSpec` 描述授权、直接传输、容量等待、失败和取消的契约测试。

## HTTP 运行看板

在 U 的配置中增加 `web`，随 `serve` 一起启动网站；不配置时不监听 HTTP：

```yaml
web:
  host: 127.0.0.1
  port: 19880
```

浏览器访问 `http://127.0.0.1:19880/`。双节点示例的 `prepare.py` 已生成此配置。网站每 2 秒刷新，展示成员审批、健康与观测时间、节点容量和 Union 额度、可用执行器、作业筛选、任务依赖、运行时长、端口字节和记录计数。点击作业可查看详情；速率由相邻节点观测计数计算，过期观测会明确标记。列表显示最近 100 个作业，统计覆盖当前保留的全部作业。已内存会话沿用 `retention_seconds`；执行元数据永久保留，可在“执行历史”中分页查看，重启后仍可查询。

这是公开只读视图，可查看所有提交者的运行元数据，包括节点、算子及输入 Item URI。网站不读取结果内容，不领取结果流，不提供邀请、审批、提交或取消操作。节点间通信、客户端提交及数据流仍使用 Flight；operator repository 继续使用 HTTP。

看板公开只读访问，无需令牌。要允许远程浏览器访问：

```yaml
web:
  host: 0.0.0.0
  port: 19880
```

HTTP 只读接口：`GET /api/overview`、`GET /api/jobs/ID`、`GET /api/history?offset=0&limit=100`。页面和接口公开展示节点状态、任务脚本、DAG 与执行历史。所有状态响应禁止缓存，不开放跨域。任务提交、计算与数据访问仍需 Flight 证书及授权。关闭 U 同时释放 HTTP 监听端口。

所有全局作业默认保存执行记录，节点同时记录本地子任务。目录、查询命令、历史接口和中断语义见 [执行记录](../docs/execution-history.md)。
