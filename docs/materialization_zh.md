# 提交执行策略与请求重放

此页说明如何向 DACP 节点或 Union 提交策略、处理失败尝试，并请求重放。策略字段、物化提交规则及存储插件契约见 [XStream 执行策略参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/materialization_zh.md)。默认使用直接流传输。

## 管理员：启用节点存储

在需要物化或重放的 DACP 节点配置中添加：

```yaml
union:
  state_directory: .union/a
  execution_timeout_seconds: 1800
  retention_seconds: 300
  materialization_store:
    provider: file
    settings:
      directory: .materialized/a
      max_bytes: 1073741824
      max_task_bytes: 268435456
      retention_seconds: 86400
```

`directory` 是节点私有路径，不能由任务提交者指定。文件存储插件随标准发行包提供，位于 `plugins/xstream-plugin-execution-file`。省略 `materialization_store` 时，该节点拒绝需要物化或重放的任务。Union 先 prepare 全部节点，再启动任何步骤，因此不支持存储会导致准备失败，而非静默降级。

## 随 Job 提交策略

将策略保存为 `execution-policy.yaml`，通过 Shell 的 `submit workflow.yaml --policy execution-policy.yaml` 或 SDK 提交。客户端传送策略内容，服务器无需读取客户端文件。

Python：

```python
import yaml
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('workflow.yaml').read_text(),
                      execution_policy=yaml.safe_load(Path('execution-policy.yaml').read_text())) as execution:
        job_id = execution.job_id  # 保存，用于之后重放
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

Scala：

```scala
val policy = dacp.union.YamlDocument.parse(java.nio.file.Files.readString(
  java.nio.file.Path.of("execution-policy.yaml")))
scala.util.Using.resource(union.submit(workflowYaml, policy)) { execution =>
  println(execution.id)
  scala.util.Using.resource(execution.results())(_.foreach(println))
}
```

状态与执行历史分别记录 `execution_policy`（提交值）和 `effective_execution_policy`（应用默认值与覆盖后的已连接端口策略）。节点状态的 `materialized_tasks` 记录源执行及是否提交；新执行历史的 `replay_of` 和 `materialized_sources` 关联重放来源。启用物化的子 Job 不进行同进程步骤合并，以保留明确的提交边界。

## 什么时候重放

仅在客户端**显式请求**时重放。原执行必须已经结束：成功、失败、取消、超时或重启后标记为中断。运行中的工作流不支持重放；仅当显式设置正数 `max_retries` 时才自动重试。

```python
with union.replay(job_id, steps=['ta']) as replay:
    print(replay.job_id)  # 新的执行 ID
    for event in replay.results():
        if event['type'] == 'data':
            print(event.get('batch', event['data']))
```

也可调用 `execution.replay(steps=['ta'])`。Scala 对应 `union.replay(jobId, Some(Seq("ta")))` 或 `execution.replay(Some(Seq("ta")))`。

- **下游失败**：复用 `ta` 的已提交输出，不重新执行 `ta` 算子；仍需执行的下游重新运行。客户端需要重新消费结果流。
- **重新获取成功结果**：若最终输出步骤的全部已连接输出均已物化，可直接重放这些步骤。无须读取它们的原始输入或执行其上游；被剪枝步骤记录为 `skipped`。
- 省略 `steps` 时，先检查策略中所有输出均要求物化的步骤，选择当前已有完整、未过期提交的步骤，再按最终输出裁剪所需工作流。指定 `steps` 时，每个步骤都必须可用，否则拒绝。至少需要一个可重放步骤。
- 只有某步骤**全部已连接输出**均物化时，才能跳过该算子；仅物化部分输出仍可保存，但不能用于跳过这个多输出步骤。多端口重放并行读取，保留背压，避免端口顺序导致互相等待。
- 重放使用原任务文件与原策略，不能借此修改算子、参数或输入。节点会再次核对定义与存储清单。客户端必须仍为原提交者，Union 仍须获节点批准，算子及输入授权也会重新校验。
- 原执行元数据及节点存储都保留时，Union/节点重启后仍可新建重放执行。它不是自动恢复旧进程或执行计划，也不是按字节游标续读。

结果过期、未完整提交、损坏、节点离线或授权失效会报错，不静默当成重放成功。未选中复用的步骤可能重新读取当前原始数据；本功能不保证跨节点数据快照一致性，也不提供有副作用算子的 exactly-once 执行。

## 自动重试次数与总超时

重试次数和总超时使用 XStream 策略字段；服务器会实施自己的超时上限，Union 成员还可能设置更小的本地上限。 Union 在成员准备算子及运行环境期间暂停执行预算；准备阶段采用独立的 `preparation_timeout_seconds` 上限，详见[环境就绪与执行计时](union_zh.md#环境就绪与执行计时)。

```yaml
max_retries: 2
timeout_seconds: 120
materialization:
  allowed: false
```

自动重试允许全部直接流传递，也允许全局禁止物化。结果在执行过程中增量发送，**一次尝试的结果在成功结束前都是暂定结果**。重试前，服务端发送 `attempt_failed` 通知；Python 调用方必须提供 `on_retry` 回调，Scala 提供 `Some(callback)`，先丢弃上一轮部分结果，再继续接收下一轮。未声明支持重试通知的客户端不能领取开启重试的结果。最终失败、超时或取消时，调用方也应丢弃当前暂定结果；收到 `complete` 且 Flight 正常结束才可确认成功。

```python
pending = bytearray()  # 仅为小结果示例；大结果应使用可回滚的接收器

def reset_attempt(event):
    pending.clear()
    print("Retry:", event["retry_count"])

try:
    with execution.results(on_retry=reset_attempt) as results:
        for event in results:
            if event["type"] == "data":
                pending.extend(event["data"])
except BaseException:
    pending.clear()
    raise
```

每次重试使用独立 `attempt_id`，保留同一个公开执行 ID。已提交、端口完整且可用的物化步骤输出优先复用，其余必要步骤重新执行；只保存部分输出端口的步骤仍需重新执行。失败尝试的工作线程和流先停止，再通知客户端并开始下一轮。显式取消、授权或参数错误、总超时不重试；Union 的准备失败也不重试。算子副作用可能重复发生，重试不保证 exactly-once。显式重放创建新的执行，沿用策略并重新计算重试预算。

## 单机多步骤任务

DACP Server 的单机任务采用同样的策略 YAML、流通知和存储插件，无需配置 Union。管理员在服务器配置顶层增加：

```yaml
execution_timeout_seconds: 1800
materialization_store:
  provider: file
  settings:
    directory: .materialized/local
    max_bytes: 1073741824
    max_task_bytes: 268435456
    retention_seconds: 86400
```

这是单机执行使用的存储设置；`union.materialization_store` 用于该节点参与 Union 的子任务。两个设置可以引用同一个存储定义，由服务器统一管理生命周期。未启用存储时仍可执行纯流任务与自动重试，但不能物化或重放。物化提交边界使用独立步骤执行；显式策略的单机任务不进行同进程合并。跨节点调度策略提交到 Union。

```python
from dacp import Job

policy = {
    "max_retries": 2,
    "timeout_seconds": 120,
    "materialization": {"default": "materialize"}
}
# script 中用 outputs 显式声明最终输出；输入输出格式与普通任务相同。
with Job.run("dacp://localhost:8815", script,
             execution_policy=policy, on_retry=reset_attempt) as results:
    for event in results:
        if event["type"] == "data":
            pending.extend(event["data"])
    execution_id = results.execution_id

# 原执行结束后，复用仍有效的完整物化步骤；恢复原任务描述与策略。
with Job.replay("dacp://localhost:8815", execution_id,
                on_retry=reset_attempt) as results:
    for event in results:
        pass  # 接收新一轮结果，不能与旧结果拼接
```

原提交者才能重放；必须保留原执行历史和未过期的物化文件。失败任务也可从已提交边界重放。关闭结果流（`close()`、离开上下文或 shell 中 Ctrl-C）取消本次执行，终止步骤与后续重试。回调抛出异常也会关闭流并取消。

Scala 使用 `Job.run(node, script, executionPolicy = policy, onRetry = Some(reset))` 与 `Job.replay(node, executionId, onRetry = Some(reset))`。结果流及 `OutputEvent.executionId` 提供执行 ID。

Shell 支持：

```text
submit workflow.yaml --policy policy.yaml
run piflow_csv_parser@2.0 --input source=/observations --view bytes --policy policy.yaml
replay EXECUTION_ID
```

Shell 显示执行 ID，重试前明确标记上一轮输出作废并重置显示状态。已经打印的终端文字仍保留作为尝试记录，不能把整段终端输出当作最终结果文件。最终输出需要物化时，在任务文件的 `outputs` 中声明端口，再通过 `submit` 提交。

## 远程协议参考

沿用 Arrow Flight TLS / Token，无 HTTP 写接口。Action 名均加 `dacp.union.` 前缀：

| Action | 请求 JSON | 返回 |
|---|---|---|
| `submit` | `{"job_yaml":"...","execution_policy_yaml":"materialization:\n  allowed: false\n"}` | 新执行快照；策略可省略 |
| `replay_job` | `{"job_id":"原执行 ID","steps":["ta"]}` | 新执行快照；`steps` 可省略 |
| 节点 `materialized_status` | `{"union_id":"...","execution_id":"...","task":"ta"}` | 是否提交、到期时间、存储端口校验信息；仅批准的调度者可用 |

结果继续通过原有 `results` Flight 流读取，要求 `start/data/end/complete` 以及成功的 Flight 结束状态。普通结果流仍只允许领取一次；第二次读取使用 `replay_job` 创建新执行。

节点 `prepare` 的各任务增加 `materialize: ["result"]` 以及可选 `replay_from: "源执行 ID"`。准备阶段验证所有重放输出存在，并持有租约；重放数据继续使用新的执行 ID 与原消费者证书绑定，不直接向客户端暴露磁盘路径。

存储扩展的接口、ServiceLoader 注册及契约测试见 [XStream 执行策略参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/materialization_zh.md)。

单机 Flight DoExchange 的 `OperatorRequest` 增加 `execution_policy_yaml`（YAML 字节）、`retry_aware`（布尔值）与 `replay_from`（原执行 ID，此时 `job_yaml` 必须为空）。结果流首先发送 `execution` 元数据帧（包含 `execution_id`），随后输出帧也包含该 ID。两种传输在下一次尝试前均发送 `attempt_failed` 元数据，包含 `attempt_id`、`next_attempt_id`、`retry_count` 和 `message`。Union 的结果 ticket 通过 `retry_aware: true` 声明接收能力。

## 临时空间请求

在执行策略中传入 `resources.tmp_size_mb` 与 `resources.memory_mb`，例如 `resources: {tmp_size_mb: 2048, memory_mb: 4096}`。此配置控制容器内存型 `/tmp`，不是磁盘存储。Union 将请求下发给各成员，成员在启动前按其启动配置 `execution_resources.max_tmp_size_mb` 和 `max_memory_mb` 验证。默认值、单位、融合语义和内存约束见 [XStream 执行策略参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/materialization_zh.md#容器临时空间与内存)。
