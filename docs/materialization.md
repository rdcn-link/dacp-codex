# 执行策略、输出物化与重放

[English](materialization_en.md)

执行策略同时用于通过 Python 或 Scala 客户端提交的单机 DACP Server 任务和 Union 协作工作流。业务任务文件定义算子、输入输出、依赖及执行节点；独立的执行策略 JSON 控制哪些**已连接的输出端口**物化。

默认仍是直接流处理，不保存输出。`materialize` 表示在产出节点边传输边保存原始 OperatorFrame（包括 bytes 或 records 的 schema 与批次边界）。它不创建 Dataset，不把跨节点数据中转到 Union，不包含算子内部状态 checkpoint。

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

`directory` 是节点私有路径，不能由任务提交者指定。文件存储插件随标准发行包提供，位于 `plugins/dacp-plugin-execution-file`。省略 `materialization_store` 时，该节点拒绝需要物化或重放的任务。Union 先 prepare 全部节点，再启动任何步骤，因此不支持存储会导致准备失败，而非静默降级。

文件插件必须提供 `directory`；其余三个设置默认为上述值，且须为正整数。`max_bytes` 限制整个目录的受管理数据，`max_task_bytes` 限制单个任务的所有选定输出，两者包含帧长度前缀和清单开销；配额不足使执行失败，清理未提交数据。物化保留时间与内存会话的 `union.retention_seconds` 相互独立。到期数据不能被新执行获取；正在使用的租约关闭后才删除文件。存储目录只允许一个写入服务，启动时清理未提交临时目录。

只有算子成功、输入完整消费且所有选定输出均完整时，任务的全部选定端口才原子提交。提交前不可重放；取消、失败和配额错误会废弃未提交数据。已成功提交的上游输出不因下游失败而删除。文件插件同步文件并原子发布清单，通过长度和 SHA-256 检查拒绝截断或损坏的重放数据。

## 用户：独立策略 JSON

例如 `execution-policy.json`：

```json
{
  "max_retries": 0,
  "materialization": {
    "allowed": true,
    "default": "stream",
    "tasks": {
      "ta": {
        "outputs": {
          "result": "materialize"
        }
      }
    }
  }
}
```

- `allowed` 默认 `true`，只表示允许，不会自动开启物化。
- `default` 默认 `stream`；可设为 `materialize`，为所有已连接输出开启物化。
- `tasks.<任务 id>.outputs.<端口>` 可覆盖默认值，值为 `stream` 或 `materialize`。这里的任务 id 是步骤标识，不是服务器名。
- 省略策略等同于直接流处理；不存在风险判断后自动物化。
- 未知字段、类型错误、不存在的步骤、未连接的端口、非法取值及策略冲突在提交时拒绝。不为未连接的算子端口额外创建消费者。

全局禁止引擎保存结果流：

```json
{"materialization":{"allowed":false,"default":"stream"}}
```

此时任何物化默认值或步骤覆盖都会报错，即使某个覆盖最终未被执行。该约束不等于禁止算子自身文件写入、格式处理临时文件、日志或执行元数据。策略不能扩大节点的算子、输入前缀、容量和身份授权。

Python：

```python
import json
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('workflow.yaml').read_text(),
                      execution_policy=json.loads(Path('execution-policy.json').read_text())) as execution:
        job_id = execution.job_id  # 保存，用于之后重放
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

Scala：

```scala
val policy = dacp.union.Json.parse(java.nio.file.Files.readString(
  java.nio.file.Path.of("execution-policy.json")))
scala.util.Using.resource(union.submit(workflowYaml, policy)) { execution =>
  println(execution.id)
  scala.util.Using.resource(execution.results())(_.foreach(println))
}
```

状态与执行历史分别记录 `execution_policy`（提交值）和 `effective_execution_policy`（应用默认值与覆盖后的已连接端口策略）。节点状态的 `materialized_tasks` 记录源执行及是否提交；新执行历史的 `replay_of` 和 `materialized_sources` 关联重放来源。启用物化的子 Job 不进行同进程步骤合并，以保留明确的提交边界。

## 什么时候重放

仅在客户端**显式请求**时重放。原执行必须已经结束：成功、失败、取消、超时或重启后标记为中断。运行中的工作流不支持重放；仅当显式设置正数 `max_retries` 时才自动重试。

```python
with union.replay(job_id, tasks=['ta']) as replay:
    print(replay.job_id)  # 新的执行 ID
    for event in replay.results():
        if event['type'] == 'data':
            print(event.get('batch', event['data']))
```

也可调用 `execution.replay(tasks=['ta'])`。Scala 对应 `union.replay(jobId, Some(Seq("ta")))` 或 `execution.replay(Some(Seq("ta")))`。

- **下游失败**：复用 `ta` 的已提交输出，不重新执行 `ta` 算子；仍需执行的下游重新运行。客户端需要重新消费结果流。
- **重新获取成功结果**：若最终输出步骤的全部已连接输出均已物化，可直接重放这些步骤。无须读取它们的原始输入或执行其上游；被剪枝步骤记录为 `skipped`。
- 省略 `tasks` 时，先检查策略中所有输出均要求物化的步骤，选择当前已有完整、未过期提交的步骤，再按最终输出裁剪所需工作流。指定 `tasks` 时，每个步骤都必须可用，否则拒绝。至少需要一个可重放步骤。
- 只有某步骤**全部已连接输出**均物化时，才能跳过该算子；仅物化部分输出仍可保存，但不能用于跳过这个多输出步骤。多端口重放并行读取，保留背压，避免端口顺序导致互相等待。
- 重放使用原任务文件与原策略，不能借此修改算子、参数或输入。节点会再次核对定义与存储清单。客户端必须仍为原提交者，Union 仍须获节点批准，算子及输入授权也会重新校验。
- 原执行元数据及节点存储都保留时，Union/节点重启后仍可新建重放执行。它不是自动恢复旧进程或执行计划，也不是按字节游标续读。

结果过期、未完整提交、损坏、节点离线或授权失效会报错，不静默当成重放成功。未选中复用的步骤可能重新读取当前原始数据；本功能不保证跨节点数据快照一致性，也不提供有副作用算子的 exactly-once 执行。

## 自动重试次数与总超时

顶层 `max_retries` 是 0–10 的整数，默认 `0`；`2` 表示首次执行之后最多再试两次。`timeout_seconds` 是 1–86400 的整数，覆盖整个任务的执行、资源等待和所有重试；省略时使用服务器的 `execution_timeout_seconds`，提交值不能超过服务器上限。Union 下各节点还可执行更小的本地时限。

```json
{"max_retries": 2, "timeout_seconds": 120, "materialization": {"allowed": false}}
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

DACP Server 的单机任务采用同样的策略 JSON、流通知和存储插件，无需配置 Union。管理员在服务器配置顶层增加：

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
submit workflow.yaml --policy policy.json
run piflow_csv_parser@1.0 --input source=/observations --view bytes --policy policy.json
replay EXECUTION_ID
```

Shell 显示执行 ID，重试前明确标记上一轮输出作废并重置显示状态。已经打印的终端文字仍保留作为尝试记录，不能把整段终端输出当作最终结果文件。最终输出需要物化时，在任务文件的 `outputs` 中声明端口，再通过 `submit` 提交。

## 开发者：Flight 协议与存储插件

沿用 Arrow Flight TLS / Token，无 HTTP 写接口。Action 名均加 `dacp.union.` 前缀：

| Action | 请求 JSON | 返回 |
|---|---|---|
| `submit` | `{"job_yaml":"...","execution_policy":{...}}` | 新执行快照；策略可省略 |
| `replay_job` | `{"job_id":"原执行 ID","tasks":["ta"]}` | 新执行快照；`tasks` 可省略 |
| 节点 `materialized_status` | `{"union_id":"...","execution_id":"...","task":"ta"}` | 是否提交、到期时间、存储端口校验信息；仅批准的调度者可用 |

结果继续通过原有 `results` Flight 流读取，要求 `start/data/end/complete` 以及成功的 Flight 结束状态。普通结果流仍只允许领取一次；第二次读取使用 `replay_job` 创建新执行。

节点 `prepare` 的各任务增加 `materialize: ["result"]` 以及可选 `replay_from: "源执行 ID"`。准备阶段验证所有重放输出存在，并持有租约；重放数据继续使用新的执行 ID 与原消费者证书绑定，不直接向客户端暴露磁盘路径。

公共契约在 `dacp-plugin-api`：`MaterializationStoreProvider`、`MaterializationStore`、`MaterializationWriter`、`MaterializedTask` 和 `MaterializedChunks`。外部实现通过 `META-INF/services/dacp.MaterializationStoreProvider` 注册提供者类，将 JAR 与依赖安装到节点 `plugin_directory`，并将 `provider` 设置为其 `id`，无须修改核心注册表。

插件负责私有配置、配额、过期清理、原子任务提交、持久化、租约与读取完整性；核心负责授权、策略校验、帧协议、取消及运行资源。每个 opaque chunk 不超过 9 MiB，插件不得缓存整个任务。关闭未提交 writer 必须中止，提交后才可 `get`，到期后不可新建租约；清理不得破坏现有租约。新实现应通过发现、提交/中止、重启读取、配额、过期/租约、截断/损坏和关闭清理契约测试；参考 `MaterializationSpec` 及跨节点 `UnionSpec`。

单机 Flight DoExchange 的 `OperatorRequest` 增加 `execution_policy_json`（JSON 字节）、`retry_aware`（布尔值）与 `replay_from`（原执行 ID，此时 `job_yaml` 必须为空）。结果流首先发送 `execution` 元数据帧（包含 `execution_id`），随后输出帧也包含该 ID。两种传输在下一次尝试前均发送 `attempt_failed` 元数据，包含 `attempt_id`、`next_attempt_id`、`retry_count` 和 `message`。Union 的结果 ticket 通过 `retry_aware: true` 声明接收能力。
