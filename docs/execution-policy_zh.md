# 使用执行策略提交 Job

[English](execution-policy.md)

节点内嵌 XStream，Union 协调经批准成员上的执行。执行策略与 Job 分开提供：

```yaml
max_retries: 1
timeout_seconds: 1800
resources:
  tmp_size_mb: 512
  memory_mb: 1024
```

`max_retries` 表示额外执行尝试次数，默认 0。失败尝试先停止全部参与者、丢弃 Sink 暂存；没有显式检查点时重跑全部步骤，有效的完整检查点则替代已覆盖的上游计算。取消、超时、授权/配置错误、清理失败及提交开始后的失败，都不会触发自动重试。各次尝试共享执行时间预算，Union 的环境准备使用独立超时。

使用 Shell `submit workflow.yaml --policy execution-policy.yaml` 或 SDK 提交。客户端发送策略内容，不发送服务端文件路径。节点/Union 的启动配置限制执行时间、容器临时内存和并发数。[XStream 执行策略参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/execution-policy_zh.md)说明字段和资源限制。

## 选择输出检查点

```yaml
max_retries: 2
checkpoints:
  sort_step: [result]
```

该策略先完整保存 `sort_step.result`，再启动其下游读取。下游处理失败后，在全部旧线程停止、Sink 丢弃成功后，同一次执行可复用该输出；未覆盖分支重新计算。只有全部已连接输出均保存时，才能跳过该生产步骤。默认仍然流式执行，省略 `checkpoints` 或使用 `{}` 即可；不自动选择边界，也不提供跨执行 replay。

管理员在每个生产节点（或作为成员的 Union）配置 `checkpoint_store`：

```yaml
checkpoint_store:
  provider: file
  settings:
    directory: /var/lib/dacp/checkpoints
    max_bytes: 1073741824
    max_checkpoint_bytes: 268435456
```

文件实现随 XStream 执行文件插件提供，Job 不能指定宿主路径，存储超额及校验失败均明确报错。Union 将检查点固定在生产节点，在内部尝试间保留，并在终态调用绑定证书身份的 `release_checkpoints` 操作释放。清理按调度方身份、Union ID 和 Job 执行 ID 隔离，不开放任意产物读取 API。工作节点还会在配置的执行、准备和保留宽限期后回收遗留终态执行；宿主重启清理全部旧检查点。检查点不恢复丢失节点或进程。见 [XStream 检查点契约](https://github.com/rdcn-link/xstream-codex/blob/main/docs/execution-policy_zh.md#显式输出检查点)。

## 安全接收暂定结果

```scala
import java.io.ByteArrayOutputStream
import scala.util.Using
import dacp.client.{Job, TlsConfig}
import dacp.union.Json

val provisional = new ByteArrayOutputStream()
val policy = Json.obj().put("max_retries", 1)
Using.resource(Job.run(node, workflowYaml, tls,
  executionPolicy = policy,
  onRetry = Some(_ => provisional.reset()))) { stream =>
  stream.foreach { event =>
    if (event.kind == "data") provisional.write(event.data)
  }
}
val finalBytes = provisional.toByteArray // Use only after successful complete consumption.
```

示例假设只有一个 bytes 输出且结果较小；`node` 是 DACP origin，`workflowYaml` 是 Job 文本，`tls` 是验证服务器身份并携带授权 token 的 `TlsConfig`。大结果应暂存文件，records 则需要 Arrow 解码，不能直接拼接字节。Union 的 Scala `execution.results(onRetry = Some(...))` 与 Python `execution.results(on_retry=...)` 遵循相同的丢弃规则。自动重试要求接收器显式声明支持重试。

## Sink 协调与执行历史

SDK Sink 暂存完成后等待整个 Job 的决定。Union 等待全部 Worker 子任务报告 `ready_to_commit` 且最终结果流结束，再调用各成员绑定证书的内部 `complete_execution`。多级 Union 子任务把提交决定交给上级。现有取消操作会丢弃暂存，重试前确认旧工作线程停止。这些是内部服务操作，不增加公开管理入口。

历史记录保存提交/生效策略、尝试标识及状态。`commit_started` 标记发布边界；发布开始后失败不会自动重试。多个 Sink 的提交不是分布式原子事务，需要核查不确定结果，并采用幂等外部发布。格式解析暂存、日志、应用最终结果和执行历史分别管理。

手动重跑需要将保存的 Job 和已解析变量重新作为一个新 Job 提交。可复现性需要稳定的输入版本、固定的算子版本以及可控随机性。

## 执行单元模式

节点启动配置支持 `operator_fusion: isolated`、`fused` 或 `auto`（默认）。Union 成员节点将此模式应用于兼容的本地子 Job。`auto` 采用保守的结构代价估计，不代表实测延迟或更快的保证；仍执行兼容性、Sink、检查点及重试边界约束。这不是单个 Job 的执行策略字段。详见[合并参考](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-fusion_zh.md)。
