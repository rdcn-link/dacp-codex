# 算子与工作流结构推导

[English](structure-inference_en.md)

每个算子必须提供结构推导方法。清单只声明输入契约，不再允许 `outputs`；输出端口、表示、格式、schema 和未知信息均由方法返回。无输出算子返回空映射，数据源接受空输入。方法实现缺失、响应格式错误或超时会导致校验失败，不回退到清单。

## 使用方式

在 shell 中，先查看结构，再提交同一个工作流：

```text
infer workflow.yaml --vars variables.json
submit workflow.yaml --vars variables.json
```

`infer` 只查询元信息，不执行数据处理，不预览实际数据。单机和 Union 均通过 Flight 提供 `dacp.infer`（单算子）及 `dacp.infer_job`（完整工作流）。普通客户端使用自己的服务器连接凭据；Union 向实际执行节点请求推导，公开结果只含任务名与结构，不返回内部成员地址。

Python：

```python
from dacp import Job
report = Job.infer("dacp://localhost:18861", workflow_text, variables={"LIMIT": 123})
# 已连接的 Union SDK 实例：
report = union.infer(workflow_text, variables={"LIMIT": 123})
```

Scala：

```scala
val report = dacp.client.Job.infer("dacp://localhost:18861", workflowText)
val unionReport = union.infer(workflowText)
```

结构推导仅在用户显式调用 `infer` 时执行，供工作流编制、校验和预览使用。提交、调度、重试、重放及步骤执行均不自动调用推导方法，假定提交的工作流符合要求。执行时使用实际流头中的表示、格式和 schema，保留协议完整性、端口连接、授权和资源限制检查；无输出算子无需虚构结果端口。

## 统一请求与响应

单次命令行推导使用 `infer /input/inference.json`，读取请求文件并向标准输出写一个 JSON 响应。服务器使用下述常驻 worker 协议，日志写标准错误，不发送 Arrow 数据帧。

请求：

```json
{
  "inputs": {
    "source": {"representations": ["bytes"], "formats": ["text"], "certainty": "known"}
  },
  "params": {"uppercase": true}
}
```

响应：

```json
{
  "outputs": {
    "result": {"representations": ["bytes"], "formats": ["text"], "certainty": "known"}
  },
  "diagnostics": []
}
```

`certainty` 必须为 `known` 或 `unknown`。不确定表示时可返回空的 `representations`，不确定格式时返回空的 `formats`；所有输出端口名称仍必须列出。已知 records 必须给出 `schema`，其值为 Base64 编码的 Arrow IPC schema 消息，与流头一致。不得用空 schema 假装未知。可选 `structure` 对象携带领域结构（例如多维数组的维度），不将科学数据自动展开成表格；当前执行协议仍只支持 bytes/records，扩展表示需要相应的执行协议支持。

诊断为 `{"severity":"error","code":"unknown_column","message":"Unknown column: temperature"}` 或 `warning`。错误阻止执行，警告不会。清单输入契约仍用于检查算子能否接受连接进来的流。

## Python 作者

`BytesOperator` 与 `RecordsOperator` 已实现推导方法；表格模板复用 `output_schema`，输入 schema 未知时保留未知。自定义多端口算子实现 `infer_outputs(inputs, params)`，供编辑时显式调用：

```python
from dacp_operator import descriptor, result, run_operator

def infer_outputs(inputs, params):
    if set(inputs) != {"source"}:
        raise ValueError("Expected source input")
    return result({"result": descriptor(["bytes"], ["text"], certainty="known")})

def process(ctx):
    out = ctx.bytes_output("result", "text")
    for chunk in ctx.input("source").bytes():
        out.write(chunk.upper())

if __name__ == "__main__":
    run_operator(process, infer_outputs)
```

`descriptor(..., schema=pyarrow_schema, certainty="known")` 处理 Arrow schema 序列化，`records_schema(input_descriptor)` 读取输入 schema。不要在模块导入或推导方法中启动业务处理。

## Scala 作者

继承 `Operator` 时必须实现方法；字节及表格模板已经提供实现：

```scala
import dacp.{StreamDescriptor,InferenceResult}
import dacp.operator.{Operator,OperatorContext}
import com.fasterxml.jackson.databind.JsonNode

object Uppercase extends Operator {
  def inferOutputs(inputs: Map[String, StreamDescriptor], params: JsonNode): InferenceResult = {
    require(inputs.keySet == Set("source"), "Expected source input")
    InferenceResult(Map("result" -> StreamDescriptor(Set("bytes"), Set("text"), certainty="known")))
  }
  def process(ctx: OperatorContext): Unit = {
    val out = ctx.bytesOutput("result", "text")
    ctx.input("source").foreachBytes(chunk => out.write(new String(chunk, "UTF-8").toUpperCase.getBytes("UTF-8")))
  }
}
```

原生 `command` 执行器的适配方法根据 stdin/stdout 配置推导 bytes 端口，不启动底层可执行程序。外部执行器插件通过 `inferenceWorkerCommand`（源码为 `sourceInferenceWorkerCommand`）接入相同 JSON 协议，无需修改核心注册表。清单和插件配置不得另建输出 schema 来源。

## 隔离与限制

推导方法只能做元信息计算，不能读实际数据、访问外部服务或产生副作用。服务端在无网络、只读根文件系统的隔离 worker 容器中调用，每个 worker 限制为 512 MiB 内存、64 个进程和一个 CPU；请求及响应各最多 1 MiB。只挂载元数据管道、worker 适配代码和只读算子制品，不挂载数据集或凭据。`read_item` 是核心传输算子，使用 Flight 元信息查询而不打开内容流；数据插件描述 Item 时也应避免扫描内容。

修改 SDK 或运行时后需要重新构建相应运行环境与算子包。仓库发布不在仓库进程中执行用户代码；推导能力在执行节点实际调用时验证。旧的清单输出声明直接移除，不提供兼容路径。

契约测试：`sbt test`（`dacp-operator-sdk`、`dacp-server`），以及仓库根目录 `.venv/bin/python scripts/test-operator-inference.py`。

## 按需启动与空闲回收

节点为显式推导按需启动 worker；相同节点实例、调用主体、算子制品、运行镜像和启动配置的请求复用同一 worker。参数和输入结构不属于复用键，每次都重新调用推导方法，不缓存结果。不同主体、算子内容或镜像不会共用 worker。Python 模块或 wheel 只初始化、安装一次，JVM 只启动并加载一次；SDK 模板实例在 worker 内复用。推导方法必须保持确定性，不依赖前一请求的状态。

节点顶层配置：

```yaml
inference_workers:
  max_workers: 4
  idle_seconds: 60
  timeout_seconds: 60
```

以上为默认值。`max_workers` 范围为 1–64，`idle_seconds` 为 1–86400，`timeout_seconds` 为 1–120，均为整数。超时包含排队、冷启动及本次计算；外层 Flight 调用可能具有更短的截止时间。相同 worker 串行处理请求；达到上限时优先淘汰最久未使用的空闲 worker，否则等待可用位置。空闲检查周期为一秒，超过期限的空闲 worker 会销毁。异常、非法响应、超时或执行中的推导请求取消，会销毁对应 worker，下一次显式请求重建；不会自动重试本次推导。取消一个尚在排队的请求不影响正在服务其他请求的 worker。节点关闭时清理全部 worker。

这是运行资源复用，不与任务执行容器混用，也不使提交、调度、执行、重试或重放自动触发推导。Union 将显式推导转发给成员，由成员管理 worker；Union 不加载成员的算子代码。

## 执行器插件的 worker 协议

外部执行器仍通过 `META-INF/services/dacp.OperatorExecutor` 注册，在 `plugin_directory` 中安装 JAR 和依赖。实现 `inferenceWorkerCommand(program, artifactPath)`，支持源码时再实现 `sourceInferenceWorkerCommand(program, artifactPath)`，无须修改核心注册表或客户端。默认实现将普通启动命令的参数改为：

```text
infer-worker /input/requests.fifo /input/responses.fifo
```

进程打开请求 FIFO 读取、响应 FIFO 写入，先在响应管道写一个大端 32 位零值，表示初始化完成。之后循环读取“大端 32 位长度 + UTF-8 JSON 请求”，返回相同封装的 JSON 推导结果；长度范围为 1–1048576 字节，请求和结果格式仍采用本文的共享协议。请求之间不退出、不重新加载算子；在请求间 EOF 时退出，截断、超限、方法异常或非法结果时退出并报告标准错误。不要向元数据管道写日志，不接收或发送真实数据流。

Java/Scala SDK 的 `OperatorRunner.infer` 与 `Operator.main` 支持该模式；Python 的 `run_inference`、`run_operator` 和 `Operator.main` 也支持。内置 JVM、Python 和原生命令执行器提供适配器和当前 worker SDK；源码编译和 wheel 安装都只在 worker 初始化时执行。自带容器入口的算子须在镜像内提供支持 worker 模式的 SDK 或实现上述协议。原生命令由适配器推导结构，不启动底层可执行程序。

验证：`InferenceWorkerPoolSpec` 覆盖复用、容量、空闲回收、异常重建、取消、超时和关闭；SDK `InferenceSpec` 与 `scripts/test-operator-inference.py` 覆盖连续请求和帧边界。`scripts/test-inference-workers.py` 使用真实容器和 Flight 验证不同参数复用同一 Python/JVM 进程、空闲回收及故障重建。
