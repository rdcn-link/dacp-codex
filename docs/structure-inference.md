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

执行提交前逐步推导，检查输入表示、格式和已知 schema 是否兼容、被引用输出是否存在，以及算子返回的错误诊断。未知结构允许继续传播，不能视作已经验证。每一步实际启动前还会用真实输入流头重新调用推导方法；输出端口、表示、格式和已知 schema 必须符合推导契约。无输出算子无需虚构结果端口。

## 统一请求与响应

程序接收 `infer /input/inference.json`，从文件读取请求，仅在标准输出写一个 JSON 响应。日志写标准错误，不发送 Arrow 数据帧。

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

`BytesOperator` 与 `RecordsOperator` 已实现推导方法；表格模板复用 `output_schema`，输入 schema 未知时保留未知。自定义多端口算子实现 `infer_outputs(inputs, params)`，使用同一方法供编辑和执行校验调用：

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

原生 `command` 执行器的适配方法根据 stdin/stdout 配置推导 bytes 端口，不启动底层可执行程序。外部执行器插件通过 `inferenceCommand`（源码为 `sourceInferenceCommand`）接入相同 JSON 协议，无需修改核心注册表。清单和插件配置不得另建输出 schema 来源。

## 隔离与限制

推导方法只能做元信息计算，不能读实际数据、访问外部服务或产生副作用。服务端在无网络、只读根文件系统的隔离容器中调用，限制为 15 秒、512 MiB 内存、64 个进程和一个 CPU；请求及响应各最多 1 MiB。只挂载推导请求和只读算子制品，不挂载数据集。`read_item` 是核心传输算子，使用 Flight 元信息查询而不打开内容流；数据插件描述 Item 时也应避免扫描内容。

修改 SDK 或运行时后需要重新构建相应运行环境与算子包。仓库发布不在仓库进程中执行用户代码；推导能力在执行节点实际调用时验证。旧的清单输出声明直接移除，不提供兼容路径。

契约测试：`sbt test`（`dacp-operator-sdk`、`dacp-server`），以及仓库根目录 `.venv/bin/python scripts/test-operator-inference.py`。
