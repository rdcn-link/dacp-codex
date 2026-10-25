# 流式作业

[English](jobs_en.md)

DACP Server 与 Union 使用同一种 Job 格式。步骤通过 `inputs` 引用其他步骤的输出流建立关联，列表顺序不代表执行顺序。不支持 `depends_on` 控制依赖。

```yaml
job: weather-copy
tasks:
  - id: read
    title: Read weather observations
    operator: read_item@1.0
    inputs: {}
    params:
      uri: dacp://localhost:18861/weather/observations
      view: records
  - id: copy
    title: Stream observation records
    operator: identity@1.3
    inputs:
      source: read
outputs:
  result: copy
```

## 字段与流连接

`job` 是工作流名称。每个 task 包含唯一 `id`、可选说明 `title`、带版本的 `operator`、命名输入 `inputs`，以及可选的参数 `params` 和执行位置 `node`。输入值和 Job 输出使用 `task` 或 `task:port`，省略端口时使用 `result`。不支持对象形式连接或连接级 `view`。每个输出最多有一个消费者；分发数据需要显式复制算子或不同输出端口。

`read_item@1.0` 是无需算子容器的内置传输源。它没有输入，参数 `uri` 指向一个 Item，`view` 为 `bytes` 或 `records`，输出端口为 `result`。`live_ticks` 等生成器的输入也为空。处理算子消费上游输出，表示形式转换必须显式完成。

向 Union 提交时，数据 URI 使用 Union 公开的数据集名称。省略 `node` 时自动调度，也可使用 `dacp.nodes` 返回的逻辑节点。用户提交的工作流不能直接使用成员实际地址。

## 结构推导与无输出步骤

所有算子必须实现[结构推导](structure-inference.md)，输出描述由该方法给出，不在清单中声明。无输出算子可以推导出空输出映射，并成功结束而不发送数据帧。Job 可设 `outputs: {}`，客户端等待整体完成而不接收数据。未选入最终结果的分支仍会执行。

上下游以有界缓冲并发运行。故障、取消、超时会终止本次尝试并释放资源。重试与物化通过独立的[执行策略 JSON](materialization.md) 配置。重试可能重复外部副作用，业务算子需要提供适当的幂等机制。

## Shell

```text
submit job.yaml
submit job.yaml --vars variables.json
run identity@1.3 --input source=/observations --view records
run live_ticks@1.0
```

Shell 将 Item 输入转换为 `read_item` 步骤，逐块打印结果。连接节点或 Union 使用 [TLS 与 Token](security.md)。多输出可重复指定 `--output PORT`，无输出步骤使用 `--no-output`。源码 heredoc 仅允许 DEBUG 模式，见[源码提交](operator-source.md)。

## 工作流变量

提交工作流时，可以通过独立 JSON 文件设置变量，无需修改工作流模板。JSON 文件的顶层必须是对象，例如 `{"VAR_NAME": 123}`；它不是 YAML 的 `VAR_NAME: 123` 写法。

工作流中，将需要替换的完整值写为 `$VAR_NAME`：

```yaml
params:
  limit: $VAR_NAME
  schema: $SCHEMA
```

对应的 `variables.json`：

```json
{
  "VAR_NAME": 123,
  "SCHEMA": {"temperature": "float64"}
}
```

在已连接服务器的 shell 中执行：

```text
submit workflow.yaml --vars variables.json
submit workflow.yaml --vars variables.json --policy policy.json
```

也支持交互输入：`submit --vars variables.json`，输入工作流后以单独一行 `END` 结束。完整的转换示例为：

```text
submit examples/convert-variables.yaml --vars examples/convert-variables.json
```

示例路径相对于 shell 工作目录；请将变量文件的 `INPUT_URI` 改为目标服务器可访问的 Item URI，并确保所引用的算子可用。

变量名区分大小写，匹配 `[A-Za-z_][A-Za-z0-9_]*`。提供变量对象后，客户端解析 YAML/JSON，递归替换完整的字符串值，保留 JSON 的数字、布尔、字符串、数组、对象和 `null` 类型，随后将解析后的工作流提交给服务器。字典键不替换；`prefix-$VAR_NAME` 不进行局部拼接；变量值中的 `$OTHER` 不再次展开。不会读取环境变量或执行 Shell 命令。缺失变量或无效变量文件会在提交前报错，多余变量允许存在。不提供 `--vars` / SDK `variables` 参数时保持原有提交行为。

Python SDK 对单机和 Union 提交均接受变量字典：

```python
import json
from pathlib import Path
from dacp import Job

workflow = Path("workflow.yaml").read_text()
variables = json.loads(Path("variables.json").read_text())
with Job.run("dacp://localhost:18861", workflow, variables=variables) as result:
    for event in result:
        print(event)

# union 为已连接的 dacp.union.Union 实例
execution = union.submit(workflow, variables=variables)
```

Scala SDK 使用相同规则：

```scala
import dacp.client.Job
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.{Files, Path}

val workflow = Files.readString(Path.of("workflow.yaml"))
val variables = new ObjectMapper().readTree(Files.readString(Path.of("variables.json")))
val result = Job.run("dacp://localhost:18861", workflow, variables = variables)
try result.foreach(println) finally result.close()
// union 为已连接的 dacp.client.Union 实例
val execution = union.submit(workflow, variables = variables)
```

变量文件在客户端读取，不需要上传至节点文件系统。服务器记录和执行的是替换后的工作流，因此自动重试和重放沿用首次提交的具体值；重放不能覆盖变量。需要使用新变量时重新提交。替换后的内容也可能出现在任务历史和看板中，不应将密码等秘密放入变量。
