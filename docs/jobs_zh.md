# 流式作业

[English](jobs.md)

DACP Server 与 Union 使用同一种 Job 格式。步骤通过 `inputs` 引用其他步骤的输出流建立关联，列表顺序不代表执行顺序。不支持 `depends_on` 控制依赖。

```yaml
name: weather-filter
tasks:
  - id: read
    title: Read weather observations
    operator: read_item@1.0
    inputs: {}
    params:
      uri: context:/weather/observations
      view: records
  - id: copy
    title: Filter valid observations
    operator: piflow_filter@1.0
    inputs:
      source: read
    params:
      condition: "quality = '0'"
outputs:
  result: copy
```

## 字段与流连接

`name` 是必填的可读工作流名称，与自动生成的执行 ID 分开；最多 256 个字符，不允许空白名称或控制字符。每个 task 包含唯一 `id`、可选说明 `title`、带版本的 `operator`、命名输入 `inputs`，以及可选的参数 `params`。输入值和 Job 输出使用 `task` 或 `task:port`，省略端口时使用 `result`。不支持对象形式连接或连接级 `view`。每个输出最多有一个消费者；分发数据需要显式复制算子或不同输出端口。

`read_item@1.0` 是无需算子容器的内置传输源，无输入，输出端口为 `result`。只提供 `uri` 参数：`context:/dataset/item` 指向接收 Job 的 JVM 的 DataCatalog，`dacp://host:port/dataset/item` 显式指定服务。可选 `view` 为 `bytes` 或 `records`；未指定时优先 records，否则 bytes。可选 `sql` 查询 records 视图，提供后默认选择 records，显式选择 bytes 则报错。

向 Union 提交时，使用其公开数据项路径。Union 负责解析数据所在成员、分配步骤并下发节点子 Job；用户仅描述逻辑数据流。

## 结构推导与无输出步骤

所有算子必须实现[结构推导](structure-inference_zh.md)，输出描述由该方法给出，不在清单中声明。无输出算子可以推导出空输出映射，并成功结束而不发送数据帧。Job 可设 `outputs: {}`，客户端等待整体完成而不接收数据。未选入最终结果的分支仍会执行。 提交和执行不自动调用推导方法；仅在编制时按需显式调用 `infer`。

上下游以有界缓冲并发运行。故障、取消、超时会终止本次尝试并释放资源。重试与物化通过独立的[执行策略 YAML](materialization_zh.md) 配置。重试可能重复外部副作用，业务算子需要提供适当的幂等机制。


当前协作示例已发布的算子、参数和完整输入输出示例见[算子库清单](operator-catalog_zh.md)。

## Shell

```text
submit job.yaml
submit job.yaml --vars variables.yaml
run piflow_filter@1.0 --input source=/observations --view records --params '{"condition":"CAST(quality AS INTEGER) = 0"}'
run piflow_csv_parser@1.0 --input source=/observations --view bytes
```

Shell 将 Item 输入转换为 `read_item` 步骤，逐块打印结果。连接节点或 Union 使用 [TLS 与 Token](security_zh.md)。多输出可重复指定 `--output PORT`，无输出步骤使用 `--no-output`。源码 heredoc 仅允许 DEBUG 模式，见[源码提交](operator-source_zh.md)。

## 工作流变量

提交工作流时，可以通过独立 YAML 文件设置变量，无需修改工作流模板。YAML 文件的顶层必须是映射，例如 `VAR_NAME: 123`，数值、布尔值和列表会保留其类型。

工作流中，将需要替换的完整值写为 `$VAR_NAME`：

```yaml
params:
  limit: $VAR_NAME
  schema: $SCHEMA
```

对应的 `variables.yaml`：

```yaml
VAR_NAME: 123
SCHEMA:
  temperature: float64
```

在已连接服务器的 shell 中执行：

```text
submit workflow.yaml --vars variables.yaml
submit workflow.yaml --vars variables.yaml --policy policy.yaml
```

也支持交互输入：`submit --vars variables.yaml`，输入工作流后以单独一行 `END` 结束。完整的转换示例为：

```text
submit examples/convert-variables.yaml --vars examples/convert-variables-values.yaml
```

示例路径相对于 shell 工作目录；请将变量文件的 `INPUT_URI` 改为目标服务器可访问的 Item URI，并确保所引用的算子可用。

变量名区分大小写，匹配 `[A-Za-z_][A-Za-z0-9_]*`。提供变量对象后，客户端解析 YAML，递归替换完整的字符串值，保留数字、布尔、字符串、列表、映射和 `null` 类型，随后将解析后的工作流提交给服务器。字典键不替换；`prefix-$VAR_NAME` 不进行局部拼接；变量值中的 `$OTHER` 不再次展开。不会读取环境变量或执行 Shell 命令。缺失变量或无效变量文件会在提交前报错，多余变量允许存在。不提供 `--vars` / SDK `variables` 参数时保持原有提交行为。

Python SDK 对单机和 Union 提交均接受变量字典：

```python
import yaml
from pathlib import Path
from dacp import Job

workflow = Path("workflow.yaml").read_text()
variables = yaml.safe_load(Path("variables.yaml").read_text())
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
val variables = dacp.union.YamlDocument.parse(Files.readString(Path.of("variables.yaml")))
val result = Job.run("dacp://localhost:18861", workflow, variables = variables)
try result.foreach(println) finally result.close()
// union 为已连接的 dacp.client.Union 实例
val execution = union.submit(workflow, variables = variables)
```

变量文件在客户端读取，不需要上传至节点文件系统。服务器记录和执行的是替换后的工作流，因此自动重试和重放沿用首次提交的具体值；重放不能覆盖变量。需要使用新变量时重新提交。替换后的内容也可能出现在任务历史和看板中，不应将密码等秘密放入变量。

工作流、执行策略和变量统一采用 YAML，可同时提交：

```text
submit workflow.yaml --policy policy.yaml --vars variables.yaml
```

Union 下发的子作业保留全局作业的 `name`。节点与 Union 看板统一展示 Name、Job ID、Source、Type、Status、Started at、Duration；列表优先显示名称，详情保留完整 ID。子作业定义和执行策略均显示为 YAML。YAML 文档限制为 1 MiB，拒绝重复键、不安全对象标签及集合别名；字符串形式的编号、日期和布尔文本应加引号。

`read_item` 的 `view` 可省略：优先使用 Item 提供的 records 视图，否则使用 bytes。显式指定时不会自动改选；不支持所选视图则报错。

## 读取路径、远程服务与 SQL

```yaml
name: hot-weather-observations
tasks:
- id: read
  operator: read_item@1.0
  inputs: {}
  params:
    uri: context:/ds-6zmr-crvh/observations
    sql: |
      SELECT station_id, temperature
      FROM source
      WHERE temperature > 30
      LIMIT 100
outputs:
  result: read
```

显式指定服务时，使用 `uri: dacp://remote.example:18861/weather/observations`。`context:` URI 引用接收 Job 的 JVM 绑定的 DataCatalog，在远程下发之前固定数据源。路径属于提交目标的公开目录：普通节点解析本地名称，Union 解析其公开名称并调度至数据所在成员。Job 不需要知道内部成员地址。

SQL 支持 `SELECT *` 或字段列表、固定的 `FROM source`、可选 `WHERE` 和 `LIMIT`。谓词支持 `= != <> < <= > >=`、`IN`、`IS NULL`、`IS NOT NULL`、`AND`、`OR`、`NOT` 和括号；字符串使用单引号（内部单引号写成两个），字段名可使用双引号。仅支持单数据项只读查询，不支持 JOIN、聚合、函数、排序、多语句或数据修改。SQL 转换为通用 `Scan` 契约，由数据源插件执行，不拼接到数据库 SQL；插件可以下推，不支持的能力明确报错。数据保持分批流式传输，投影后的结构用于结构推导和有界样例。

远程凭据配置在实际执行读取的节点，设计器也需单独配置相同服务的读取凭据；不会将目标服务的 Token 转发给其他服务，也不允许在 Job 中携带凭据。Union 选中的任意读取成员应具备所需凭据。

```yaml
remote_item_credentials:
  "dacp://remote.example:18861": /etc/dacp/remote-reader.token
```
