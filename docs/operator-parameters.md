# 算子参数契约

每个算子必须在 `operator.yaml` 和发布清单 `manifest.json` 中提供 `parameters`。它是语言无关、可静态读取的参数结构定义，适用于 Python、Java/Scala、原生命令及自定义执行器。不启动算子或推导 worker，不从说明文字或示例值猜测类型。

输入流声明在 `inputs`，参数声明在 `parameters`，输出流结构由 `inferOutputs` / `infer_outputs` 给出。三者相互独立。参数契约不是输出 schema。

## 完整示例

```yaml
name: weather_filter
version: "1.0"
image: example/python:1
executor: python
entrypoint: weather_filter
artifact: weather_filter.py
inputs:
  source: {representations: [records], formats: []}
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties:
    min_temperature:
      type: number
      description: 最低保留温度，摄氏度。
      default: 0
      minimum: -100
      maximum: 100
    station:
      type: string
      description: 需要保留的站点名称。
      minLength: 1
    include_quality:
      type: boolean
      description: 是否输出质量标记。
      default: true
    columns:
      type: array
      description: 输出字段名称，按指定顺序保留。
      items: {type: string, minLength: 1}
      minItems: 1
  required: [station]
  additionalProperties: false
```

Job 步骤实际传入的是值，而不是契约：

```yaml
params:
  station: DEMO-001
  min_temperature: $MIN_TEMPERATURE
  include_quality: true
  columns: [timestamp, temperature]
```

变量文件：

```yaml
MIN_TEMPERATURE: 12.5
```

参数声明类型仍为 `number`，不是 `variable`。`$MIN_TEMPERATURE` 是完整值引用，变量替换保留 YAML 类型。Python 从 `ctx.params['min_temperature']` 获取数值；Scala 从 `ctx.params.path("min_temperature").asDouble()` 获取数值。

## 支持的定义

采用 JSON Schema 风格的受限子集；不声称支持全部 JSON Schema。未知关键字会被拒绝，避免编辑器忽略约束。

| 关键字 | 用途 |
|---|---|
| `type` | 必填；`string`、`integer`、`number`、`boolean`、`null`、`object` 或 `array`；也支持非空、无重复的类型数组，如 `[string, object]` |
| `title`、`description` | 可选的显示名称和说明 |
| `default` | 可选的默认值，必须符合本定义的类型与约束 |
| `enum` | 可选的非空、无重复枚举，所有值必须符合类型与约束 |
| `minimum`、`maximum` | 数值上下界，包含边界 |
| `minLength`、`maxLength` | 字符串 Unicode 码点长度范围 |
| `items`、`minItems`、`maxItems` | 数组必须声明元素定义，可设置长度范围 |
| `properties` | 对象必须声明的属性映射，每项递归使用同一契约 |
| `required` | 对象必须声明的必填属性名数组；没有必填项时用 `[]` |
| `additionalProperties` | 对象必须显式声明；`false` 拒绝未知属性，`true` 允许动态属性，也可用子定义限制动态属性类型 |

根定义必须为 `type: object`。最多嵌套 16 层。空参数列表也必须明确声明：

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

动态映射示例，如将列名映射到聚合函数：

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties:
    aggregates:
      type: object
      properties: {}
      required: []
      additionalProperties: {type: string, enum: [AVG, SUM, MIN, MAX, COUNT]}
  required: [aggregates]
  additionalProperties: false
```

## 发布、设计与执行

- 发布/seed 验证结构、默认值、枚举、范围和必填属性声明。缺失 `parameters` 的定义不能发布。`publish --ports FILE` 的文件必须同时包含 `inputs` 和 `parameters`；可以直接传完整的 `operator.yaml`。HTTP 上传的清单遵循相同规则。
- 仓库列表及详情 API 返回完整定义；网页详情展示 Parameters。清单版本仍为 `api_version: 2`，本项目处于未发布开发阶段，不保留旧定义的兼容分支。
- 设计器打开时自动连接配置的 Union（优先）或节点，并独立加载默认仓库。连接由服务配置指定，界面不提供连接切换。属性面板有 **Step**、**Parameters** 和 **Outputs** 三个标签页。
- Parameters 从声明生成字段，名称和允许的类型由算子决定；显示必填项、说明、默认值、枚举及范围。可选参数通过复选框显式启用；未启用时不写入 Job。默认值显示为提示及编辑初值，不由服务器自动注入参数。
- 合法修改自动保存并同步 Job YAML；未完成或非法输入保留在表单中并显示提示；变量引用延后到提交时替换。无对应仓库定义时提示先加载定义，不将手动选择的类型当成正式契约。浏览器数值采用 JavaScript Number；整数输入限制在安全整数范围。
- 不增加运行前的 `inferSchema` 或参数推导调用，也不在运行时根据契约自动填充默认值。算子实现仍负责业务参数检查及缺省行为，作者应确保声明与实现一致。

共享 Scala 契约验证在 `dacp-operator-contract` 模块；仓库、节点解析器及设计器使用相同实现，不包含任何算子名称或执行器专属分支。第三方作者增加定义即可被发现，无需修改核心注册表。

算子还必须提供 `category` 和 `icon`，规则和发布示例见[分类与图标](operator-presentation.md)。
