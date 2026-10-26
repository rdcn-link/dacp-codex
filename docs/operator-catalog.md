# 协作示例使用的算子库

[English](operator-catalog_en.md)

当前部署的协作示例仓库包含以下 6 个算子，版本均为 `1.0`，执行器均为 `python`。它们共享 Python / Arrow 运行环境，各自的 wheel 仅包含业务代码与 SDK。此清单描述当前示例部署，不表示任意新仓库都会自动安装这些算子。

```bash
curl http://localhost:8820/v1/operators
```

该接口和仓库首页显示实际已发布版本。`read_item@1.0` 是 DACP 节点内置的数据读取步骤，无需发布到仓库。源码树中的 Scala、Java、原生命令、常用算子集合及其他 PiFlow 移植代码属于可构建示例，不等于当前仓库已发布的能力。

## 端口、参数和执行方式

以下算子均使用输出端口 `result`。输出描述由 `infer_outputs(inputs, params)` 给出，清单只声明输入；执行作业时不自动调用推导方法。

| 算子 | 输入 → 输出 | 参数示例 | 执行方式与限制 |
|---|---|---|---|
| `piflow_csv_parser@1.0` | `source: bytes` → `records` | `{"header":true,"delimiter":","}` | 按批解析 UTF-8 CSV；列值均为字符串，不自动猜测类型。无表头示例：`{"header":false,"schema":"station_id,temperature,quality"}` |
| `piflow_filter@1.0` | `source: records` → `records` | `{"condition":"CAST(temperature AS REAL) >= 30"}` | 逐批筛选；使用 SQLite 条件表达式，保留输入 schema |
| `piflow_execute_sql_stop@1.0` | `source: records` → `records` | `{"viewName":"observations","sql":"SELECT station_id, COUNT(*) AS n FROM observations GROUP BY station_id"}` | 收集输入后执行 SQLite SQL；输入与输出均有限额，不是常量内存流算法 |
| `piflow_merge@1.0` | `left: records`, `right: records` → `records` | `{}` | 先输出 left，再输出 right；两侧有序 schema 必须完全一致；不去重、不排序 |
| `piflow_join@1.0` | `left: records`, `right: records` → `records` | `{"joinMode":"inner","correlationColumn":"station_id"}` | 收集两侧输入后关联；模式为 `inner`、`left`、`right`、`full_outer`；右侧同名非键列追加 `_right`，仍有歧义则报错 |
| `piflow_csv_save@1.0` | `source: records` → `bytes`（CSV） | `{"header":true,"delimiter":","}` | 输出 CSV 字节流，**不在服务器保存 CSV 文件**；仅支持标量列 |

SQL 与 Join 的输入收集上限为 128 MiB（实现会估算内存占用）；实际内存还包含 Arrow、Python 对象和算法开销。SQLite SQL 不等于 Spark SQL，不能直接使用任意 Spark 函数。具体契约和源码见 [PiFlow 适配说明](../examples/operators/piflow/README.md)、[specs.json](../examples/operators/piflow/specs.json) 与 [operators](../examples/operators/piflow/operators/)。

## 输入输出范例

CSV 输入：

```csv
station_id,temperature,quality
S001,32.5,0
S002,18.0,0
S003,35.0,1
```

经 CSV parser 后，records 的 schema 为三个字符串列。Filter 使用：

```json
{"condition":"quality = '0' AND CAST(temperature AS REAL) >= 30"}
```

输出仍是三个字符串列，仅保留 `S001`。经 CSV save 后得到：

```csv
station_id,temperature,quality
S001,32.5,0
```

需要数值列时，通过 SQL 显式转换，例如 `SELECT station_id, CAST(temperature AS REAL) AS temperature FROM observations`，不要假定 CSV parser 自动转换类型。

## Shell 使用

连接节点并选择包含 `/observations` 的气象数据集后：

```text
run piflow_csv_parser@1.0 --input source=/observations --view bytes
```

如果 Item 已提供 records 视图，可直接过滤：

```text
run piflow_filter@1.0 --input source=/observations --view records --params '{"condition":"CAST(temperature AS REAL) >= 30"}'
```

组合解析、过滤与 CSV 输出时，使用以下 Job。节点示例 URI 为 `dacp://localhost:18861/weather/observations`；提交 Union 时替换成实际目录返回的 Union 公开 URI，不使用成员内部 URI。省略 `node` 由服务器调度。

```yaml
job: High-temperature observations
tasks:
- id: read
  operator: read_item@1.0
  inputs: {}
  params:
    uri: $INPUT_URI
    view: bytes
- id: parse
  operator: piflow_csv_parser@1.0
  inputs:
    source: read
- id: filter
  operator: piflow_filter@1.0
  inputs:
    source: parse
  params:
    condition: "quality = '0' AND CAST(temperature AS REAL) >= 30"
- id: export
  operator: piflow_csv_save@1.0
  inputs:
    source: filter
outputs:
  result: export
```

保存为 `weather.yaml`，另建 `variables.json`：

```json
{"INPUT_URI":"dacp://localhost:18861/weather/observations"}
```

```text
submit weather.yaml --vars variables.json
```

默认结果通过 stream 返回；选定边界的物化、重试和超时由独立的[执行策略](materialization.md)控制。算子内部收集数据与引擎保存输出是不同机制。

## 发布与核对

仓库浏览、下载公开；发布需要 token。各算子的构建、测试和逐个上传命令见 [PiFlow 发布说明](../examples/operators/piflow/README.md#逐个验证发布)，HTTP 上传契约见[仓库手册](../dacp-operator-repository/README.md)。发布后先核对 `/v1/operators`，再核对对应版本的页面、参数、源码和许可证；源码集合的完整移植进度不能当作在线仓库清单。
