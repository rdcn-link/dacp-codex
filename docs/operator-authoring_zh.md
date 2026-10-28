# 编写、发布和运行算子

算子必须在 `inputs` 之外声明静态 `parameters` 契约，详见[参数类型与表单](operator-parameters_zh.md)。输出结构仍只由推导方法给出。
输出结构统一由必选推导方法提供，清单声明输入与参数契约；详见[结构推导](structure-inference_zh.md)。

算子是一个数据处理程序：接收命名的 Item 输入流和一份 JSON 业务参数，处理后返回命名输出流。下面先用一个完整的 Python 示例串起开发过程，再说明 Scala、打包发布和底层协议。

## 优先使用算子模板

新算子建议使用 [Scala / Python SDK](../dacp-operator-sdk/README_zh.md)，只实现业务转换。下面保留底层协议示例，供需要直接控制帧的作者参考。

Scala 的连续字节流模板可直接接入 Java 流库，参见 [ByteStreamOperator 与 GZIP 示例](../dacp-operator-sdk/README_zh.md#scala-连续字节流模板)。表格流使用 `RecordsOperator` 按 Arrow 批次处理。

## 启动参数的具体含义

例如参数文件 `/input/params.json` 包含 `{"min_amount":20,"factor":2}`，Python 入口的等效调用为（实际启动器还会包一层加载程序）：

```bash
/usr/local/bin/python /operator/filter_amount.py operator /input /input/params.json
```

`sys.argv[1:]` 是 `['operator', '/input', '/input/params.json']`。`operator` 是固定协议标记，第二项是流目录，第三项是参数文件路径，不是 JSON 字符串。Scala 的 `main(args)` 接收同样三项，例如：

```bash
/opt/java/bin/java --add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true -cp '/operator/scala-identity_2.13-1.0.jar:/opt/lib/*' example.ScalaIdentity operator /input /input/params.json
```

## 多输入与多输出

完整例子：[split_orders](../examples/operators/python/split_orders/1.0/split_orders.py)、[端口定义](../examples/operators/python/split_orders/1.0/operator.yaml)、[作业文件](../examples/operators/python/split_orders/1.0/job.yaml)。两个输入 `left` / `right` 均为 `id:int64, amount:float64`；`min_amount=20` 时，示例的 `accepted` 输出 id 2、3，`rejected` 输出 id 1、4。

manifest 由框架生成，必须按端口查找，不要硬编码文件编号：

```json
[{"port":"left","file":"0.arrow","representation":"records","format":"csv"},{"port":"right","file":"1.arrow","representation":"records","format":"csv"}]
```

Shell 绑定示例：

```text
run split_orders@1.0 --input left=/orders_a --input right=/orders_b --view records --output accepted --output rejected --params '{"min_amount":20}'
```

`--view` 应用于全部输入；不同输入需要不同表示时使用作业 YAML。每个输出端口可有独立 schema，必须先声明所有输出再写数据，空输出也需要 start/end。SDK 自动处理这些帧。输入按端口名排序逐个消费到 EOS，避免交替读 FIFO 导致阻塞。Shell 返回所选端口 `accepted` / `rejected`；作业 YAML 的 `outputs` 可以改成 `qualified` / `other` 等标签。每个输出仅支持一个消费者，复制需显式声明不同端口；DEBUG 内联 Python 帮助器仍只有 `result`，native 执行器仍只有单一 bytes stdin/stdout。


## 先分清输入数据、参数和输出

| 内容 | 示例 | 程序如何获取 |
|---|---|---|
| 输入端口 | `source` | 从 `/input/manifest.json` 找到对应流文件 |
| 输入 Item | 当前 Dataset 中的 `/orders` | 框架读取此 Item，以 Arrow IPC 流送入算子，不把源文件路径交给程序 |
| 输入表示 | `records` | manifest 中的 `representation`；表格按 Arrow 记录批次读取 |
| 业务参数 | `{"min_amount":20,"factor":2}` | 读取启动参数指定的 JSON 文件，通常是 `/input/params.json` |
| 输出端口 | `result` | 程序在 stdout 的协议帧中标明端口名称 |
| 输出表示与格式 | `records` / `arrow` | `start` 帧声明表示、格式和 schema，后续 `data` 帧传记录批次 |

`--input` 绑定数据，`--params` 传业务设置，两者不能混为一谈。端口名称由算子定义；`source`、`result` 是本文的选择，不是所有算子的固定名称。每个端口承载一个 Item 流，一个程序可以声明多个端口。

`representation` 决定如何读取：`records` 是有类型的表格，`bytes` 是文件或二进制内容。`format` 描述内容格式，例如 `csv`、`parquet`、`text`、`arrow`。同一 CSV Item 可以选择原始 bytes 或解析后的 records；records 的传输编码始终是 Arrow IPC，不是 CSV 文本。**Shell 的 `run` 默认使用 bytes；本文表格算子必须指定 `--view records`。**

## 示例目标：筛选订单并增加计算列

完整文件在 [filter_amount/1.0](../examples/operators/python/filter_amount/1.0/filter_amount.py)，包括[定义](../examples/operators/python/filter_amount/1.0/operator.yaml)、[输入数据](../examples/operators/python/filter_amount/1.0/orders.csv)和[作业](../examples/operators/python/filter_amount/1.0/job.yaml)。

输入 CSV：

```csv
id,amount
1,12.5
2,30.0
3,60.0
```

本例要求 `id: int64`、`amount: float64`。参数 `min_amount` 决定筛选阈值，`factor` 决定乘数：

```json
{"min_amount": 20, "factor": 2}
```

处理规则是保留 `amount >= min_amount`，输出 `id`、`amount` 和新列 `scaled_amount = amount * factor`。因此结果是：

| id | amount | scaled_amount |
|---|---|---|
| 2 | 30.0 | 60.0 |
| 3 | 60.0 | 120.0 |

两个参数都可省略，默认分别为 `0.0` 和 `1.0`；必须是有限数字，拒绝布尔、字符串和未知参数。空 amount 不匹配；没有匹配行时仍输出三列 schema，然后正常结束。计算使用 float64，不是财务定点金额算法。

## 第一步：定义端口

```yaml
name: filter_amount
version: '1.0'
description: Filter amount >= min_amount and append scaled_amount = amount * factor.
image: dacp/operator-python:1.0
executor: python
entrypoint: filter_amount
artifact: filter_amount.py
inputs:
  source:
    representations: [records]
    formats: []
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties:
    min_amount:
      type: number
      default: 0
      description: Minimum order amount.
    factor:
      type: number
      default: 1
      description: Amount scaling factor.
  required: []
  additionalProperties: false
```

`source` 只接受 records，`formats: []` 表示不限制输入格式；程序仍必须校验列和类型。`result` 输出 records，格式标为 arrow。也可在端口定义中声明 Arrow schema JSON 值，见[端口契约](reference_zh.md#任务执行与算子)。业务参数值写在 Job 的 `params` 中，类型和约束由算子定义的 `parameters` 声明；程序仍负责业务校验。

## 第二步：读取参数和输入批次

启动器向程序传入 `operator /input /input/params.json`。Python 的 `sys.argv[1]` 为 `operator`，`sys.argv[2]` 为输入目录，`sys.argv[3]` 为参数文件。不要把业务 JSON 当成位置参数直接解析。

框架生成的 manifest 类似下面这样；`file` 是框架分配的文件名，不能硬编码：

```json
[
  {"port":"source","file":"source.arrow","representation":"records","format":"csv"}
]
```

运行时该文件是 FIFO，内容为 Arrow IPC 流，程序可以这样读取：

```python
params = read_params(Path(sys.argv[3]))
directory = Path(sys.argv[2])
manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
item = manifest[0]  # 本例先验证只有一个名为 source 的 records 输入
with (directory / item['file']).open('rb') as raw:
    with pa.ipc.open_stream(raw) as reader:
        for batch in reader:
            selected = batch.filter(
                pc.greater_equal(batch.column('amount'), params['min_amount']))
```

这里的 `read_params`、导入和完整校验均在[完整程序](../examples/operators/python/filter_amount/1.0/filter_amount.py)中。程序按批次处理，不收集完整 Dataset。停止于 Arrow EOS，不等待 FIFO 的操作系统 EOF。多个输入必须按端口名称字典序消费，例如 `left` 再 `right`。

## 第三步：声明输出并发送批次

输出不是 `print(table)` 或直接往 stdout 写 CSV。Python/Scala 算子在 stdout 写协议帧；普通文本日志写 stderr。records 输出的 schema 在开始前确定，本例为：

```python
OUTPUT_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('amount', pa.float64()),
    ('scaled_amount', pa.float64()),
])
```

完整程序先发送 `start`，再按批次输出 `data`，最后发送 `end`：

```python
emit('start', representation='records', format='arrow',
     schema=base64.b64encode(OUTPUT_SCHEMA.serialize().to_pybytes()).decode('ascii'))

# selected 是本批保留下来的行；没有行时跳过 data。
output = pa.record_batch([
    selected.column('id'), selected.column('amount'),
    pc.multiply(selected.column('amount'), params['factor']),
], schema=OUTPUT_SCHEMA)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, OUTPUT_SCHEMA) as writer:
    writer.write_batch(output)
emit('data', sink.getvalue().to_pybytes())

# 所有输入消费、所有批次输出完成后执行一次。
emit('end')
```

`emit` 的完整实现在示例文件中。它把头和载荷封装为长度前缀帧，所有帧标记 `port: result`。每个 records data 载荷是一个独立的、恰好包含一个批次的 Arrow IPC 流，不是 JSON 行，也不是把多个 IPC 流简单当作一个流追加。输出批次 schema 必须与 start 一致。

本例先把输入批次切成最多 512 行的小批次，避免生成过大的输出帧。空结果只发送 start/end。框架在程序成功退出并校验全部输出端口结束后，才向客户端报告任务完成；算子自身不发送最终 `complete` 事件。

## 第四步：本地验证，再发布

从仓库根运行本地验证，不需要启动 DACP 或 Docker；Python 环境需已安装 PyArrow：

```bash
python3 scripts/test-operator-authoring.py
```

该脚本用普通 Arrow IPC 文件替代 FIFO，验证分批输入、参数、空值、预期结果、输出 schema、空结果和无效参数。它验证算子协议，不代替容器、Flight 和授权集成测试。

构建共享运行环境并发布：

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish filter_amount@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint filter_amount \
  --artifact examples/operators/python/filter_amount/1.0/filter_amount.py \
  --ports examples/operators/python/filter_amount/1.0/operator.yaml \
  --root .operator-catalog
```

依赖预装在运行时镜像，执行时不联网安装。版本不可覆盖；修改已发布代码时发布新版本。

## 第五步：绑定数据并运行

先让 DACP 节点发布示例 CSV。将以下定义合并到节点现有配置中，并把文件路径改为本机绝对路径；监听、TLS、授权等配置仍需完整：

```yaml
datasets:
  data1:
    provider: mixed
    items:
      /orders:
        file: /absolute/path/to/dacp-codex/examples/operators/python/filter_amount/1.0/orders.csv
        schema: {id: int64, amount: float64}
remote_operator_library_urls: [http://localhost:8820]
```

仓库需要通过 HTTP 提供 `.operator-catalog`，启动命令见后文。通过已配置证书的 Shell 连接此 Dataset：

```bash
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

在 Shell 提示符中输入：

```text
run filter_amount@1.0 --input source=/orders --view records --params '{"min_amount":20,"factor":2}'
```

Shell 等待结果，随批次到达显示前面的两行结果，结束后显示执行信息并返回提示符。以下作业表达相同绑定，也适合为多个输入分别选择 view：

```yaml
name: filter-amount-demo
tasks:
- id: read_filter_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://localhost:8815/data1/orders
    view: records
- id: filter
  title: Filter orders and calculate scaled amounts
  operator: filter_amount@1.0
  inputs:
    source: read_filter_source
  params:
    min_amount: 20
    factor: 2
outputs:
  result: filter
```

```text
submit examples/operators/python/filter_amount/1.0/job.yaml
```

`id` 用于任务间依赖引用，`title` 用于展示；顶层 `outputs.result` 指定向客户端公开哪个任务端口。所有输出是一次性流，不自动保存到服务器，也不会变成 Dataset。取消消费会取消上游执行；已经显示的数据在后续失败时属于部分结果。

## bytes 输出应该是什么样

文本、压缩文件、图像等可选择 bytes。比如输入原始 UTF-8 `hello\n`，输出大写文本时，逻辑输出为：

```text
HELLO
```

对应 start 头为 `{"type":"start","port":"result","representation":"bytes","format":"text"}`；data 的载荷直接是 `b'HELLO\n'`，不再套 Arrow IPC。注意输入 bytes 仍通过 Arrow IPC 的 `data: binary` 列传入，这与输出帧载荷不同。Shell 可增量显示文本，未知二进制显示十六进制；多个块不保证按行或 UTF-8 字符边界划分。

原生 stdin/stdout 程序可使用 [command 执行器](native-commands_zh.md)，由适配器处理协议帧；Python/JVM 执行器则要求程序自己实现协议。已有的[字节恒等 Scala 程序](../examples/operators/scala/src/main/scala/example/ScalaIdentity.scala)和[Python 恒等程序](../examples/operators/python/identity_python/1.0/identity.py)可作为起点。

## Scala 示例

完整的 [ScalaIdentity.scala](../examples/operators/scala/src/main/scala/example/ScalaIdentity.scala) 使用 [build.sbt](../examples/operators/scala/build.sbt) 独立编译为薄 JAR。它消费 `source` 字节输入，并向 `result` 保留原始字节块和格式。[Uppercase.scala](../examples/operators/inline/Uppercase.scala) 演示相同协议下的数据转换。

在项目根目录运行：

```bash
scripts/build-operator-image.sh
(cd examples/operators/scala && sbt package)
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --image dacp/operator-jvm:1.1 --executor jvm --entrypoint example.ScalaIdentity \
  --artifact examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  --ports examples/operators/scala/operators/scala_identity/1.0/operator.yaml \
  --root .operator-catalog
```

构建与发布需要 JDK 17、sbt、Linux 和 Docker 权限。普通 JAR 执行无需在运行时内安装编译器。共享依赖标为 `Provided`，仅打包应用代码与运行时缺少的依赖。多个 main 类可共用同一个 JAR 和运行时。

## Python 示例

完整的 [identity.py](../examples/operators/python/identity_python/1.0/identity.py) 保留字节块或 Arrow 记录批次。共享运行时 [Dockerfile](../examples/operators/python/Dockerfile) 使用 Python 3.12 和 PyArrow 23.0.1。

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_python@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact examples/operators/python/identity_python/1.0/identity.py \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

wheel 使用示例 [pyproject.toml](../examples/operators/python/pyproject.toml)：

```bash
python -m pip wheel --no-deps --no-build-isolation --wheel-dir /tmp/dacp-wheels examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact /tmp/dacp-wheels/dacp_example_ops-1.0-py3-none-any.whl \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

预先安装本地构建工具。wheel 使用 `--no-index --no-deps` 离线安装到临时 `/tmp`，pip 输出写入 stderr。运行依赖由镜像提供。不同模块可共用同一个 wheel 和运行时。

## 端口声明与批量发布

必须提供 `publish --ports FILE`；该文件可为包含 `icon`、`inputs` 和 `parameters` 的 YAML 或 JSON（不允许声明 `outputs`，输出由必选推导方法给出），也可直接使用完整算子定义：

```yaml
name: identity_python
version: '1.0'
image: dacp/operator-python:1.0
executor: python
entrypoint: identity
artifact: identity.py
inputs:
  source:
    representations: [bytes, records]
    formats: []
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

每个端口声明表示（`bytes`、`records`）、格式限制（`[]` 表示不限），以及可选的 Arrow schema JSON 值。任务必须准确绑定所有声明的输入端口。定义文件位于 `<directory>/<name>/<version>/operator.yaml`，制品路径相对于定义文件；直接 CLI 制品路径相对于当前目录。container 执行器不设置 artifact 与 entrypoint，由镜像入口实现相同协议。

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/python --root .operator-catalog
```

`publish` 拒绝覆盖已有名称与版本；`seed` 验证已有不可变条目。名称与版本须匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。运行时归档和代码按摘要验证并共享。提供已有仓库的下载服务不需要 Docker。

## 传输与执行

通过管理员批准的渠道（例如 `scp`）传输 JAR、源码或 wheel 以及端口定义。若缺少运行时，先以 `docker save` 传输一次，并在仓库主机使用 `docker load` 导入。发布按已安装镜像的不可变 ID 导出。然后启动仓库：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve \
  --root .operator-catalog --host 0.0.0.0 --port 8820
```

DACP 节点配置 `remote_operator_library_urls: [http://repository-host:8820]`，或将定义与代码放入 `local_operator_library`。连接 shell 后运行：

```text
run identity_python@1.0 --input source=/reports/orders
run scala_identity@1.0 --input source=/reports/orders
```

shell 等待并逐块显示结果，不保存输出数据集。另见[流契约](operator-streams_zh.md)与 [DEBUG heredoc 源码](operator-source_zh.md)。

## 上传到远程仓库

仓库仅接受源码，由仓库完成扫描与隔离构建，再由管理员指定多个分类和最终可信度并批准公开。开发者不提交最终产物、不指定分类；必须声明 `source_public`。完整提交 YAML、源码目录、命令及审核流程见[源码提交指南](operator-submission_zh.md)。本指南中的本地打包用于开发验证，不会使算子自动公开。

```bash
python scripts/submit-operator.py submission.yaml --source sources.zip \
  --url https://repository.example.org --token-file /secure/submitter.token
```

## 协议限制与排错

帧布局为两个大端 32 位长度（JSON 头长度、载荷长度），随后是 UTF-8 JSON 头和载荷。头最多 64 KiB，载荷最多 8 MiB。每个端口发送 start、零或多个 data、end；多个输出必须先发送全部 start，再发送任何 data。start 可包含 base64 Arrow IPC schema 消息，records 必须提供。输入 manifest 也可能携带 schema，但实际 reader schema 仍需校验。

常见问题：

- 提示不支持输入表示：检查 `--view records`、端口声明以及 Item 公开的 views。
- 参数错误：检查 `--params` 是否为 JSON 对象，以及参数类型和拼写；框架不会替程序校验业务参数。
- 协议解析失败：检查 stdout 是否混入 `print`/日志；所有诊断写 stderr。
- 输出 schema 不一致：所有批次使用 start 声明的类型、列名和顺序。
- 程序不结束：输入使用 Arrow stream reader 的 EOS，不要继续读原始 FIFO 等待 EOF。
- 发送 end 后仍失败：end 只结束一个端口，程序还需正常退出；非零退出表示失败，已显示结果属于部分结果。

容器根和输入只读，临时文件放 `/tmp`；容器禁用网络，并限制内存、PID、临时空间和执行期限。依赖需预置。需要 JAR/源码/wheel 的小型发布时复用共享运行环境；不将算子加载进服务器 JVM。仓库算子可在 NORMAL/DEBUG 执行，内联源码仅允许 DEBUG。多维数组、图像等新表示必须先定义共享契约，不得静默展开为记录。

算子还必须提供 `icon`；公开 `categories` 由管理员指定，规则和发布示例见[分类与图标](operator-presentation_zh.md)。

## 输出样例接口

除了结构推导，算子必须实现输出样例接口。接口定义、Python／JVM 示例、行数与字节数限制，以及现有六个算子的实现行为，参见[结构推导与样例接口](structure-inference_zh.md#必需的样例接口与现有算子)。样例应反映算子的实际转换，不能直接复用不相符的上游样例；不得为预览触发生产副作用。
