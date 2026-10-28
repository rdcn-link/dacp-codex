# PiFlow EcoFlow 对应算子

将 [PiFlow EcoFlow](https://github.com/cas-bigdatalab/piflow/tree/EcoFlow) 的算子适配为 DACP 算子。审阅基线固定为提交 `84de10436a0d382e0d8d2ce1ab3a82f71ebafc29`，该分支包含 67 个 `ConfigurableStop` 派生算子。完整清单、上游类名、源文件和 SHA-256 位于 [inventory.json](inventory.json)。只有通过对应契约测试的实现才能发布；清单中的 `pending` 不是已实现能力。

上游 Scala / Java 原文保存在 `upstream/`，许可证见 [LICENSE](LICENSE)。适配实现、修改说明与输入输出契约随每个算子的源码包提供。这里不是 PiFlow 工作流文件的兼容执行器；任务使用 DACP 自己的端口和参数合同。

当前协作部署仅发布 CSV parser、filter、execute SQL、merge、join、CSV save 这 6 个 `1.0` 算子。可直接使用的参数与工作流见[当前算子库](../../../docs/operator-catalog_zh.md)。下文描述完整源码工程和移植范围，不等于在线仓库已全部发布；实际状态以 `/v1/operators` 为准。

## 编写和使用

每个 Python 算子位于 `operators/`，实现 `process(ctx)`，使用 DACP SDK 读写 Arrow 流。发布名称采用 `piflow_` 前缀。普通单输入单输出端口为 `source` / `result`，双输入通常为 `left` / `right`；具体以 [specs.json](specs.json) 为准。参数示例和执行模式也记录在该文件。

例如，`piflow_select_field@1.0` 的参数为：

```json
{"columnNames":"station_id,temperature"}
```

输入为 `records`，包含 `station_id`、`temperature` 等列；输出仍为 `records`，仅保留指定列并按指定顺序排列。它逐批处理，保留空输入的输出模式。列名必须唯一且存在。`piflow_drop_field@1.0` 使用相同参数结构，删除指定列；与上游一样忽略不存在的列，但不允许删除全部列。

纯 Python 算子复用 `dacp/operator-python:1.0` 的固定镜像摘要。SDK 随代码包提供，不要求节点的旧运行环境预先安装 SDK。运行环境独立存储并按摘要共享；算子 wheel 只包含业务代码和小型 SDK，不重复携带 Python 或 Arrow。需要附加依赖的算子必须使用相应运行环境，不能依赖执行时联网安装。

## 逐个验证、发布

先准备一个包含 `identity_python/1.1/manifest.json` 及其 runtime blob 的本地目录。清空在线仓库时保留的备份可以作为该目录。以下命令在项目根目录执行：

```bash
.venv/bin/python examples/operators/piflow/test.py select_field
.venv/bin/python examples/operators/piflow/publish.py select_field \
  --runtime-catalog /path/to/runtime-catalog \

```

本地打包工具先执行对应测试，在 `examples/operators/piflow/target/catalog` 生成开发产物，不会公开发布。仓库发布采用下方的标准化源码提交流程，每次可提交一个算子。

`mode: streaming` 表示按 Arrow 批次增量处理；需要全局比较或全局状态的实现必须明确标为相应模式，并设置资源上限。Arrow 流接口不意味着所有算法都能在常量内存中执行。外部数据库、HDFS、Hive、SSH 等算子的完整集成验证还需要可访问的测试服务和节点授权。

## 移植范围与差异

[移植审阅表](review_zh.md) 列出全部 67 个上游算子的对应名称、状态和限制；[英文表](review.md) 同步维护。`inventory.json` 的状态记录移植工程进度，不能替代当前仓库的实际发布清单。参数、端口、执行器和执行模式见 `specs.json`。科学 Scala 算子的构建方式见 [spark/README.md](spark/README_zh.md)。

通用 SQL 和绘图数据算子使用 SQLite，不能直接套用所有 Spark SQL 函数。图表算子输出用于绘图的记录，不生成图片。`Fork` 固定输出 `left/right`，`Route` 固定输出 `result/matched/unmatched`。CSV 读取和 Excel 读取均输出字符串列，不自动猜测类型；Excel 空单元格为 null。文件保存类算子返回字节流，不要求服务端保存结果文件。算子输入输出示例见审阅表。

`MockData` 声明的 `source` 是触发输入，消费并丢弃其内容，再按 `schema/count` 生成记录。它不把现有数据改写成随机数据，输出与输入内容独立。`ReichsteinCheck` 采用直接的 Python 实现，避免为多次小样本统计启动 Spark 作业；明确修复分箱边界、阈值排序和无效阈值参与中位数的问题，并从估计样本中排除缺测值。

### Excel 依赖

Excel 算子复用 Python 运行环境，将固定版本的纯 Python 依赖及其许可证随代码包提供，不在执行时联网安装。构建前下载 wheel；发布脚本按照 `excel-dependencies.json` 校验 SHA-256：

```bash
mkdir -p examples/operators/piflow/target/excel-deps
.venv/bin/pip download --only-binary=:all: --no-deps \
  --dest examples/operators/piflow/target/excel-deps \
  openpyxl==3.1.5 et_xmlfile==2.0.0
```

Excel 仅支持 XLSX，不支持旧 XLS。写出会序列化临时工作簿，读取会缓存压缩文件；均不是常量内存的逐字节转换。限制输入及展开后文件为 128 MiB，读取范围最多一百万单元格，写出最多一百万行。可选择工作表和起始单元格；读取也支持矩形范围。日期输出为 ISO 文本，公式只读取已有缓存值，写出时以 `=` 开头的文本仍保存为文本。

```json
{"header":true,"dataAddress":"'Weather Station'!B2:C100"}
```

上例用于 `piflow_excel_read`；写出只接受起始单元格，例如 `"'Weather Station'!B2"`。Excel 参数中的 `header` 是 JSON 布尔值。

## 验证与资源

Python 发布现在依次执行 SDK 契约测试、构建、打包后容器测试、HTTP 上传；失败即停止发布。也可以单独重测已构建的包：

```bash
.venv/bin/python examples/operators/piflow/container_test.py select_field
.venv/bin/python examples/operators/piflow/spark/test.py add_day_id \
  --spark-home /tmp/dacp-piflow-spark --docker
```

测试覆盖跨批次记录、空输入、Unicode、CSV 引号及换行、多输入多输出、参数错误与科学算法数值样例，具体以每个算子的测试分支为准。`integration_test.py` 检查真实 Flight 提交、远程算子解析、三步 CSV 工作流和最终结果；其文件头说明临时测试节点配置。DACP 节点至少需要三个任务槽位以并行连接这三个步骤。

容器测试使用只读根目录、禁网、1 GiB 内存、128 进程/线程、512 MiB 临时空间。Spark 算子可能使用临时 shuffle 文件，Excel 写出可能使用临时工作簿文件；这些不属于持久化结果或 checkpoint。Python 有状态算法通常限制物化输入或状态为 128 MiB，但中间对象仍有额外内存开销。科学算子的 JAR 约 230 KB，Spark 共享运行环境约 413 MB；Python 运行环境独立共享，Excel 代码包额外包含约 270 KB 的压缩依赖。

清空旧仓库时已经将原内容移至 `.operator-backups/` 下的带时间戳目录，在线仓库仅保留本次逐项发布的集合。网站地址和本次实际发布情况以运行中的仓库为准；无需等到全部算子完成才下载使用。

可运行 `.venv/bin/python examples/operators/piflow/audit.py` 审计上游源文件摘要、在线发布清单、页面，以及代码、源码与许可证下载的文件名、长度和 SHA-256。

## 有界输出样例

所有 Python 适配算子均显式注册 `preview_outputs`。当前仓库的六个算子通过 SDK `preview_process`，只对上游有界样例执行其纯变换逻辑；CSV 保存算子返回字节流样例，不写文件。接口是必需的，缺失时明确报错。详情见[样例接口与限制](../../../docs/structure-inference_zh.md#必需的样例接口与现有算子)。从仓库根目录运行 `./.venv/bin/python scripts/test-operator-previews.py` 验证六个已部署算子的样例及大小限制。

仓库只接收源码，旧示例中的本地构建仅用于开发验证；公开发布采用[源码提交与审核流程](../../../docs/operator-submission_zh.md)。
