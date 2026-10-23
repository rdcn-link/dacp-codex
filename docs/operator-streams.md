# 基于 Item 的算子输入与纯流式输出

已通过 Flight DoExchange、Python Shell/SDK 和 Scala SDK 实现。当前端口支持 bytes 和 records；其他科学数据表示需要明确扩展契约。每个输出端口最多有一个下游消费者，需要复制时使用独立端口。多个输入端口按名称字典序消费。
源算子可以声明 `inputs: {}`，任务也显式填写 `inputs: {}`；不需要虚构触发 Item。输出至少包含一个命名端口。Shell 直接使用 `run live_ticks@1.0 --params '{"max_records":3}'`，省略 `--input`。本地任务默认在所连接节点执行，也可显式指定 `node`。


## 用户使用契约

所有算子输出都是流。不提供保存文件选项、结果 Dataset 模式或结果模式选择。Shell 和 SDK 直接消费流；算子不发布输出 Dataset，也不返回持久化 Dataset URI。YAML 作业可描述流水线，其中输出名称表示流之间的连接，而不是已保存的数据集。

Shell 用法：

```text
run scala_identity@1.0 --input source=/reports/orders
run format_convert@1.1 --input source=/reports/orders --params '{"compression":"zstd"}'
submit pipeline.yaml
```

dacp-shell 已实现这些命令。不需要 `--result`、文件目标或已保存的输出 Dataset。

## 算子输入输出单位

每个算子端口接收或产生一个通过流传输的数据 Item。算子可以声明多个命名输入和输出端口。Dataset 是用于定位 Item 的命名空间或集合，不是隐式输入端口，也不是持久化结果目标。

```text
convert:
  inputs:  source -> 一个 CSV Item
  outputs: result -> 一个 Parquet Item 流

join:
  inputs:  left -> 一个表 Item, right -> 一个表 Item
  outputs: result -> 一个表 Item 流
```

每个端口声明接受或产生的表示，以及适用的 schema/格式约束。Item 可以是表、文件、图像、结构化科学对象或多维数组。以 Item 为单位不要求展开结构、物化整个对象或让它完全放入内存。新表示必须先定义公共契约再实现。

输入绑定指定一个 Item、其公开支持的表示和接收端口。流水线绑定指定上游命名输出。明确拒绝仅指定 Dataset、缺少必需端口、重复绑定、不支持的表示及 schema 不匹配。不得隐式选择所有 Item、合并内容或重复调用算子。Shell Item 编号在提交前解析为当前列表中的明确路径。

Dataset 范围的处理属于作业编排：

- 将明确选定的兼容 Item 映射为独立算子调用。
- 将明确选定的 Item 绑定到声明端口，例如关联的 `left` 和 `right`。

Map 必须显式请求，不能从 Dataset 选择推断。实现前需要说明选择、并发、顺序和失败策略。多 Item 聚合需要专门设计的输入表示或端口契约，不能使用任意 Dataset 参数代替。

输出同样以 Item 为单位。每个命名输出都是带元数据和内容的 Item 流；多个输出不会自动形成持久化 Dataset。作业连接输出和下游输入端口。Dataset Item 路径标识源，输出端口名称标识传输中的结果，不分配公开 Dataset 名称。

## 显示和生命周期

流携带输出名称和明确的内容元数据：表示、格式及适用的 schema。记录显示为表格，文本按文本显示，未知二进制按增量十六进制行显示。数组、图像和分组科学对象必须通过有文档的表示保留结构，不能静默展开成记录。

从用户角度，`run` 同步执行：提交后等待流元数据，随批次或块到达增量输出，不先收集完整结果。所有输出成功结束、执行失败或用户中断后才返回提示符。首个结果到达不等于执行成功；停滞请求仍受执行超时约束。

每个 records 输出只显示一次列名，随后按批次追加行。文本解码保留跨块字符。二进制使用每行 16 字节的十六进制表示，偏移量按端口计算。多个输出显示端口名称和元数据；Flight 信封明确标注端口。显示缓存保持有界。

默认消费并显示完整流。预览限制需要显式指定，不能静默应用于 `run`。Ctrl-C 取消请求及上游执行、释放资源并返回提示符。浏览时的默认预览限制不影响算子执行。

流只消费一次，不是可浏览的 Dataset，消费后不能重新打开。只有算子成功退出且流正常终止后才显示完成。算子输出部分数据后失败时，报告已经显示的数据不完整。

有界预览停止消费并取消剩余请求，不在后台继续计算。断开连接和中断必须把取消传递给生产者、上游 Flight 读取和算子容器。慢消费者通过有界缓存施加背压。命名输出共享带帧交换，可能交错到达。

## 内部临时存储

不创建用户可见输出文件或持久化结果 Dataset。算子或格式仍可能需要有界临时空间，例如完成 Parquet 元数据。临时空间在成功、失败或取消时清理，不是另一种结果目标。纯流式交付不意味着所有格式都能立即输出有用字节。

## 传输和调度限制

`OperatorRequest` 通过 Flight DoExchange 选择作业 YAML 或内部任务 JSON。每批有一个 binary `data` 单元，内容为帧：大端 32 位头长度、大端 32 位载荷长度、JSON 头和载荷。头最多 64 KiB，载荷最多 8 MiB。事件为 `start`、`data`、`end` 和最终 `complete`。records 起始事件包含 IPC schema；每个 records 数据帧包含恰好一个批次的独立 Arrow IPC 流。bytes 保持原字节。执行成功且所有公开端口正常终止后发送完成。

算子通过 Arrow IPC FIFO 接收命名输入，向 stdout 写帧，向 stderr 写诊断。内置算子和外部执行器使用相同契约。没有输出 Dataset、输出 TTL、旧输出目录或结果模式选择器。

每个输出最多一个消费者。多个输入按名称字典序消费。尚未实现通用流水线调度、扇出、Dataset 映射、重试、检查点和重放。bytes 和 records 已实现，其他表示需要明确契约。

## Shell 预览控制

`run ... --preview-rows 20` 限制每端口记录数，`run ... --preview-bytes 4096` 限制每端口字节数。`submit pipeline.yaml` 支持相同参数。首先到达的限制取消整个剩余作业，报告对应端口，不报告执行成功。不指定这些参数时显示所有结果。浏览限制不影响作业。

## SDK 资源所有权

Python `Job.run` 返回支持上下文管理的 `JobStream`。Scala `Job.run` 返回 `AutoCloseable` 的 `OutputEvent` 迭代器，应使用 `Using.resource`。两者在耗尽或失败时释放传输资源；提前退出必须显式关闭以取消执行。Scala 事件字节数组在迭代推进或关闭后仍有效；从数组创建的 records 读取器需要单独关闭。见 [Scala SDK](../dacp-client-scala/README.md) 和 [内联源码示例](operator-source.md)。
