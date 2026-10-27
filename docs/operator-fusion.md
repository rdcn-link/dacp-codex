# 算子同进程合并执行

[English](operator-fusion_en.md)

DACP 会自动检查 Job 的执行位置、算子环境和执行器能力。满足条件时，一个节点上的整个子 Job 使用一个隔离容器、一个 JVM 或 Python 解释器，步骤在同进程内并发执行。内部边使用有界内存流和背压，不经服务器或 Flight 转发，也不生成中间结果文件。入口和最终结果仍使用原有 Flight 流协议，提交的 Job 无须增加开关。

管理员可在节点配置中设置 `operator_fusion_enabled: false`，强制独立执行，用于性能对比或故障排查；默认值为 `true`。该开关同时作用于普通 Job 和 Union 子 Job，执行记录中的原因是 `disabled_by_configuration`。

## 合并条件

- 子 Job 至少有两个步骤，所有步骤解析后的运行环境镜像 SHA-256 必须完全相同。镜像标签、JDK 主版本相同并不足够。
- 所有步骤使用同一个支持 `OperatorExecutor.groupCommand` 的执行器。混合 JVM / Python / 原生命令不合并。
- JVM 算子必须实现 `dacp.operator.Operator`，包括 Scala 的 `BytesOperator`、`ByteStreamOperator` 和 `RecordsOperator` 模板。通过 JAR 类元数据检查能力，不在服务器 JVM 加载用户类。
- Python 算子必须显式定义线程安全的 `process(ctx)`，并使用同一个 `.py` 或 wheel 代码包。wheel 中的各模块可提供不同入口。该限制避免多个包互相覆盖 Python 模块；依赖仍由共同的镜像提供。
- 不合并 DEBUG 内联源码、普通容器入口或原生可执行程序。它们仍按原有方式执行。

普通 Job 的所有步骤在同一节点时，直接在该节点尝试合并；如果指定的唯一节点是远端，整个 Job 交给目标节点判断。Union 为每个成员节点的子 Job 分别判断，不移动原始数据或改变任务节点。协调器会先检查节点区域收缩后的依赖图：例如 A → B → A 会形成区域循环，禁止合并，以免等待下游输入时阻塞上游启动。

同一镜像只是必要条件，不能证明任何程序都能安全地放在同一个解释器里。算子不得依赖进程级全局参数、修改共享环境变量或写协议标准输出；Python `process(ctx)` 不得修改 `sys.argv`、`sys.stdout`、全局工作目录或被其他步骤共享的可变模块状态。调用 `System.exit` / `os._exit` 会终止整个子 Job。需要进程隔离的程序应使用不声明合并能力的执行器。

## JVM 算子

现有 SDK 模板可直接使用。例如，先构建并发布 Scala 常用算子集中的 `scala_text_uppercase@1.0` 和 `scala_text_strip@1.0` 后，两者 引用相同镜像，均实现 SDK 契约，可按以下方式组成 Job。它们不在当前协作示例的六算子仓库中，不能直接假定可用：

```yaml
name: clean-text
tasks:
- id: read_uppercase_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://localhost:18861/data/message
    view: bytes
  node: dacp://localhost:18861
- id: uppercase
  title: 转大写
  node: dacp://localhost:18861
  operator: scala_text_uppercase@1.0
  inputs:
    source: read_uppercase_source
  params: {}
- id: trim
  title: 去除首尾空白
  node: dacp://localhost:18861
  operator: scala_text_strip@1.0
  inputs:
    source: uppercase
  params: {}
outputs:
  result: trim
```

目标节点必须配置这些算子的仓库地址，输入 Item 也必须实际存在并获准访问。合并启动器携带当前 SDK 和调度代码，Arrow / Jackson / Scala 等依赖继续复用镜像；不要求已有镜像预装新的合并启动器。

## Python 算子

将业务函数作为模块顶层的 `process(ctx)` 暴露，同时保留独立执行入口：

```python
from dacp_operator import OperatorContext


def process(ctx):
    output = ctx.bytes_output('result', 'text')
    for chunk in ctx.input('source').bytes():
        output.write(chunk.upper() if ctx.params['mode'] == 'upper' else chunk.lower())


if __name__ == '__main__':
    with OperatorContext() as ctx:
        process(ctx)
```

不同步骤可传入 `params: {mode: upper}` 和 `params: {mode: lower}`。`process` 接收独立上下文，按命名端口消费全部输入、声明全部输出元数据后再写数据；输出接口自动维护成功结束事件。包内导入应无执行副作用，不得在导入时启动任务。仅提供命令行 `main` 的旧代码包不会被强行合并；发布含 `process(ctx)` 的新版本后才具备能力。仓库版本不可覆盖，重新生成代码包时须使用新的版本标识。

## 状态、限制与资源

执行记录和 Union 节点快照包含 `execution_mode`，可为 `shared_process` 或 `isolated_steps`，并通过 `fusion_reason` 解释未合并原因。普通协调端转交整个远端 Job 时记录 `delegated_job`，实际执行模式在目标节点记录。步骤状态和端口流量仍分别统计，内部流量的跨节点字节数为零。

合并不承诺所有步骤零拷贝：当前仍使用 Arrow IPC 适配 SDK 输入，但在进程内完成，不经过独立算子进程。字节流的内部通道缓冲区为 256 KiB，事件队列最多两个帧；单个 Arrow 记录批的大小仍由算子控制。文档类算子自身的 Item 大小限制不变。

取消或任一步骤失败会终止整个共享执行环境，关闭管道并停止其他未完成步骤，不重试已运行的算子，避免重复副作用。隔离、无网络、只读文件系统、容器资源限制仍由服务器负责。Union 的授权和任务槽预留仍按步骤计数，不因合并放宽权限或并发配额。

插件贡献者可实现 `OperatorExecutor.groupCommand(OperatorGroup)`；返回 `None` 表示该组合不具备同进程能力。返回命令可附带只读启动资源 `files` 和 `/operator/` 下的代码挂载 `mounts`。核心负责位置、固定环境身份、生命周期、外部端口校验和取消；插件负责语言加载、内部端口契约、背压和步骤事件。注册仍使用 `META-INF/services/dacp.OperatorExecutor`，无需修改核心执行器注册表。

## 验证

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-sdk && sbt test)
.venv/bin/python scripts/test-python-group.py
.venv/bin/python scripts/test-operator-sdk.py
```

测试覆盖超过内部缓冲容量的数据、同进程 PID、Arrow 记录及空值、端口契约失败和不支持的入口回退。

## 性能对比

可复现的大数据对比见[流式性能测试算子](../examples/operators/stream-benchmark/README.md)。测试通过节点配置开关对同一镜像、代码和 Job 比较两种策略，并保留逐次原始数据。
