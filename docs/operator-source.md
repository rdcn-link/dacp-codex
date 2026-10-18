# DEBUG 源码算子

Python shell 和任务 API 已支持内嵌源码。服务端 YAML 配置必须显式设置 `mode: DEBUG`；默认值为 `NORMAL`。此开关控制代码执行权限，与日志级别无关。协调节点和每个执行节点均检查此限制，包括直接 worker 请求。NORMAL 模式仍支持仓库算子。

配置固定的运行时白名单：

```yaml
mode: DEBUG
source_runtimes:
  python@1.0: {executor: python, image: 'dacp/operator-python:1.0'}
  scala@1.0: {executor: jvm, image: 'dacp/operator-scala-debug:1.0'}
```

镜像必须预先安装到执行节点的 Docker 中。服务端将镜像解析为本地不可变 ID。提交的源码不能选择镜像、安装依赖或更换执行插件；依赖应由管理员加入运行时镜像。源码和编译产物均为临时资源。编译与执行使用同一个隔离容器：禁止网络、根文件系统只读、`/tmp` 有容量限制，并限制内存与进程数。DEBUG 模式面向通过 mTLS 认证的可信开发客户端。

在 `dacp-server` 目录运行 `sbt 'run ../examples/source-debug.yaml'` 可启动示例：端口为 18815，`data1` 数据集发布 `/orders`。Python 运行时构建方法见[算子编写指南](operator-authoring.md)。Scala 源码需要编译器镜像：先在项目根目录运行 `scripts/build-operator-image.sh` 构建共享 JVM 运行时，再运行 `scripts/build-scala-source-runtime.sh`。后者将 Scala compiler/reflect 2.13.16 和 JDK 编译模块加入独立的 DEBUG 镜像。普通 JAR 执行镜像无需编译器。

使用示例客户端证书连接 shell 到 `dacp://localhost:18815/data1`，以 heredoc 输入源码：

```text
run --runtime python@1.0 --input source=/orders --params '{"output_format":"csv"}' <<END
def process(inputs, params):
    for chunk in inputs['source']:
        yield chunk.upper()
END
```

源码输入始终使用 heredoc，不提供 `--source FILE` 或 `--code`。结束标记须为标识符，并在结束行独占一行。缩进与换行保持原样。Ctrl-C 或 EOF 中止输入且不提交。源码上限 512 KiB，完整任务请求上限 1 MiB。

Python 插件调用 `process(inputs, params)`。`inputs` 将各命名端口映射为流式迭代器：bytes 视图产生 `bytes`，records 视图产生 PyArrow `RecordBatch`。请按端口名称的字典序消费输入。函数向唯一输出端口 `result` 产生字节块或记录批次；同一输出流的表示与记录 schema 必须一致。空输出表示为空字节流。`params.output_format` 设置输出格式标签，默认为 `binary`。可用 `--view records` 请求记录输入。`print` 诊断写入 stderr。shell 按记录表格、已知文本格式或其他字节的十六进制形式逐块显示全部结果。Ctrl-C 关闭结果流并取消任务；若输出后发生错误，shell 标注结果不完整。不保存输出数据集。

Scala 源码是包含 main 类的完整编译单元：

```text
run --runtime scala@1.0 --entrypoint Uppercase --input source=/orders <<END
// 在此粘贴 examples/operators/inline/Uppercase.scala 的完整内容。
END
```

[完整 Scala 示例](../examples/operators/inline/Uppercase.scala) 从 `/input/manifest.json` 读取命名 Arrow 输入，并向 stdout 写入协议帧。main 参数为 `operator /input /input/params.json`。stdout 专用于协议帧，诊断写入 stderr。每帧先写两个大端 32 位整数，分别为 JSON 头与载荷长度，然后写入 JSON 头及载荷。对 `result` 依次发送 `start`、零个或多个 `data`、`end`。记录流的每个 data 帧包含一个独立 Arrow IPC 流；start 包含 base64 编码的 Arrow schema。头最大 64 KiB，载荷最大 8 MiB。编译诊断写入 stderr，编译失败则任务失败；编译期间不下载依赖。

任务 YAML 与 SDK 使用相同源码表示：

```yaml
job: uppercase
tasks:
  - id: uppercase
    operator:
      runtime: python@1.0
      source: |
        def process(inputs, params):
            for chunk in inputs['source']:
                yield chunk.upper()
    inputs:
      source: {uri: 'dacp://localhost:18815/data1/orders', view: bytes}
    params: {output_format: csv}
```

SDK 使用 `with Job.run(node, yaml_text, **credentials) as events:`，迭代事件消费流，退出上下文时关闭。`run identity@1.1 --input source=/orders` 仍选择仓库算子；算子 ID 与内嵌源码互斥。

语言启动插件实现 `OperatorExecutor.sourceCommand`，通过 `META-INF/services/dacp.OperatorExecutor` 注册，与制品启动器共用发现机制。默认实现拒绝源码执行，因此仅开启 DEBUG 不会自动使任意插件接受源码。管理员在 `source_runtimes` 选择插件 ID；新增语言无需修改核心注册表或客户端。

预览：`run` 和 `submit` 支持 `--preview-rows N`、`--preview-bytes N`，按端口计数。首次达到限制即取消整个作业剩余执行。默认打印完整流；records 表头只显示一次，文本行带端口标签。
