# DEBUG 源码算子

[English](operator-source.md)

仅当协调端和执行节点均配置 `mode: DEBUG` 时，DACP 才接受内联 Python 或 Scala 源码。NORMAL 模式仍可运行普通仓库算子。源码执行面向具有作业执行权限的可信开发者，不绕过认证或运行时限制。

## 配置运行时

```yaml
mode: DEBUG
source_runtimes:
  python@1.0: {executor: python, image: 'xstream/operator-python:1.0'}
  scala@1.0: {executor: jvm, image: 'xstream/operator-scala-debug:1.0'}
```

管理员在每个执行节点安装这些镜像。提交的源码不能选择任意镜像或安装依赖。执行器在配置的隔离与资源限制内编译和运行源码。参见 [XStream 执行器指南](https://github.com/rdcn-link/xstream-codex/blob/main/plugins/xstream-plugin-operator-executors/README_zh.md)和[算子开发指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-authoring_zh.md)。

## 从 Shell 提交

```text
run --runtime python@1.0 --input source=/orders --params '{"output_format":"csv"}' <<END
def process(inputs, params):
    for chunk in inputs['source']:
        yield chunk.upper()
END
```

源码始终通过 heredoc 输入，结束标记必须单独占一行。源码限制为 512 KiB，完整作业请求限制为 1 MiB。`--view records` 选择记录输入，此时 Python 输入迭代器产生 PyArrow 记录批次而非字节块。通过 `result` 端口产生相应的输出块，诊断信息写入标准错误。

Scala 源码是完整编译单元，通过 `--entrypoint` 选择主类，参考 [Scala 示例](https://github.com/rdcn-link/xstream-codex/blob/main/examples/operators/inline/Uppercase.scala)。启动器传入 `operator /input /input/params.json`。使用 XStream 算子 SDK 管理流帧和生命周期，不手工构造传输帧。编译依赖必须预先安装在选定镜像中。

Shell 增量接收输出，Ctrl-C 取消执行。`--preview-rows` 和 `--preview-bytes` 可限制预览，不创建结果 Dataset。同一步骤的源码与普通仓库算子互斥。执行器实现 `OperatorExecutor.sourceCommand`，通过 `META-INF/services/xstream.execution.OperatorExecutor` 注册；默认拒绝源码执行。
