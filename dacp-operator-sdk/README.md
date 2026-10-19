# 算子 SDK

SDK 封装输入发现、参数读取、Arrow IPC、输出帧、结束事件和资源清理。单输入模板使用 `source` / `result`；多端口使用 `OperatorContext`。

## Python

在仓库根目录安装：

```bash
python -m pip install ./dacp-operator-sdk/python
```

```python
from dacp_operator import BytesOperator

class Identity(BytesOperator):
    def transform(self, chunk, params):
        return chunk

if __name__ == '__main__':
    Identity().main()
```

表格处理继承 `RecordsOperator`，实现 `transform(batch, params)`，返回 `pyarrow.RecordBatch` 或 `None`；改变列结构时覆盖 `output_schema(input_schema, params)`。业务参数通过字典读取，例如 `params.get('min_amount', 20)`。

[双输入、双输出示例](../examples/operators/python/split_orders/1.0/split_orders.py)直接使用上下文。Python 共享运行时 Dockerfile 已包含 SDK，需要从仓库根目录重新构建：

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
```

## Scala

先在 `dacp-plugin-api` 和 `dacp-operator-sdk` 目录分别执行 `sbt publishLocal`，再构建示例：

```bash
cd examples/operators/scala
sbt package
```

构建依赖为 `"io.github.rdcn-link" %% "dacp-operator-sdk" % "0.1.0-SNAPSHOT" % Provided`。共享 JVM 运行时通过 `/opt/lib/*` 提供 SDK，更新后需要重建运行时镜像。

```scala
import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.BytesOperator

object Identity extends BytesOperator {
  override def transform(chunk: Array[Byte], params: JsonNode): Array[Byte] = chunk
}
```

完整示例：[SimpleIdentity.scala](../examples/operators/scala/src/main/scala/example/SimpleIdentity.scala)。表格模板 `RecordsOperator` 实现 `transform(batch, params): Option[VectorSchemaRoot]`；默认保留输入 schema，修改列时覆盖 `outputSchema`。输入 root 由 SDK 复用；输出同步序列化，不接管用户创建的 root，创建者负责关闭它。

多端口继承 `Operator`，实现 `process(ctx)`：用 `ctx.inputNames`、`ctx.input(name).foreachRecords`、`ctx.recordsOutput(name, schema)` 或 bytes 对应方法读写。`ctx.params` 是 Jackson JSON 对象。

## 流契约

所有输入按端口名的字典序逐个读到 Arrow EOS；不要交替读取多个 FIFO。所有输出必须在首次写数据前声明；正常退出自动发送 end，异常退出不发送成功结束。空结果允许没有 data 帧。stdout 保留给协议，普通打印重定向到 stderr。每个 records 批次序列化后最多 8 MiB，大批次应由业务程序切分；bytes 输出自动切为最多 64 KiB。参数校验及有界内存的业务算法由作者负责。SDK 不提供通用 join、广播或流水线调度。

算子仍需 `operator.yaml` 声明运行时、入口、端口和参数，并按[编写指南](../docs/operator-authoring.md)打包上传。SDK 不改变仓库发布接口。
