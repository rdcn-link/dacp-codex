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

## Scala 连续字节流模板

`ByteStreamOperator` 使用标准 Java `InputStream` / `OutputStream`，适合接入压缩、摘要、解析等库。输入按需跨 Arrow 批次和字节块读取；输出缓冲区最多 64 KiB，每帧最多 64 KiB，不缓存完整 Item。它改善流式库的接入方式，但不是零复制承诺。

```scala
import java.io.{InputStream, OutputStream}
import java.util.zip.GZIPOutputStream
import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.ByteStreamOperator

object StreamGzip extends ByteStreamOperator {
  override def outputFormat: String = "gzip"
  override def transform(input: InputStream, output: OutputStream, params: JsonNode): Unit = {
    val gzip = new GZIPOutputStream(output)
    try input.transferTo(gzip) finally gzip.close()
  }
}
```

完整文件：[StreamGzip.scala](../examples/operators/scala/src/main/scala/example/StreamGzip.scala)。将算子定义的入口设为 `example.StreamGzip`，输入声明 `source` 的 `bytes` 表示，输出声明 `result` 的 `bytes` / `gzip`。参数仍通过 `params` 读取。算子程序必须读到输入 EOF；提前关闭或读取固定长度后未到 EOF 会失败，不会产生成功 end。输出 `flush()` 立即发布缓冲数据；正常返回自动 flush 并发送 end。关闭输出适配器不会关闭协议 stdout，因此可以安全地关闭 `GZIPOutputStream` 等包装器；异常退出不自动发布残留缓冲。

多端口作者也可使用 `ctx.input(name).byteStream()` 与 `ctx.bytesOutput(name, format).byteStream()`；仍需先声明全部输出，并按字典序完整消费每个输入。`BytesOperator` 是逐块数组模板，不把完整 Item 读入内存；它不保留业务记录边界。表格流使用已有的 `RecordsOperator`，按 `VectorSchemaRoot` 批处理并保留 schema，不应先转成字节再解析。

## 流契约

所有输入按端口名的字典序逐个读到 Arrow EOS；不要交替读取多个 FIFO。所有输出必须在首次写数据前声明；正常退出自动发送 end，异常退出不发送成功结束。空结果允许没有 data 帧。stdout 保留给协议，普通打印重定向到 stderr。每个 records 批次序列化后最多 8 MiB，大批次应由业务程序切分；bytes 输出自动切为最多 64 KiB。参数校验及有界内存的业务算法由作者负责。SDK 不提供通用 join、广播或流水线调度。

算子仍需 `operator.yaml` 声明运行时、入口、端口和参数，并按[编写指南](../docs/operator-authoring.md)打包上传。SDK 不改变仓库发布接口。

## 同进程 Job

JVM 模板实现 `Operator`，可由执行器在独立共享 JVM 内调用。Python 模板增加 `process(ctx)` 方法；模块可用顶层 `def process(ctx): MyOperator().process(ctx)` 暴露合并入口。上下文和输出由执行器管理，不要在该入口自行创建或关闭另一个上下文。资格、线程安全要求与示例见 [同进程合并执行](../docs/operator-fusion.md)。
