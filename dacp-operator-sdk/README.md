# Operator SDK

The SDK handles input discovery, parameter loading, Arrow IPC, output frames, end events and resource cleanup. Single-input templates use `source` / `result`; use `OperatorContext` for multiple ports.

## Python

Install from the repository root:

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

For tables, extend `RecordsOperator` and implement `transform(batch, params)`, returning a `pyarrow.RecordBatch` or `None`. Override `output_schema(input_schema, params)` when changing columns. Read business parameters from the dictionary, for example `params.get('min_amount', 20)`.

The [two-input, two-output example](../examples/operators/python/split_orders/1.0/split_orders.py) uses the context directly. The shared Python runtime Dockerfile includes the SDK; rebuild from the repository root:

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
```

## Scala

Run `sbt publishLocal` separately in `dacp-plugin-api` and `dacp-operator-sdk`, then build the example:

```bash
cd examples/operators/scala
sbt package
```

The build dependency is `"io.github.rdcn-link" %% "dacp-operator-sdk" % "0.1.0-SNAPSHOT" % Provided`. The shared JVM runtime supplies the SDK through `/opt/lib/*`; rebuild its image after updating.

```scala
import com.fasterxml.jackson.databind.JsonNode
import dacp.operator.BytesOperator

object Identity extends BytesOperator {
  override def transform(chunk: Array[Byte], params: JsonNode): Array[Byte] = chunk
}
```

Complete example: [SimpleIdentity.scala](../examples/operators/scala/src/main/scala/example/SimpleIdentity.scala). The table template `RecordsOperator` implements `transform(batch, params): Option[VectorSchemaRoot]`. It retains the input schema by default; override `outputSchema` when changing columns. The SDK reuses input roots. Output serialization is synchronous and does not take ownership of user-created roots; their creator must close them.

For multiple ports, extend `Operator` and implement `process(ctx)`. Use `ctx.inputNames`, `ctx.input(name).foreachRecords`, `ctx.recordsOutput(name, schema)` or their bytes counterparts. `ctx.params` is a Jackson JSON object.

## Scala continuous byte-stream template

`ByteStreamOperator` uses standard Java `InputStream` / `OutputStream`, suitable for compression, hashing and parsing libraries. Input reads cross Arrow batches and byte chunks on demand. The output buffer and each output frame are at most 64 KiB; the whole Item is never buffered. This simplifies integration with streaming libraries but does not promise zero-copy behavior.

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

Complete file: [StreamGzip.scala](../examples/operators/scala/src/main/scala/example/StreamGzip.scala). Set the operator entry point to `example.StreamGzip`, declare input `source` with representation `bytes` and output `result` with `bytes` / `gzip`. Read parameters through `params` as usual. Programs must read input through EOF. Closing early, or stopping after a fixed number of bytes without reaching EOF, fails without successful end events. Output `flush()` publishes buffered data immediately. Successful return automatically flushes and emits end. Closing the output adapter never closes protocol stdout, so wrappers such as `GZIPOutputStream` can safely be closed. Failure does not automatically publish remaining buffered data.

Multiport authors can also use `ctx.input(name).byteStream()` and `ctx.bytesOutput(name, format).byteStream()`. Declare every output before writing and fully consume each input in lexical order. `BytesOperator` is a chunked array template; it does not read the entire Item into memory or preserve business record boundaries. Table streams use the existing `RecordsOperator`, processing `VectorSchemaRoot` batches with their schema instead of converting them to bytes and parsing again.

## Stream contract

Consume inputs one at a time in lexical port-name order through Arrow EOS; do not alternate between FIFOs. Declare all outputs before the first data write. Successful exit automatically sends end events; failure sends no successful end. Empty outputs may have no data frames. stdout is reserved for the protocol; ordinary prints are redirected to stderr. Each serialized records batch must fit within 8 MiB; business code must split larger batches. Bytes output is automatically chunked to at most 64 KiB. Authors own parameter validation and bounded-memory business algorithms. The SDK does not provide general joins, broadcasting or pipeline scheduling.

Operators still need `operator.yaml` declaring runtime, entry point, ports and parameters, and must be packaged and uploaded as described in the [authoring guide](../docs/operator-authoring.md). The SDK does not change the repository publishing API.

## In-process Jobs

JVM templates implement `Operator` and can be invoked inside an isolated shared JVM. Python templates provide `process(ctx)`; modules can expose fusion through `def process(ctx): MyOperator().process(ctx)`. The executor owns context and output lifecycle; do not create or close another context in that entrypoint. See [operator fusion](../docs/operator-fusion.md) for eligibility, thread-safety requirements and examples.

## Structure inference

Every operator must implement `inferOutputs` (Scala) or `infer_outputs` (Python). Templates provide implementations; custom operators implement the method alongside processing. Sinks return an empty mapping. Manifests no longer declare outputs. See the [shared protocol and examples](../docs/structure-inference.md).

Required `previewOutputs` (JVM) / `preview_outputs` (Python) methods provide bounded output samples through inference workers. Default requests allow 10 rows / 16 KiB; both SDK and host enforce row counts and serialized UTF-8 JSON size. Custom operators must implement this interface; pure-transform templates may supply it. If current inputs cannot yield a sample, report the reason. Preview failures preserve Schema. See [sample interfaces and examples](../docs/structure-inference.md#bounded-output-sample-interface).
