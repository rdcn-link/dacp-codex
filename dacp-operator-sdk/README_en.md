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

## Stream contract

Consume inputs one at a time in lexical port-name order through Arrow EOS; do not alternate between FIFOs. Declare all outputs before the first data write. Successful exit automatically sends end events; failure sends no successful end. Empty outputs may have no data frames. stdout is reserved for the protocol; ordinary prints are redirected to stderr. Each serialized records batch must fit within 8 MiB; business code must split larger batches. Bytes output is automatically chunked to at most 64 KiB. Authors own parameter validation and bounded-memory business algorithms. The SDK does not provide general joins, broadcasting or pipeline scheduling.

Operators still need `operator.yaml` declaring runtime, entry point, ports and parameters, and must be packaged and uploaded as described in the [authoring guide](../docs/operator-authoring_en.md). The SDK does not change the repository publishing API.
