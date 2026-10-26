# In-process operator fusion

[中文](operator-fusion.md)

DACP automatically checks Job placement, operator environments and executor capabilities. When eligible, an entire node subjob uses one isolated container and one JVM or Python interpreter, running steps concurrently in the same process. Internal edges use bounded memory streams and backpressure without passing through the server or Flight and without intermediate result files. Job inputs and final results retain the existing Flight streaming protocol; submissions need no new switch.

Administrators can set `operator_fusion_enabled: false` in the node configuration to force isolated execution for benchmarks or troubleshooting. It defaults to `true` and applies to both ordinary Jobs and Union sub-Jobs. Execution history reports `disabled_by_configuration`.

## Eligibility

- A subjob needs at least two steps. All resolved runtime image SHA-256 IDs must match exactly; matching tags or JDK major versions is insufficient.
- Every step must use the same executor supporting `OperatorExecutor.groupCommand`. Mixed JVM, Python and native commands do not fuse.
- JVM operators must implement `dacp.operator.Operator`, including Scala `BytesOperator`, `ByteStreamOperator` and `RecordsOperator` templates. Eligibility inspects JAR class metadata without loading user classes in the server JVM.
- Python operators must explicitly define a thread-safe `process(ctx)` and share one `.py` file or wheel. Different wheel modules may provide different entrypoints. This restriction avoids packages overwriting each other's Python modules; shared dependencies remain in the common image.
- DEBUG inline source, ordinary container entrypoints and native executables do not fuse and retain their existing execution path.

When all steps of an ordinary Job share one node, that node attempts fusion. If the sole target is remote, the complete Job is delegated there for the decision. Union evaluates each member's subjob independently without moving raw data or changing placement. The coordinator first checks the graph obtained by contracting node regions: A → B → A introduces a region cycle and disables fusion, preventing downstream-input waits from blocking upstream startup.

An identical image is necessary but does not prove arbitrary programs can safely share an interpreter. Operators must not depend on process-global arguments, modify shared environment variables or write protocol stdout. Python `process(ctx)` must not modify `sys.argv`, `sys.stdout`, the working directory or mutable module state shared with other steps. Calling `System.exit` or `os._exit` terminates the entire subjob. Programs requiring process isolation should use an executor that does not advertise fusion capability.

## JVM operators

Existing SDK templates work directly. After building and publishing the Scala common-operator examples, `scala_text_uppercase@1.0` and `scala_text_strip@1.0` reference the same image and implement the SDK contract. They are not in the current six-operator collaboration repository; publish them before using this example:

```yaml
job: clean-text
tasks:
- id: read_uppercase_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://localhost:18861/data/message
    view: bytes
  node: dacp://localhost:18861
- id: uppercase
  title: Uppercase text
  node: dacp://localhost:18861
  operator: scala_text_uppercase@1.0
  inputs:
    source: read_uppercase_source
  params: {}
- id: trim
  title: Trim whitespace
  node: dacp://localhost:18861
  operator: scala_text_strip@1.0
  inputs:
    source: uppercase
  params: {}
outputs:
  result: trim
```

The target node must configure the operators' repository and the input Item must exist and be authorized. The fusion launcher carries the current SDK and scheduling code; Arrow, Jackson and Scala dependencies remain supplied by the image. Existing images need not preinstall the new fusion launcher.

## Python operators

Expose a module-level `process(ctx)` business function while retaining an independent execution entrypoint:

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

Steps can pass `params: {mode: upper}` and `params: {mode: lower}`. Each function receives its own context, fully consumes named inputs, and declares all output metadata before writing data. Output interfaces maintain successful end events. Package imports must have no execution side effects or launch tasks. Packages exposing only command-line `main` are not forced into fusion; publish a new version with `process(ctx)` to enable it. Repository versions cannot be overwritten, so regenerated bundles require a new version identifier.

## Status, limits and resources

Execution records and Union node snapshots include `execution_mode`: `shared_process` or `isolated_steps`, with `fusion_reason` explaining ineligibility. An ordinary coordinator delegating a complete remote Job records `delegated_job`; the actual mode is recorded at the target node. Step states and port traffic remain tracked separately, with zero remote payload for internal edges.

Fusion does not promise zero-copy execution: the SDK input adapter still uses Arrow IPC, processed within the same process rather than separate operator processes. Internal byte-channel buffers are 256 KiB and event queues hold at most two frames. Individual Arrow record-batch sizes remain operator-controlled. Item size limits of document operators remain unchanged.

Cancellation or any step failure terminates the entire shared environment, closes pipes and stops unfinished steps. Already executed operators are not retried, avoiding duplicate side effects. The server retains isolation, disabled networking, read-only filesystems and container resource limits. Union authorization and task-slot reservations still count steps; fusion does not loosen permissions or concurrency quotas.

Plugin contributors can implement `OperatorExecutor.groupCommand(OperatorGroup)`. Returning `None` declines fusion for that combination. A command may carry read-only startup resources in `files` and code `mounts` under `/operator/`. Core handles placement, pinned environment identity, lifecycle, external-port checks and cancellation. Plugins own language loading, internal port contracts, backpressure and step events. Registration still uses `META-INF/services/dacp.OperatorExecutor`, without core registry changes.

## Validation

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-sdk && sbt test)
.venv/bin/python scripts/test-python-group.py
.venv/bin/python scripts/test-operator-sdk.py
```

Tests cover streams larger than internal buffers, same-process PIDs, Arrow records and nulls, port-contract failures and unsupported-entrypoint fallback.

## Performance comparison

See the [streaming benchmark operators](../examples/operators/stream-benchmark/README_en.md) for a reproducible large-data comparison. The node configuration switch compares both strategies using identical images, code and Jobs, and retains every raw sample.
