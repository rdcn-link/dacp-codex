# Scala scientific operator runtime

EcoFlow scientific operators retain the upstream Scala / Spark algorithms and connect to the DACP SDK through a private adapter. Spark dependencies are not added to DACP core modules. Each operator is compiled, tested, and published individually as a small JAR, sharing one runtime pinned by image digest: JDK 17, Scala 2.13.16, Spark 4.0.1, and dependencies from that distribution. Generic Python operators continue to use the smaller Python runtime.

## Build

From the project root, prepare the SDK and API JARs and obtain the pinned Spark distribution contents:

```bash
(cd dacp-plugin-api && sbt package)
(cd dacp-operator-sdk && sbt package)
.venv/bin/python -m pip download --no-deps --dest /tmp/dacp-piflow-deps pyspark==4.0.1
mkdir -p /tmp/dacp-piflow-spark
tar -xzf /tmp/dacp-piflow-deps/pyspark-4.0.1.tar.gz \
  -C /tmp/dacp-piflow-spark --strip-components=1
.venv/bin/python examples/operators/piflow/spark/build-runtime.py \
  --spark-home /tmp/dacp-piflow-spark
.venv/bin/python examples/operators/piflow/spark/publish.py add_day_id \
  --spark-home /tmp/dacp-piflow-spark
```

The current local helper scripts invoke Docker through `sudo -n -u cnic -g docker` to match permissions on the demo host; adjust that prefix to the administrator's setup on other machines. The runtime is built using `jlink` from the host JDK 17 and Spark JARs, without pulling a base image. It retains the `java.beans` classes Spark needs from `java.desktop`, but omits graphical native libraries and supports headless computation only. Publication requires access to the local repository token file.

## Contracts and limits

- Inputs are named `records` streams; the output is a `records` stream on `result`. See the parent directory's `specs.json` for port mappings and example parameters.
- The Spark adapter reads complete inputs before execution, with a limit of 128 MiB or one million records per input. This is not a constant-memory streaming algorithm. Outputs return in batches; reusable Spark rows are copied before partition collection to prevent overwritten records.
- Spark runs with `local[2]`, UTC, no Web UI, and ANSI mode disabled to approximate upstream SQL behavior. A shared runtime does not imply automatic sharing of a SparkSession across workflow tasks.
- Before publication, each operator is tested with the same constraints as node execution: no network, read-only root, 1 GiB memory, 128 processes/threads, and a 512 MiB non-executable temporary filesystem. Spark may create execution and shuffle files there; these are not retained as recoverable results.
- Original upstream files remain unchanged. Builds apply reviewable Scala 2.13 / Spark 4 adaptations, including `StringBuilder.toString.stripSuffix`, stable `spark.implicits` imports, immutable snapshots for `Row.fromSeq`, and the mathematical PI constant import.
- Passing execution fixtures does not certify a scientific model's validity. Actual analyses still require checking units, missing values, parameter ranges, and applicable data conditions. Operator-specific fixes and verified numeric behavior must be recorded individually.

The current adapter disables adaptive queries and automatic broadcast joins, limits RPC/shuffle threads, and reduces EC partitions from twenty to two to lower container thread pressure. See the [port review](../review_en.md) for corrections and unported items. Reichstein uses the Python implementation in the parent directory and is not built with this Spark builder.
