# Operators for the shared-process benchmark

[中文](README_zh.md)

This example uses Scala `ByteStreamOperator` to convert ASCII text to upper or lower case in 64 KiB chunks, without buffering an entire Item. The benchmark script generates a Python wheel containing equivalent business logic and the current SDK.

## Run

From the repository root:

```bash
(cd examples/operators/stream-benchmark && sbt package)
(cd dacp-server && sbt stage)
.venv/bin/python scripts/benchmark-operator-fusion.py \
  --docker-prefix 'sudo -n -u cnic -g docker --' \
  --rows 1000000 4000000 --repeats 3 \
  --output examples/output/fusion-benchmark
```

Prerequisites: existing test CA/client/node certificates in `examples/certs`, a Python environment with PyArrow, accessible Docker, and local `dacp/operator-jvm:1.1` and `dacp/operator-python:1.0` images. Omit `--docker-prefix` when Docker is directly accessible. The output directory must not already exist.

The script starts two independent DACP servers on temporary ports with only `operator_fusion_enabled` differing; it does not modify running demo nodes. Three steps convert text to upper case, lower case, then upper case. Each row is 64 bytes: one million rows are 64,000,000 bytes and four million rows are 256,000,000 bytes. This measures a CSV text byte stream, with rows counted by newlines, rather than an Arrow record stream.

Original input files are generated on disk as dataset sources. Intermediate results use in-memory pipes (fusion) or memory queues, Flight and FIFOs (isolated execution), without saving ordinary result files. The client validates incrementally without saving final output; code bundles, parameters, logs and execution history still use files.

Each language and strategy gets one warmup. Each data size then runs three repetitions with alternating strategy order. Wall time starts at client Job submission and ends after draining the entire output stream. It includes TLS/Flight, operator resolution, container startup and cleanup, transformations, and client SHA-256/row validation; it excludes server startup, data generation, and warmups. Images are local, and filesystem caches are not explicitly cleared.

The output directory retains `results.json`, individual Job scripts, configurations, access logs, and execution history. The script validates output SHA-256, bytes, rows, actual execution strategy, and one container for fusion versus three for isolated execution. Test servers stop automatically when the script exits. Existing limits are 1 GiB per container: the three fused steps share that limit, while isolated steps each get 1 GiB. No additional CPU quota is set; other host load can affect results.

This benchmark covers three lightweight streaming transformations. Its speedup cannot be assumed for compute-intensive operators or cross-node networks. Python threads are also affected by the GIL and the behavior of individual extension libraries.

The repository accepts source only. Local builds in these examples are for development verification; use the [source submission and review workflow](../../../docs/operator-submission.md) for public publication.
