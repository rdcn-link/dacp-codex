# Shared-process execution measurements (2026-10-20)

[中文](operator-fusion-benchmark.md)

Local measurements of three lightweight text transformations: upper case → lower case → upper case. Data, code, parameters, runtime images and the Flight client are identical; only the node's `operator_fusion_enabled` setting differs. Scala/JVM and Python were measured separately. Each strategy received one warmup, followed by three measurements per data size with alternating execution order. The table reports median end-to-end time in seconds.

| Runtime | Rows | Input MiB | Isolated (s) | Shared process (s) | Time reduction | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Scala/JVM | 1,000,000 | 61.04 | 48.29 | 14.18 | 70.6% | 3.41× |
| Scala/JVM | 4,000,000 | 244.14 | 60.02 | 13.75 | 77.1% | 4.37× |
| Python | 1,000,000 | 61.04 | 40.66 | 11.84 | 70.9% | 3.43× |
| Python | 4,000,000 | 244.14 | 83.04 | 14.93 | 82.0% | 5.56× |

Docker create events confirmed 3 containers per isolated Job versus 1 per fused Job. All 24 measured samples validated the full output SHA-256, bytes, rows and actual execution mode, with identical output. One million rows are 64,000,000 bytes; four million rows are 256,000,000 bytes. Each row is 64 bytes of numbered CSV text.

## Raw wall times

`fused` means shared process; `isolated` means separate processes. Each row lists the three measurements in seconds, excluding warmups.

| Runtime | Rows | Strategy | Three measurements (s) |
|---|---:|---|---|
| jvm | 1,000,000 | fused | 12.60 / 25.10 / 14.18 |
| jvm | 1,000,000 | isolated | 48.29 / 51.84 / 37.16 |
| jvm | 4,000,000 | fused | 11.17 / 26.27 / 13.75 |
| jvm | 4,000,000 | isolated | 60.02 / 57.28 / 67.36 |
| python | 1,000,000 | fused | 27.12 / 11.16 / 11.84 |
| python | 1,000,000 | isolated | 35.20 / 49.85 / 40.66 |
| python | 4,000,000 | fused | 17.74 / 14.11 / 14.93 |
| python | 4,000,000 | isolated | 83.04 / 90.08 / 62.80 |

## First-data latency

Median time from submission to the first data frame reaching the client, in seconds. This is not total Job completion time.

| Runtime | Rows | Isolated (s) | Shared process (s) |
|---|---:|---:|---:|
| jvm | 1,000,000 | 36.34 | 10.14 |
| jvm | 4,000,000 | 17.65 | 6.27 |
| python | 1,000,000 | 27.11 | 6.70 |
| python | 4,000,000 | 29.58 | 8.11 |

## Conditions and limits

- Linux x86-64 host with 16 logical CPUs and approximately 31 GiB RAM, shared with demo services without CPU isolation. Fixed image IDs are in the raw JSON.
- Timing includes client submission, TLS/Flight, operator resolution, container startup and cleanup, complete stream consumption and client validation. It excludes data generation, node startup, image downloads and warmups. Input file caches were not explicitly evicted.
- Input files were saved on the node in advance; operators process chunks. Fused intermediate data uses bounded memory pipes; isolated execution uses memory queues, local Flight and FIFOs, without saving intermediate results. The client validates incrementally without saving final output. Code bundles, configuration, logs and execution history still use files.
- This measures a CSV-text byte stream rather than an Arrow record stream, and does not measure cross-node networks. Existing limits are 1 GiB per container: three fused steps share that limit; each isolated step gets its own container.
- Samples vary substantially. Sizes were run sequentially, so caches, JIT and host load affect absolute times. Some larger-data samples completing sooner do not imply that processing more data is intrinsically faster.
- These are end-to-end results. The gains cannot all be attributed to pure IPC, nor assumed for compute-intensive operators, different dependencies or Python operators sensitive to the GIL.

[Reproduction commands and operator code](../examples/operators/stream-benchmark/README_en.md) · [Complete sampling JSON](benchmarks/operator-fusion-20261020.json)
