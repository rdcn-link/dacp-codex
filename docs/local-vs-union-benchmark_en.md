# Local versus cross-node streaming job benchmark

[中文](local-vs-union-benchmark.md)

Measured on 2026-10-27: median local duration was **21.86 seconds**, versus **23.33 seconds** across nodes, an observed **6.7%** increase (local relative speedup: 1.067). All three nodes and Union ran on one physical host; these results do not represent cross-site network performance.

## Method

The input was a 500,000-row weather observation CSV containing 18,500,050 bytes. Both modes used `read_item → piflow_csv_parser → piflow_filter → piflow_csv_save`. Local execution placed every step on A. Cross-node execution read and parsed on A, filtered on B, and exported on C; the client received results through Union. The filter was `quality = '0' AND CAST(temperature AS REAL) >= 30`.

The execution policy prohibited materialization. Intermediate results flowed between steps; the client counted and hashed chunks without saving output payloads. The input file already existed on disk, and execution history and logs were still persisted. Timing covered client submission through result-stream completion, including scheduling, runtime startup, processing and transfer.

Each mode had one excluded warmup followed by three alternating measured rounds.

| Round | Local (s) | Cross-node (s) |
|---|---:|---:|
| 1 | 85.313 | 36.682 |
| 2 | 21.790 | 23.331 |
| 3 | 21.858 | 20.360 |
| Median | 21.858 | 23.331 |

Warmups took 96.206 seconds locally and 107.385 seconds across nodes. The first measured round remained substantially slower, indicating startup or cache variability. With this limited sample, 6.7% is a workload-specific observation rather than a fixed cross-node overhead.

Every run produced 166,600 rows and 6,164,250 bytes with the same SHA-256:

```text
4d39d4dde925bd0bf3f5a856e583df880d8bd5b63949f524e48a3afbf99b58de
```

## Reproduction

Use [benchmark-local-vs-union.py](../scripts/benchmark-local-vs-union.py); `--help` lists its arguments. Prepare a CSV Item with the fields above and provide the node and Union Item URIs, three member names, CA, and separate Token files. The script writes both workflow YAML files, policy YAML, variable YAML, per-round records and a summary JSON. Defaults are one warmup and three measured rounds per mode. Credentials are excluded from result files.
