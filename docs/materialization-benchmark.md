# Streaming versus per-step materialization

[中文](materialization-benchmark_zh.md)

Measured on 2026-10-26 with 500,000 weather observations: 18,500,050 input CSV bytes and 29,094,344 final Arrow payload bytes.

Four steps: A read_item → A CSV parser → B quality filter → C temperature filter. Three node processes share one physical host, loopback Flight and XFS on /dev/vdb. Each policy receives one warmup, followed by three alternating measured runs. Timing includes submission, scheduling, process startup, processing, transfer, complete result consumption and successful commit. There is no schema inference or retry.

| Policy | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) | Persisted result bytes per run |
|---|---:|---:|---:|---:|---:|
| Streaming | 32.247 | 26.517 | 23.827 | 26.517 | 0 |
| Materialize every step | 37.510 | 24.867 | 22.233 | 24.867 | 106,275,999 (4 committed steps) |

Speedup, defined as materialized median / streaming median, is **0.9378×**. This test did not observe a streaming speedup. Run-to-run variation is substantial; the approximately 6.6% duration difference does not establish a stable performance difference.

The current materialization implementation writes while transmitting and commits with fsync on completion. Downstream steps still process concurrently. This is not a baseline that writes each complete intermediate file before the next step reads it, and it does not establish a comparison with traditional file-based workflow engines.

All eight runs produced 500,000 rows and identical final-payload SHA-256 hashes. Zero persisted bytes refers to engine-managed result materialization, excluding the existing input file, logs and execution history.

[Raw measurements](benchmarks/materialization-20261026.json) include task IDs, workflow, policies, result hashes and committed steps. Input is UTF-8 CSV with the header `station_id,timestamp,temperature,humidity,quality`. Row i, starting at zero, is `S{i%1000:04d},2026-10-26T{i%24:02d}:00:00Z,{10+i%300/10:.1f},{40+i%60},0`, with LF line endings. To reproduce, replace public URIs and logical node names with your own deployment and publish the required [collaboration operators](operator-catalog.md).
