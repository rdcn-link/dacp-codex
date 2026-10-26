# PiFlow EcoFlow operator adaptations

Adapt operators from [PiFlow EcoFlow](https://github.com/cas-bigdatalab/piflow/tree/EcoFlow) to DACP. The review baseline is pinned to commit `84de10436a0d382e0d8d2ce1ab3a82f71ebafc29`; this branch contains 67 operators derived from `ConfigurableStop`. [inventory.json](inventory.json) records the complete inventory, upstream classes, source files, and SHA-256 digests. Only implementations passing their individual contract tests can be published; `pending` entries are not implemented capabilities.

Original Scala / Java sources are retained under `upstream/`; see [LICENSE](LICENSE). Each operator's source archive includes its adaptation, change descriptions, and input/output contracts. This is not a compatibility executor for PiFlow workflow files; tasks use DACP's own port and parameter contracts.

The current collaboration deployment publishes only CSV parser, filter, execute SQL, merge, join and CSV save at version `1.0`. See the [current catalog](../../../docs/operator-catalog_en.md) for runnable parameters and workflows. The sections below describe the full source project and adaptation scope, not a fully published online repository; `/v1/operators` is authoritative.

## Authoring and usage

Each Python operator under `operators/` implements `process(ctx)` and uses the DACP SDK to read and write Arrow streams. Published names have a `piflow_` prefix. Ordinary single-input/output ports are `source` / `result`, and two-input operators generally use `left` / `right`; [specs.json](specs.json) is authoritative. It also records example parameters and execution modes.

For example, `piflow_select_field@1.0` accepts:

```json
{"columnNames":"station_id,temperature"}
```

Its `records` input contains columns such as `station_id` and `temperature`; its `records` output contains only the selected columns, in the requested order. Processing is incremental by batch, and empty inputs retain an output schema. Column names must be unique and present. `piflow_drop_field@1.0` uses the same parameter structure to remove columns; like upstream, absent columns are ignored, but removing every column is rejected.

Pure Python operators reuse the pinned image digest of `dacp/operator-python:1.0`. The SDK is bundled with the code, so older node runtimes need not have it preinstalled. Runtime blobs are stored separately and shared by digest; operator wheels contain only business logic and the small SDK, without duplicating Python or Arrow. Operators requiring additional dependencies must use an appropriate runtime rather than install packages over the network during execution.

## Verify and publish individually

Prepare a local directory containing `identity_python/1.1/manifest.json` and its runtime blob. The backup retained when clearing the live repository can serve as this directory. Run from the project root:

```bash
.venv/bin/python examples/operators/piflow/test.py select_field
.venv/bin/python examples/operators/piflow/publish.py select_field \
  --runtime-catalog /path/to/runtime-catalog \
  --publish http://localhost:8820
```

Publication runs the corresponding test first, builds the wheel, source ZIP, and manifest, then uploads through the token-authenticated HTTP API. The token defaults to `.operator-catalog/.publish-token`; override it with `--token-file`. Tokens are never printed. Without `--publish`, artifacts are built locally under `examples/operators/piflow/target/catalog`. Each invocation handles one operator without waiting for the complete collection.

`mode: streaming` means incremental processing by Arrow batch. Implementations requiring global comparisons or state must declare an appropriate mode and resource limits. An Arrow streaming interface does not mean every algorithm executes in constant memory. Complete integration verification of external database, HDFS, Hive, and SSH operators additionally requires reachable test services and node authorization.

## Scope and semantic differences

The [port review](review_en.md) lists all 67 upstream operators, corresponding names, status, and limitations; the [Chinese review](review.md) is maintained alongside it. States in `inventory.json` track adaptation work and do not replace the actual inventory of the current repository. Parameters, ports, executors, and execution modes are in `specs.json`. Build scientific Scala operators using [spark/README_en.md](spark/README_en.md).

Generic SQL and chart-data operators use SQLite, so not every Spark SQL function is supported. Chart operators return records for plotting, without rendering images. `Fork` has fixed `left/right` outputs; `Route` has fixed `result/matched/unmatched` outputs. CSV and Excel readers return string columns without type inference; empty Excel cells become null. File-writing operators return byte streams rather than requiring server-side result files. Input/output examples are in the review.

`MockData` declares `source` as a trigger input: its contents are consumed and discarded before generating records according to `schema/count`. Output is independent of input contents. `ReichsteinCheck` uses a direct Python implementation to avoid Spark jobs for repeated small-sample statistics. It explicitly fixes bin boundaries, deterministic threshold ordering, and invalid thresholds entering the median, and excludes missing values from estimation samples.

### Excel dependencies

Excel operators reuse the Python runtime and bundle pinned pure-Python dependencies and their licenses with the code. Execution never installs packages over the network. Download the wheels before building; publication verifies SHA-256 against `excel-dependencies.json`:

```bash
mkdir -p examples/operators/piflow/target/excel-deps
.venv/bin/pip download --only-binary=:all: --no-deps \
  --dest examples/operators/piflow/target/excel-deps \
  openpyxl==3.1.5 et_xmlfile==2.0.0
```

Excel supports XLSX, not legacy XLS. Writing serializes a temporary workbook, and reading buffers the compressed file; neither is a constant-memory byte transformation. Input and expanded files are limited to 128 MiB, selected read ranges to one million cells, and writes to one million rows. Sheet/start-cell selection is supported; reads also support rectangular ranges. Dates become ISO text, formulas use existing cached results only, and text beginning with `=` remains text on export.

```json
{"header":true,"dataAddress":"'Weather Station'!B2:C100"}
```

This example is for `piflow_excel_read`; writing accepts only a start cell, such as `"'Weather Station'!B2"`. Excel's `header` parameter is a JSON boolean.

## Verification and resources

Python publication now runs SDK contract tests, builds the package, runs packaged-container tests, and uploads over HTTP in that order. Any failure stops publication. Built packages can also be tested independently:

```bash
.venv/bin/python examples/operators/piflow/container_test.py select_field
.venv/bin/python examples/operators/piflow/spark/test.py add_day_id \
  --spark-home /tmp/dacp-piflow-spark --docker
```

Fixtures cover cross-batch records, empty input, Unicode, quoted CSV/newlines, multiple inputs/outputs, invalid parameters, and scientific numeric examples; coverage varies by operator's test branch. `integration_test.py` checks real Flight submission, remote operator resolution, a three-step CSV workflow, and final output. Its header documents the temporary node setup. At least three task slots are needed to connect those steps concurrently.

Container tests use a read-only root, no network, 1 GiB memory, 128 processes/threads, and 512 MiB temporary space. Spark operators may create temporary shuffle files, and Excel export may create temporary workbook files; these are neither durable results nor checkpoints. Stateful Python algorithms typically limit materialized inputs or state to 128 MiB, with additional memory overhead for intermediate objects. Scientific JARs are approximately 230 KB, sharing a Spark runtime of approximately 413 MB. Python runtimes are shared independently; Excel code includes approximately 270 KB of compressed dependencies.

The previous repository contents were moved into a timestamped directory under `.operator-backups/` before clearing the live catalog. The live repository contains only this individually published collection. Consult the running repository for its address and current publication status; operators can be downloaded as soon as each one is published.

Run `.venv/bin/python examples/operators/piflow/audit.py` to audit upstream source hashes, live publication inventory, pages, and filenames, lengths, and SHA-256 of code, source, and license downloads.
