# DACP operator repository

A standalone Scala HTTP/HTTPS repository with public search, version pages, and downloads. Operators have immutable `name@version` manifests, shared runtime images, and optional small JAR, Python source, or wheel artifacts. Runtime and code blobs are stored once per SHA-256, even when several operators reference them. Serving needs no Docker; publishing runtime images needs the Docker CLI/engine. Build with JDK 17, Scala 2.13, and sbt 1.10.x.

The Maven artifact and executable are `dacp-operator-repository` (no Scala suffix); source package is `dacp.repository`.

## Starter catalog

From the project root:

```bash
scripts/setup-operator-repository.sh --root .operator-catalog --host 0.0.0.0 --port 8820
curl http://localhost:8820/v1/operators
```

Setup builds the shared `dacp/operator-jvm:1.1` environment and optional complete `dacp/format-convert:1.1` image, stages the repository, seeds operators, and starts serving. It requires Linux, JDK 17 with `jlink`, sbt, Python 3, `ldd`, Docker engine access, and Maven access on the first build. The default catalog is `.operator-catalog` under the project root. Existing versions and shared blobs are verified on repeated setup; versions cannot be overwritten. Additional arguments go to `serve`, including HTTPS options.

| Operator | Purpose |
|---|---|
| `format_convert@1.1` | Convert CSV/NDJSON to Parquet; see [parameters](../docs/reference_en.md#format_convert-parameters) |
| `identity@1.1` | Copy bytes for transfer checks and pipelines |

Both use the same thin Scala JAR and runtime environment. The repository exports the runtime only once. See the [runtime builder](../dacp-operator-runtime/README_en.md), [user-defined Scala example](../examples/operators/scala/README_en.md), [Python example](../examples/operators/python/README_en.md), and [packaging contracts](../docs/operator-packaging.md).

## Publication and execution

```bash
# From the project root; images must already be installed.
(cd dacp-operator-repository && sbt test stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`seed` discovers `<operators>/<name>/<version>/operator.yaml`. Definitions require `name`, `version`, and `image`; optional fields are `description`, `executor` (default `container`), `entrypoint`, and `artifact` (relative file path). Names/versions match `[A-Za-z0-9_-][A-Za-z0-9_.-]*`. Unknown fields, duplicate keys, mismatched directories, and empty definition directories fail.

Manual thin-JAR publication:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@1 --root .operator-catalog --image example/jvm-arrow:1 --executor jvm --entrypoint example.Summary --artifact summary.jar --ports summary-ports.yaml --description "Summarize records"
```

For Python, use `--executor python --entrypoint my_operators.summary --artifact my_operators-1.0-py3-none-any.whl`, or a `.py` script. Dependencies belong in the shared runtime. For a complete container, omit `--executor`, `--entrypoint`, and `--artifact`; its image entrypoint implements the [Arrow IPC contract](../docs/reference_en.md#task-execution-and-operators).

Publication pins an image ID, exports each image once, stores blobs by digest, and atomically publishes metadata. Locks coordinate publishers. New names/main classes can share one code bundle without editing the server. The repository stores any valid executor ID; each node must install its corresponding launch plugin. `container`, `jvm`, and `python` ship through the same ServiceLoader contract as external plugins.

Configure each execution node:

```yaml
local_operator_library: ../operators
remote_operator_library_urls:
  - http://library-host:8820
  - https://another-library.example.org
operator_cache: /var/lib/dacp/operators
operator_max_bytes: 8589934592
```

Tasks retain references such as `operator: format_convert@1.1`. Resolution checks local definitions/installed images, completed caches, then remote repositories in order. Only manifest HTTP 404 advances to another repository. All remote/cached blobs are verified for size and SHA-256; images use pinned IDs. Blobs are shared across operators and repositories; completed caches work offline. Limits apply per artifact. See [cache layout and limitations](../docs/operator-packaging.md#content-addressed-storage).

## Public website and HTTP API

Open `http://localhost:8820/`. Anyone can search names, versions, and descriptions, view versions, and download code and runtimes without signing in. Pages show separate sizes/checksums, executor, entrypoint, runtime image ID, and task references. All text is escaped. Pages work without JavaScript or external assets. Publication is an administrator CLI action; there is no HTTP upload endpoint.

| GET / HEAD endpoint | Response |
|---|---|
| `/` or `/?q=keyword` | Public catalog; case-insensitive search, maximum 200 characters |
| `/operators/<name>/<version>` | Operator details and separate artifact downloads |
| `/assets/repository.css` | Local stylesheet |
| `/v1/operators` | JSON array of manifests sorted by name/version |
| `/v1/operators/<name>/<version>` | Manifest JSON, schema `api_version: 2` |
| `/v1/blobs/sha256/<64 lowercase hex digits>` | Shared runtime archive or code bytes |
| `/health` | `ok` followed by a newline |

Unknown paths/missing artifacts return 404; writes return 405. HEAD supplies Content-Length. Blob attachments use their digest as the HTTP filename; HTML download attributes suggest the artifact filename. Paths cannot follow symlinks outside the catalog. Eight workers use a bounded queue. Staging directories are excluded from listing. Catalog reads metadata without hashing large blobs.

Metadata lives in `<root>/<name>/<version>/manifest.json`; blobs in `<root>/blobs/sha256/<digest>`; the runtime export index in `<root>/images/<image-ID>.json`. See the [manifest schema](../docs/operator-packaging.md#content-addressed-storage). This unreleased project directly uses the new schema, without old-schema readers. Use a fresh development catalog for the new format.

## HTTPS and operation

`serve` defaults to `127.0.0.1:8820`; use `--host 0.0.0.0` for external access. `--root` is required. The staged executable runs without sbt. Stop with Ctrl-C or SIGTERM. Catalog storage must support file locks and atomic moves, and only administrators should write to it.

```bash
export DACP_REPOSITORY_TLS_PASSWORD='your-keystore-password'
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820 --tls-keystore repository.p12 --tls-password-env DACP_REPOSITORY_TLS_PASSWORD
```

Both TLS options are required together. Nodes need a JVM trust store trusting the certificate. Alternatively terminate HTTPS at a reverse proxy. SHA-256 checks integrity; HTTPS authenticates the source. Dataset/task traffic remains Flight mTLS. Cache and stream resource lifecycle belongs to the execution node; artifact caches currently have no automatic garbage collection.

## Validation

```bash
(cd dacp-operator-repository && sbt test)
(cd dacp-operator-executors && sbt test)
scripts/test-operator-repository.sh
```

Unit/HTTP tests use an injected exporter without Docker. Integration requires the built runtime, Python SDK, sbt, Docker, and free example ports; it starts a repository and two mTLS nodes with independent caches and executes the distributed conversion/identity DAG. Logs and artifacts remain in the printed temporary directory; services stop on exit.

For complete source examples, wheel packaging, remote file transfer, administrator publication, and execution instructions, see [Write and upload custom operators](../docs/operator-authoring_en.md).

Definitions must include named `inputs` and `outputs` ports. `publish --ports FILE` is required; use YAML/JSON port declarations or a complete operator definition. `seed` reads these declarations from definitions. See the [authoring guide](../docs/operator-authoring_en.md).

Native commands use `executor: command`, with plugin-owned settings in `executor_config`. `seed` preserves this mapping; manual publication accepts a standalone YAML/JSON mapping through `--executor-config FILE`. The repository stores it as `execution.config`; the execution-node plugin validates it. See [native commands](../docs/native-commands_en.md).
