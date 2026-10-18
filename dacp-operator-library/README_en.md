# DACP operator library

A standalone Scala module serving immutable `name@version` operator manifests and Docker image archives over HTTP or HTTPS. It uses JDK 17, Scala 2.13 and sbt 1.10.x. The repository runs independently from DACP data servers; serving published artifacts does not require Docker. Publication requires the Docker CLI and access to its engine.

## Starter catalog

Set up and start a populated repository from the project root:

```bash
scripts/setup-operator-library.sh --root .operator-catalog --host 0.0.0.0 --port 8820
curl http://localhost:8820/v1/operators
```

The setup command builds the shared Docker image, stages the Scala service, seeds the catalog, and starts serving it. It requires Docker engine access, sbt, and build-network access. It validates existing published versions and leaves them immutable on repeated setup; corrupt artifacts stop setup. The default catalog is `.operator-catalog` under the project root. Additional arguments are passed to `serve`, including HTTPS options.

| Starter operator | Purpose | Parameters |
|---|---|---|
| `format_convert@1.0` | Convert CSV or NDJSON Items to Parquet, preserving separate Items | `from: auto`, `to: parquet`, `schema: infer`; see the [conversion reference](../docs/reference_en.md#format_convert-parameters) |
| `identity@1.0` | Copy input byte streams into a new dataset; useful for checking transport and chaining jobs | None |

Both operators use `dacp/format-convert:1.0`. Operators may be written in Python, Scala, or any language; their containers must implement the [Arrow IPC contract](../docs/reference_en.md#task-execution-and-operators).

To seed an already built deployment without starting the service:

```bash
cd dacp-operator-library
target/universal/stage/bin/dacp-operator-library seed --operators ../operators --root .operator-catalog
```

`seed` discovers `<operators>/<name>/<version>/operator.yaml` definitions containing `name`, `version` and `image`; all referenced images must be installed locally. New operators can be contributed by adding definitions and building their images, without editing a registry. Empty definition directories and invalid identities fail explicitly. Existing versions remain unchanged even if their source image tag changes; publish a new version for updated code.

## Manual setup

Build and start from the repository root:

```bash
cd dacp-operator-library
sbt test stage
# Build the operator image first; see the project README.
target/universal/stage/bin/dacp-operator-library publish format_convert@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
target/universal/stage/bin/dacp-operator-library publish identity@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
target/universal/stage/bin/dacp-operator-library serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`serve` defaults to `127.0.0.1:8820`. `--root` is required for both commands. The staged launcher can run without sbt. Stop the service with SIGTERM or Ctrl-C.

Configure every execution node:

```yaml
local_operator_library: ../operators
remote_operator_library_urls:
  - http://library-host:8820
  - https://another-library.example.org
operator_cache: /var/lib/dacp/operators
operator_max_bytes: 8589934592
```

Tasks reference `operator: format_convert@1.0`. Resolution first checks the local definition and installed Docker image, then completed cached downloads, then remote libraries in order. A missing manifest (HTTP 404) advances to the next library. Other HTTP errors, invalid manifests and corrupt archives fail the task. Completed caches work while the library is offline. Each execution node verifies identity, image ID, archive size and SHA-256 before loading the image into its Docker engine. Execution uses the pinned image ID.

## Publication and HTTP contract

Implement an operator using the [Arrow IPC container contract](../docs/reference_en.md#task-execution-and-operators), build its Docker image, and publish it with `publish name@version --image IMAGE --root DIR`. No server registry or client changes are needed. Names and versions must match `[A-Za-z0-9_-][A-Za-z0-9_.-]*`.

Publication inspects the image ID, exports that exact ID with `docker save`, computes SHA-256 with bounded memory, and atomically publishes a complete directory. File locks coordinate publishers; an existing version cannot be overwritten. Failed exports remove partial artifacts. The repository root must be on a filesystem supporting atomic directory moves and file locks. Only administrators should write to it.

| GET endpoint | Response |
|---|---|
| `/v1/operators` | `200`, JSON array of published manifests, sorted by name/version; incomplete staging directories are excluded |
| `/health` | `200`, `ok` followed by a newline |
| `/v1/operators/<name>/<version>` | `200`, JSON manifest |
| `/v1/operators/<name>/<version>/image.tar` | `200`, Docker save archive streamed with bounded memory |

Unknown paths and missing artifacts return 404. Other HTTP methods return 405. There is no HTTP upload endpoint. Requests cannot follow symlinks outside the catalog. The service uses eight request workers with a bounded queue.

The on-disk layout is `<root>/<name>/<version>/{manifest.json,image.tar}`. The manifest contract is:

```json
{"api_version":1,"name":"identity","version":"1.0","image":"sha256:<64 lowercase hex digits>","archive":{"size":12345,"sha256":"<64 lowercase hex digits>"}}
```

Download timeouts, size limits, cache verification and cache lifecycle belong to the execution node. Cache storage is separate from task-output TTLs. Corrupted caches fail explicitly; remove the affected version's cache directory to download it again.

## HTTPS

Use a PKCS12 keystore containing the server certificate and private key. Supply its password through an environment variable:

```bash
export DACP_LIBRARY_TLS_PASSWORD='your-keystore-password'
target/universal/stage/bin/dacp-operator-library serve --root .operator-catalog --host 0.0.0.0 --port 8820 --tls-keystore library.p12 --tls-password-env DACP_LIBRARY_TLS_PASSWORD
```

The two TLS options must be supplied together. Configure execution nodes with an HTTPS URL and a JVM trust store that trusts the certificate. HTTP is intended for trusted development networks. SHA-256 verifies archive integrity; HTTPS establishes the source of the manifest. Task and dataset traffic continues to use Flight mTLS and URI authorization.

## Validation

```bash
cd dacp-operator-library
sbt test
cd ..
scripts/test-operator-library.sh
```

Unit and HTTP contract tests use an injected image exporter and require no Docker engine. The integration script requires the Python SDK, sbt, Docker access and the built `dacp/format-convert:1.0` image. It stages the Scala repository, publishes both operators, starts the library and two mTLS nodes with independent caches, and runs the distributed conversion/identity DAG. Large archives need several GB of free space. Logs, catalog and caches remain under the printed temporary directory; services stop on exit.
