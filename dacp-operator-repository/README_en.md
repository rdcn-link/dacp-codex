# DACP operator repository

[中文](README.md)

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

Both use the same thin Scala JAR and runtime environment. The repository exports the runtime only once. See the [runtime builder](../dacp-operator-runtime/README_en.md), [user-defined Scala example](../examples/operators/scala/README_en.md), [Python example](../examples/operators/python/README_en.md), and [packaging contracts](../docs/operator-packaging_en.md).

## Publication and execution

```bash
# From the project root; images must already be installed.
(cd dacp-operator-repository && sbt test stage)
python3 scripts/package-operator-sources.py
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`seed` discovers `<operators>/<name>/<version>/operator.yaml`. Definitions require `name`, `version`, `image`, `inputs`, and `outputs`; optional fields are `description`, `executor` (default `container`), `entrypoint`, `artifact` (relative file path), `executor_config`, and `provenance`. Names/versions match `[A-Za-z0-9_-][A-Za-z0-9_.-]*`. Unknown fields, duplicate keys, mismatched directories, and empty definition directories fail.

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

Tasks retain references such as `operator: format_convert@1.1`. Resolution checks local definitions/installed images, completed caches, then remote repositories in order. Only manifest HTTP 404 advances to another repository. All remote/cached blobs are verified for size and SHA-256; images use pinned IDs. Blobs are shared across operators and repositories; completed caches work offline. Limits apply per artifact. See [cache layout and limitations](../docs/operator-packaging_en.md#content-addressed-storage).

## Public website and HTTP API

Open `http://localhost:8820/`. Anyone can search names, versions, and descriptions, view versions, and download code and runtimes without signing in. Pages show separate sizes/checksums, executor, entrypoint, runtime image ID, and task references. All text is escaped. Pages work without JavaScript or external assets. Publication uses the administrator CLI or the token-authenticated HTTP API below.

| GET / HEAD endpoint | Response |
|---|---|
| `/` or `/?q=keyword` | Public catalog; case-insensitive search, maximum 200 characters |
| `/operators/<name>/<version>` | Operator details and separate artifact downloads |
| `/assets/repository.css` | Local stylesheet |
| `/v1/operators` | JSON array of manifests sorted by name/version |
| `/v1/operators/<name>/<version>` | Manifest JSON, schema `api_version: 2` |
| `/v1/blobs/sha256/<64 lowercase hex digits>` | Shared runtime archive or code bytes |
| `/health` | `ok` followed by a newline |

Unknown paths/missing artifacts return 404; unsupported write methods return 405. HEAD supplies Content-Length. Web download links include the original artifact filename after the digest (`/v1/blobs/sha256/DIGEST/FILENAME`); GET and HEAD responses use it in `Content-Disposition`, preserving file extensions. Digest-only URLs are the content-addressed retrieval API. Paths cannot follow symlinks outside the catalog. Eight workers use a bounded queue. Staging directories are excluded from listing. Catalog reads metadata without hashing large blobs.

Metadata lives in `<root>/<name>/<version>/manifest.json`; blobs in `<root>/blobs/sha256/<digest>`; the runtime export index in `<root>/images/<image-ID>.json`. See the [manifest schema](../docs/operator-packaging_en.md#content-addressed-storage). This unreleased project directly uses the new schema, without old-schema readers. Use a fresh development catalog for the new format.

## Token-authenticated publication API

Publication endpoints are enabled by default, and every publication request requires a valid token. Without `--publish-token-env`, the first server startup generates a random token in `<root>/.publish-token` (permissions `0600`); subsequent starts reuse it. Startup logs show only its path, never the token. Administrators may override this credential with `--publish-token-env`, naming an environment variable containing a token of at least 32 visible ASCII characters. Missing or incorrect tokens return 401; public browsing and downloads require no token. One token grants publication rights across the repository. Replace `.publish-token` when using the default file, or change the environment variable when using an override, then restart to rotate it without changing published operators.

Before using the upload script with the generated token, read and export it on the repository host:

```bash
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
```

Alternatively, supply your own token when starting the server:

```bash
export DACP_REPOSITORY_PUBLISH_TOKEN="$(openssl rand -hex 32)"
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 127.0.0.1 --port 8820 --publish-token-env DACP_REPOSITORY_PUBLISH_TOKEN --publish-max-bytes 8589934592
```

Use HTTPS for remote publishing, through built-in TLS described below or a trusted reverse proxy. Send the token in the `Authorization: Bearer <token>` header, not in URLs. The Web UI only supports public browsing and downloads, with no publication form or upload button. Publishing uses the token-authenticated HTTP API or upload script exclusively.

| Interface | Request and behavior |
|---|---|
| `PUT /v1/blobs/sha256/<digest>` | Raw `application/octet-stream` content; streams uploads and verifies SHA-256; identical content can be uploaded again and reused |
| `POST /v1/operators` | Existing `api_version: 2` manifest as `application/json`; all referenced artifacts must already be uploaded and pass size, digest, and metadata validation |

Success returns 201; a manifest response includes its public manifest URL in `Location`. Existing name/version pairs return 409 and cannot be overwritten. Invalid digests or manifests return 400, unsupported Content-Type returns 415, and oversized requests return 413. Manifests are limited to 64 KiB; each artifact defaults to 8 GiB, configurable with `--publish-max-bytes`. Completed uploads enter content-addressed storage, and validated manifests are published atomically. Failed temporary uploads are removed. Uploaded blobs not referenced by a manifest remain stored; automatic garbage collection is not implemented.

Upload a catalog generated locally with the CLI (`publish` or `seed`), including runtime, code, and source materials:

```bash
python3 scripts/upload-operator.py summary@1 --catalog .operator-catalog --url https://repository.example.org --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

The script uses only the Python standard library, streams large files, and collects runtime archives, operator code, source archives, license files, and SBOMs from the manifest. Use `--url http://localhost:8820` for local testing. The repository server does not execute uploaded code, fetch external URLs, or call Docker; Docker runtime export occurs on the publisher's machine. Source and licensing materials remain publisher declarations, rather than safety or legal certification.

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
(cd plugins/dacp-plugin-operator-executors && sbt test)
scripts/test-operator-repository.sh
```

Unit/HTTP tests use an injected exporter without Docker. Integration requires the built runtime, Python SDK, sbt, Docker, and free example ports; it starts a repository and two mTLS nodes with independent caches and executes the distributed conversion/identity DAG. Logs and artifacts remain in the printed temporary directory; services stop on exit.

For complete source examples, wheel packaging, remote file transfer, administrator publication, and execution instructions, see [Write and upload custom operators](../docs/operator-authoring_en.md).

Definitions must include named `inputs` and `outputs` ports. `publish --ports FILE` is required; use YAML/JSON port declarations or a complete operator definition. `seed` reads these declarations from definitions. See the [authoring guide](../docs/operator-authoring_en.md).

Native commands use `executor: command`, with plugin-owned settings in `executor_config`. `seed` preserves this mapping; manual publication accepts a standalone YAML/JSON mapping through `--executor-config FILE`. The repository stores it as `execution.config`; the execution-node plugin validates it. See [native commands](../docs/native-commands_en.md).

## Operator review evidence

Publish optional source archives, repository revisions, license declarations/text, build instructions and SBOMs. Public pages show downloads and missing information. See [source and review information](../docs/operator-provenance_en.md).

The homepage groups entries by operator name and shows only the newest version, counting operators. Numeric runs in version labels compare as integers (`1.10` is newer than `1.9`); a release ranks above its hyphenated prerelease suffix, and other text compares lexically. Search matches names, versions, and descriptions across all versions while displaying the newest version of each matching operator. Detail pages list all versions from newest to oldest; the JSON catalog still includes every version.
