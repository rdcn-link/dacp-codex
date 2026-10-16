# DACP operator library

A standalone, read-only HTTP service distributing immutable `name@version` operator manifests and Docker image archives. No dataset registry is introduced. Each task's execution node downloads its operator and runs it in its own Docker engine with the existing Arrow IPC container contract.

From the repository root:

```bash
pip install -e dacp-operator-library
# Build the existing operator image first (see root README).
dacp-operator-library publish format_convert@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
dacp-operator-library publish identity@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
dacp-operator-library serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

Configure **every execution node**:

```yaml
operator_library_url: http://library-host:8820
operator_cache: /var/lib/dacp/operators
operator_max_bytes: 8589934592
```

Existing task YAML requires no changes: `operator: format_convert@1.0` resolves at the worker, including remotely dispatched tasks. The library publishes `/v1/operators/<name>/<version>` (JSON) and the same path plus `/image.tar`. `/health` reports readiness. Publication uses the local Docker CLI and fails if the version already exists; the HTTP API has no upload endpoint. Run publication with access to the Docker socket.

Nodes verify manifest identity, archive SHA-256 and byte length before Docker loads the archive. Execution uses the image ID, never a mutable tag. Downloads have connection/read timeouts and a byte limit; failures clean partial files and fail the task. There are no artifact retries or resumable downloads. Completed versions are cached atomically under a hash of the library URL; cache hits work while the library is offline. File locks coordinate shared caches. Corrupted cache entries fail explicitly; remove that version's cache directory to fetch it again. Cache storage is managed separately from task-output TTLs.

Omitting `operator_library_url` preserves the local `operator_library` directory mode. A configured remote library never falls back to a local version. HTTP is suitable for trusted development networks; for other networks enable `serve --tls-cert server.crt --tls-key server.key` and use HTTPS with a certificate trusted by the node JVM. SHA-256 detects archive corruption; the trusted HTTPS connection establishes the source of the manifest. Existing Flight mTLS and URI allowlists remain in force for task/data traffic.

Validation:

```bash
python -m pytest dacp-operator-library/tests
scripts/test-operator-library.sh
```

The integration script requires the Python SDK, sbt, Docker access and the built `dacp/format-convert:1.0` image. It publishes both operators, starts the library and two mTLS nodes with fresh independent caches, then runs the distributed conversion/identity DAG. Large image exports and cache copies need several GB of free space. Logs, catalog and caches are left under the printed temporary directory for inspection; services stop on exit.
