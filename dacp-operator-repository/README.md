# DACP Operator Repository

[中文](README_zh.md)

Scala HTTP/HTTPS service for public operator discovery and downloads, source-only submissions, isolated builds and administrator review. The public UI has no developer upload form. Administrators sign in at `/admin` to manage submitter Tokens, runtimes and reviews.

## Build and start

Requires JDK 17 and sbt 1.10.x. Source builds also require Linux and access to Docker Engine.

```bash
cd dacp-operator-repository
sbt test stage
./target/universal/stage/bin/dacp-operator-repository serve \
  --root /srv/dacp/operators --host 127.0.0.1 --port 8820
```

On first startup, a random administrator password is written to `<root>/.admin/bootstrap-password`; the private directory has mode `0700`. Logs show the path, never the password. To choose an initial password, use `--admin-password-env ENV_NAME` with an environment variable containing 12–1024 characters. Initialization settings do not replace an existing password. Password verification uses salted PBKDF2-HMAC-SHA256; only submitter Token hashes are stored. Admin sessions last eight hours and are cleared on restart. Mutations require CSRF protection. Repeated failed logins are rate limited.

Use HTTPS for non-local administrator and developer access. Native TLS accepts `--tls-keystore PKCS12 --tls-password-env ENV_NAME`; HTTPS sessions set Secure cookies. `--publish-max-bytes` sets the runtime upload limit (default 8589934592 bytes). Source uploads are additionally capped at 64 MiB; manifests at 64 KiB.

## Development and publication

Read [source submission, builds and review](../docs/operator-submission.md) for the complete developer/admin workflows, YAML, CLI, endpoints, isolation and scan limitations, and [operator authoring](../docs/operator-authoring.md) for stream-processing examples.

1. An administrator registers compatible execution/build environments and issues a Token for each submitter.
2. A developer chooses a public runtime, submits source plus interface metadata and explicitly declares source visibility.
3. The repository analyzes and builds source asynchronously; build failures remain private.
4. An administrator reviews source, findings and artifact, selects/adds multiple category labels, and confirms a 0–5 trust score (default: scan score).
5. Only approval exposes the operator. Public source appears only when its submission declares `source_public: true`.

Public entry points: `/`, `/operators/{name}/{version}`, `/runtimes`, `/v1/operators`, `/v1/runtimes` and `/v1/categories`. The homepage groups versions and shows the newest approved version. Search includes administrator-assigned categories. Public pages display the administrator's trust score; detailed scan evidence stays private. A score is an assessment, not a safety guarantee. Blob downloads preserve filenames and cannot bypass approval or private-source restrictions.

Source builds support Python, Java and Scala through registered build recipes. Python produces a bytecode ZIP; Java/Scala produce a JAR. Dependencies remain in shared runtime images. The repository accepts no submitted binaries, arbitrary build commands, developer categories or direct publication requests. Local packager files are not automatically public. Existing local artifacts must be resubmitted as source and reviewed; there are no compatibility publication paths.

## Storage and recovery

`<root>/.admin/state.json` stores password verification data, hashed Tokens, runtime registrations, immutable submission manifests, scan/build results and review decisions. Back up the entire root, including `.admin` and `blobs/sha256`. Only one process may own a repository root. Sessions are memory-only. Queued/running builds are retried after restart; completed approvals and final trust scores persist. Unreferenced upload blobs remain private and are retained; automatic blob garbage collection is not implemented.

Blobs use SHA-256 addressing and retain filenames in descriptors. Execution images and separate build images are pinned by image ID. Every approved version is immutable. Developer Tokens identify named submitters; revocation applies to subsequent service calls, and the API enforces access regardless of client/tool choice.

## Tests and extension points

```bash
sbt test
```

Tests cover private submissions, final trust scores, multiple categories, source visibility, review gating, blob access, runtime catalogs, Token isolation/revocation, CSRF, immutable versions, source/license metadata and escaped HTML. Compiler isolation is also verified against a real Docker environment during deployment checks. See [source analysis/build plugins](../plugins/dacp-plugin-source-analysis/README.md) for ServiceLoader registration and contract tests.

Python bytecode and JVM JARs can be decompiled. `source_public` controls the original source materials, not mathematical secrecy of downloadable code. Python build and execution versions must match.
