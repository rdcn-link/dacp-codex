# Source submission, builds and review

[中文](operator-submission_zh.md)

The repository accepts **source-only submissions**. Developers cannot submit final JARs, wheels, executable artifacts or classifications. The repository analyzes source, builds a small operator artifact in an isolated environment, and waits for administrator approval. Only approved operators appear in public search, manifests and downloads.

## Developer workflow

1. Obtain a personal submitter Token from the repository administrator.
2. Browse `/runtimes` or GET `/v1/runtimes`. Select a compatible pinned execution/build environment with all required dependencies already installed.
3. Implement processing, output-schema inference and bounded output samples using the [operator SDK](operator-authoring.md).
4. Put Python modules or Java/Scala packages under `src/`. JVM resources may use `resources/`. Create a ZIP/TAR without compiled artifacts, nested archives, symbolic links or build scripts that need execution.
5. Write the submission YAML and submit it with the helper. Record the returned submission ID; submission is not publication.

Example layout:

```text
weather-filter/
  submission.yaml
  src/
    weather_filter.py
  LICENSE.txt
```

```yaml
name: weather_filter
version: '1.0'
description: Filter observation records by quality
language: python
entrypoint: weather_filter
runtime: python-arrow@1.0
source_public: false
icon:
  path: M3 5h18v14H3zM3 10h18M9 5v14
  color: '#14866b'
inputs:
  source:
    representations: [records]
    formats: []
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
provenance:
  license: Apache-2.0
  license_file: LICENSE.txt
```

Use an actually registered runtime name/version. For Java or Scala, set `language` accordingly and `entrypoint` to the fully qualified main class, such as `example.WeatherFilter`. Source files belong under `src/example/`. The helper derives the executor and immutable runtime descriptors from the runtime catalog.

```bash
# From weather-filter/, package raw source, not prebuilt artifacts.
zip -r sources.zip src
# Run from the DACP checkout; install Python SDK dependencies and PyYAML first.
python scripts/submit-operator.py /path/weather-filter/submission.yaml \
  --source /path/weather-filter/sources.zip \
  --url https://operators.example.org \
  --token-file /secure/submitter.token
```

`--token-env ENV_NAME` is also available instead of `--token-file`. Remote submission requires HTTPS; HTTP is accepted only for localhost development. The helper uploads the source and optional license/SBOM, then POSTs `/v1/submissions`. It does not upload runtime images or operator artifacts. HTTP 202 returns `submission_id` and `status: pending`. A submitter can GET `/v1/submissions` or `/v1/submissions/{id}` with their Token to inspect only their own records. Revoked Tokens cannot submit or inspect records.

## Administrator workflow

Open `/admin`, sign in, and use the Submitter Tokens tab to issue or revoke a Token for a named submitter. The new Token is displayed once; only its hash is stored. A replacement Token with the same submitter name represents the same submitter. Choose distinct names for distinct people.

Use the Runtimes tab to register a pinned runtime and a compatible build environment. Supply their Docker image archives and image IDs, language/executor lists, version and dependency information. The runtime catalog and these environment archives are public. Version entries are immutable. Administrators must ensure that the build environment compiles against the execution environment's SDK and dependency versions.

Review submissions in the Review submissions tab. It shows the original manifest, source-publication declaration, private source download, scan report, build status and generated artifact. Refresh to retrieve asynchronous progress. Choose existing category labels or add multiple new labels. Set the final trust score from 0 to 5; it defaults to the scan score. Explain adjustments and approvals below 3 in the review note. Approve to publish or reject to leave it private. A reviewed submission cannot be reviewed twice; rejected versions may be corrected and resubmitted with a new submission ID. An approved name is reserved to its submitter, and an approved version cannot be overwritten.

Public pages and manifests show the **administrator-assessed trust score**, not a guarantee of safety. The original scan score and detailed findings remain in the private review record. A score of 5 means no issue was found within the analysis coverage; it does not establish complete trust. Static analysis can miss malicious behavior and report safe code. See [OWASP's SAST limitations](https://owasp.github.io/DevSecOpsGuideline/2-Process/2-3-Build/2-3-1-Static-Analysis/2-3-1-1-Static-Application-Security-Testing/).

## Source visibility and scanning

`source_public` is explicit and mandatory. Administrators can always download source through authenticated review endpoints. If false, public manifests omit the source archive, source repository/revision and build instructions, and public blob URLs cannot download those private files. Detailed scan reports can contain source excerpts and are not public. If true, source becomes public together with the approved artifact. License and SBOM files remain public review materials after approval.

The bundled analyzer performs bounded lexical triage on Python, Java, Scala and shell source plus readable project metadata. It flags network APIs/destinations, process execution, destructive filesystem access, dynamic/native code loading, environment/credential access and file writes. Findings include rule, severity, file, line and a bounded excerpt. It does not perform interprocedural data-flow analysis, dependency vulnerability scanning or behavioral execution. Unsupported files and incomplete coverage cap the automatic score at 2; no recognized executable source scores 0. Scan failures prevent approval. The report records coverage and analyzer ID. This is an implemented baseline analyzer, not a full SAST product. Additional analyzers use `dacp.operator.SourceAnalyzer` and Java ServiceLoader; see [plugin instructions](../plugins/dacp-plugin-source-analysis/README.md).

## Isolated builds

Java and Scala compile to JARs; Python is syntax-checked and compiled into a bytecode ZIP. Fixed build recipes are contributed through `SourceBuildRecipe`; arbitrary developer-provided build commands are not accepted. The administrator's build image must include the selected compiler and `/opt/lib/*` SDK/dependency classpath. JVM build images need `/opt/java/bin/java`; Java needs `jdk.compiler`, and Scala needs its compiler libraries. Python images need `/usr/local/bin/python` and execution dependencies such as the DACP SDK. No dependencies are downloaded during the build.

Each compiler runs in a fresh Docker container with no network, a read-only root filesystem and source mount, a non-root user, no capabilities, no additional privileges, a 64-process limit, one CPU and 512 MiB RAM. `/tmp` and `/output` each have a 64 MiB temporary filesystem. No host output directory, Docker socket or repository credentials are mounted. Artifacts return through a size-limited stdout pipe, with bounded diagnostics. Builds have a 120-second timeout and a 64 MiB artifact limit. One build runs at a time; 32 submissions may wait. Pending queued/running builds resume after repository restart. Runtime-image preparation may take up to 120 seconds separately. Failed builds remain private and cannot be approved. Administrators can reject and developers resubmit corrected sources.

Archive limits: 64 MiB compressed, 16 MiB expanded, 1000 entries and 1 MiB per file. Archives are never extracted using unchecked entry paths. Compiled artifacts, nested archives, links and special TAR files are rejected. Docker isolation reduces risk but does not prove safety; the host kernel and administrator-provided images remain trusted.

## HTTP interface

| Endpoint | Access | Purpose |
|---|---|---|
| GET `/v1/operators`, `/v1/operators/{name}/{version}` | Public | Approved manifests only |
| GET/HEAD `/v1/blobs/sha256/{hash}[/{filename}]` | Public | Files referenced by approved public manifests or registered runtimes |
| GET `/v1/build-languages` | Public | Language/executor/artifact metadata from installed build plugins |
| GET `/v1/runtimes`, `/v1/categories` | Public | Runtime catalog and administrator category labels |
| PUT `/v1/blobs/sha256/{hash}` | Submitter Token | Private source/license/SBOM upload; 64 MiB limit |
| POST `/v1/submissions` | Submitter Token | Source-only manifest; 64 KiB limit |
| GET `/v1/submissions[/{id}]` | Submitter Token | Own submission status |
| POST `/api/admin/login` | Password | Create an eight-hour admin session |
| GET `/api/admin/session` | Admin session | Session and CSRF token |
| POST `/api/admin/logout` | Admin session + CSRF | End session |
| GET/POST `/api/admin/tokens` | Admin session; CSRF on POST | List/issue submitter Tokens |
| DELETE `/api/admin/tokens/{id}` | Admin session + CSRF | Revoke Token |
| GET/POST `/api/admin/runtimes` | Admin session; CSRF on POST | List/register environments |
| POST `/api/admin/blobs` | Admin session + CSRF | Stream an archive with `X-File-Name`; return its descriptor |
| PUT `/api/admin/blobs/sha256/{hash}` | Admin session + CSRF | Upload runtime archives |
| GET `/api/admin/categories` | Admin session | Existing category labels |
| GET `/api/admin/submissions[/{id}]` | Admin session | Private review records |
| GET `/api/admin/submissions/{id}/files/{hash}/{filename}` | Admin session | Private review materials |
| POST `/api/admin/submissions/{id}/review` | Admin session + CSRF | Decision, categories, optional trust score and note |

Review example:

```json
{"decision":"approved","categories":["Climate","Conversion"],"trust_score":4,"note":"Reviewed network use; destination is controlled."}
```

Omit `trust_score` to use the completed scan's score. Login uses a JSON `password`; subsequent mutations send the session cookie and `X-CSRF-Token`. Publisher Tokens cannot call admin endpoints. There is no anonymous publication or auto-approval path, including files manually placed in the local packaging directory.

Python bytecode and JVM JARs can be decompiled. `source_public` controls the original source materials, not mathematical secrecy of downloadable code. Python build and execution versions must match.
