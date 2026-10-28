# Operator source and review information

[中文](operator-provenance_zh.md)

Public repository submissions require source. Developers declare `source_public: true` or `false`; administrators always have authenticated access. The repository scans and builds source before review. Approved public manifests include source only when public disclosure was requested. Source repository/revision and build instructions are also private when source is private. License and SBOM materials are public after approval.

In a [source submission YAML](operator-submission.md), source is supplied through `--source sources.zip`. Optional supporting metadata is:

```yaml
source_public: false
provenance:
  source_repository: https://example.org/team/operator
  source_revision: exact-commit
  license: Apache-2.0
  license_file: LICENSE.txt
  build_instructions: |
    Uses the registered python-arrow@1.0 environment.
    No additional dependencies are downloaded.
  sbom: dependencies.cdx.json
```

Paths resolve relative to the submission YAML. `source_revision` requires `source_repository`. The revision and license are submitter declarations, not automatically verified attestations. License/SBOM files are retained as opaque review materials. Build instructions are review text, never executed. The actual build uses a fixed repository recipe and administrator-selected environment. Build instructions are limited to 32 KiB UTF-8 and the full HTTP manifest to 64 KiB.

Stored file descriptors contain `sha256`, `size` and `filename`. Published permitted files use `/v1/blobs/sha256/{hash}/{filename}`. Private source and pending artifacts require authenticated administrator endpoints; knowing their hashes does not grant public download access. Administrators see original scan findings, coverage and score, then set the final public trust score. Automatic analysis can miss threats; the score is not a security or legal certification.

Execution nodes download execution artifacts and runtimes, not review archives. Full [submission, build and review instructions](operator-submission.md) include API examples, source archive limits, analyzer limitations, isolation and private-source behavior.
