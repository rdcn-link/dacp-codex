# Source analysis and build plugins

[中文](README_zh.md)

Provides the repository's bounded source scanner, safe ZIP/TAR unpacker and Python/Java/Scala build recipes. It does not run uploaded programs on the host. See [submission and isolation limits](../../docs/operator-submission.md).

## Extension contracts

Public contracts live in `dacp-operator-contract`:

- `dacp.operator.SourceAnalyzer`: unique analyzer `id`, `supports(filename)`, and `analyze(Path, filename)` returning a report with `status` (`complete` or `failed`), `score` (0–5 when complete), findings and coverage details. The repository runs every discovered analyzer and uses the lowest completed score. Any analyzer failure prevents approval.
- `dacp.operator.SourceArchive`: `supports(filename)` and `unpack(archive, filename, destination)`. Validate entry paths, links, types, compressed/expanded sizes and file counts. Never write outside the destination. The bundled implementation handles ZIP, TAR, TAR.GZ and TGZ.
- `dacp.operator.SourceBuildRecipe`: language, executor, artifact filename, fixed command and artifact-entrypoint verification. Commands run inside a constrained container; they receive `/source`, `/tmp`, `/output` and the trusted `/builder/helper.jar`. Write only the artifact to stdout and diagnostics to stderr. Runtime dependencies must be supplied by the registered image.

External plugins compile against `dacp-operator-contract`, implement public no-argument classes, and list their class names in `META-INF/services/<fully-qualified-contract-name>`. Put the plugin JAR and its dependencies on the repository launcher's classpath (for example in the staged application's `lib/`), then restart. No core registry edit, server rebuild or client change is needed. Third-party archive dependencies stay in this plugin module. Duplicate build languages fail initialization; provide one recipe per language. This contract is implemented; semantic SAST, dependency vulnerability scanning and arbitrary build systems are not implemented.

For example, register a new analyzer using `META-INF/services/dacp.operator.SourceAnalyzer` containing `example.CompanyAnalyzer`. Report unsupported or failed analysis explicitly, rather than assigning a clean score. Analysis implementations are trusted administrator-installed code and must bound CPU, memory, file reads and report sizes.

## Contract tests

```bash
cd plugins/dacp-plugin-source-analysis
sbt test
```

Tests cover clean source, network/deletion evidence, incomplete coverage, archive traversal and decompression limits. Repository tests cover source-only submission, private files, asynchronous analysis/build state, administrator review, trust and classification publication, submitter isolation and Token revocation. New plugins should add equivalent tests for malformed inputs, coverage gaps and resource limits.
