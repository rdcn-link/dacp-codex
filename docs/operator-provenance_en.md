# Operator source and review information

Publishers can attach optional review evidence to an operator. Public detail pages show source availability, the source repository and revision, the declared license, build instructions, and downloads. Missing source, revision, license, or instructions are labelled explicitly. Source availability and SHA-256 checksums do not establish safety, legal clearance, or that the execution artifact was built from that source.

Add this optional mapping to `operator.yaml`:

```yaml
provenance:
  source_archive: sources.zip
  source_repository: https://example.org/team/operator
  source_revision: <exact-commit>
  license: MIT
  license_file: LICENSE.txt
  build_instructions: |
    javac -d classes src/example/Summary.java
    jar --create --file summary.jar -C classes .
  sbom: dependencies.cdx.json
```

Every field is optional. Omit fields you cannot substantiate. Paths resolve relative to `operator.yaml`. Archives must be nonempty ZIP, TAR, TAR.GZ, or TGZ files; license text and SBOM are nonempty files. These files are stored as opaque bytes: the repository does not extract archives, validate licenses, or parse SBOM contents. Prefer an SPDX license identifier/expression and a CycloneDX or SPDX SBOM. Include sources, dependency declarations, build tools and reproducible build steps. The revision requires a repository URL; it is publisher supplied and is not checked against the archive. Build instructions are limited to 32 KiB UTF-8; the complete manifest is limited to 64 KiB.

Administrator CLI example (from the project root):

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@2 \
  --root .operator-catalog --image example/jvm-arrow:1 \
  --executor jvm --entrypoint example.Summary --artifact summary.jar \
  --ports summary-ports.yaml \
  --source-archive sources.zip \
  --source-repository https://example.org/team/operator \
  --source-revision 'REPLACE_WITH_EXACT_COMMIT' \
  --license MIT --license-file LICENSE.txt \
  --build-instructions BUILD.txt --sbom dependencies.cdx.json
```

Replace `<exact-commit>` before running. Unlike YAML inline instructions, `--build-instructions` reads a UTF-8 text file. Publication uses the administrator CLI or the [token-authenticated HTTP API](../dacp-operator-repository/README_en.md#token-authenticated-publication-api); public browsing and downloads require no token. Existing versions cannot be modified; publish a new version when adding evidence.

API v2 manifests carry an optional `provenance` object. Text fields retain the names above. Each file field becomes `{ "sha256": "<64 lowercase hex digits>", "size": 123, "filename": "sources.zip" }`. Download its bytes at `/v1/blobs/sha256/<sha256>` and compare both size and SHA-256 with the manifest. Publication and repeated seeding verify evidence blobs. Source, license and SBOM files share the same content-addressed storage as execution artifacts.

Execution nodes fetch only execution artifacts and runtimes. Review files are downloaded explicitly by readers and are never executed or automatically installed. Inspect source, dependencies, the license and the build process before trusting an operator; attached evidence is a publisher declaration.

## Source-bearing examples

From the project root, run `python3 scripts/package-operator-sources.py` before seeding new example definitions. It creates `.operator-sources/dacp-sources.zip` from the current nonignored Git working-tree files, including uncommitted changes, with fixed ZIP timestamps. Review its contents before publication; it is a working-tree snapshot, not a certified commit export. The project currently declares no license, so examples intentionally omit license metadata and SBOMs.

The new examples are `identity@1.3`, `format_convert@1.3`, `identity_python@1.1` and `native_uppercase@1.1`. Build the JVM JAR, install the shared runtime images, and compile the native binary using the existing example instructions. Then package sources and seed `operators`, `examples/operators/python`, and `examples/operators/native/operators` separately. The standard setup script packages sources and seeds JVM examples automatically. Each new example links the snapshot and its build instructions; previous immutable versions remain available.

Integration validation: with the repository serving these examples on `localhost:8820`, built `dacp-server`, example mTLS certificates, the Python SDK/PyArrow installed in `.venv`, and Docker access, run `.venv/bin/python scripts/test-operator-provenance.py`. It starts and stops a temporary execution node, checks JVM/Python/native streams and Parquet conversion, verifies review downloads and pages, and asserts that execution never downloads source evidence.
