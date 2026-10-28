# Operator packaging and shared environments

Operators must declare a static `parameters` contract alongside `inputs`; see [parameter types and forms](operator-parameters_en.md). Output structures remain exclusively defined by inference methods.
Output structures come exclusively from the mandatory inference method; manifests declare input contracts only. See [structure inference](structure-inference_en.md).

Operators consist of an immutable `name@version` manifest, a shared runtime image, and an optional small code artifact. Multiple names and versions can reference the same runtime and code bundle. This is implemented for complete containers, thin Java/Scala JARs, Python source files, and Python wheels. Operators always execute in a separate Docker container; public repository artifacts are never loaded into the server JVM.

## Runtime and dependency ownership

Build dependencies into a versioned runtime image once. Published manifests pin its Docker image ID and the SHA-256/size of its Docker-save archive. Dependencies are not installed from the internet during a task. A new dependency set produces a new runtime image; a code-only release can reuse the existing runtime descriptor. Runtime images are platform-specific: publish a runtime matching the execution node's OS/architecture. Automatic platform selection is not implemented.

The JVM environment supplies `/opt/java/bin/java` and dependencies under `/opt/lib/*`. The Python environment supplies `/usr/local/bin/python`, pip, and all operator dependencies. Wheels install offline with `--no-index --no-deps` into temporary storage; pip checks platform/Python tags. A single `.py` file requires no installation. Prefer wheels for multiple modules or native extensions. Package native libraries in platform-compatible wheels or the shared environment. Raw source support is for a single script, not automatic dependency resolution.

A bundle can contain several main classes or Python modules. Publish each operator with its own entrypoint and the same artifact. Bundle operators that share dependencies and release schedules; independent code artifacts can still share the same runtime. Java and Scala use the same `jvm` executor. A fat JAR is supported but duplicates dependencies; thin JARs give smaller incremental downloads.

## Definitions and publication

Definitions live at `<operators>/<name>/<version>/operator.yaml`. Required fields are `name`, `version`, `image`, `category`, `icon`, `inputs`, and `parameters`; optional fields are `description`, `executor` (defaults to `container`), `entrypoint`, `artifact`, and plugin-owned `executor_config`. Artifact paths are relative to the definition. Unknown fields and duplicate YAML keys fail.

```yaml
name: summary
version: "1"
image: example/jvm-arrow:1
executor: jvm
entrypoint: example.Summary
artifact: summary.jar
inputs:
  source: {representations: [records], formats: []}
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

`jvm` needs a JAR and main class. `python` needs a `.py` or `.whl` artifact and a module name (`entrypoint` selects the module for wheels; source executes the script). `command` adapts native stdin/stdout programs; see [native commands](native-commands_en.md). `container` uses its image's existing entrypoint and accepts neither an entrypoint override nor a code artifact. Every executor receives `operator /input /input/params.json` and `DACP_OPERATOR=name@version`, following the existing [Arrow IPC contract](reference_en.md#task-execution-and-operators).

```bash
# Run from the project root after staging the repository.
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@1 --root .operator-catalog --image example/jvm-arrow:1 --executor jvm --entrypoint example.Summary --artifact summary.jar --ports ports.yaml
```

The repository does not need executor plugins to store or serve manifests. Execution nodes must have a matching plugin. Publisher validation is generic; executor-specific artifact and entrypoint validation happens on the execution node.

## Content-addressed storage

Manifest schema `api_version: 2` separates execution, runtime, and code:

```json
{
  "api_version": 2,
  "name": "summary",
  "version": "1",
  "description": "Summarize records",
  "inputs": {"source": {"representations": ["records"], "formats": []}},
  "execution": {"executor": "jvm", "entrypoint": "example.Summary"},
  "runtime": {
    "image": "sha256:<image ID>",
    "archive": {"sha256": "<archive digest>", "size": 123456, "filename": "runtime.tar"}
  },
  "artifact": {"sha256": "<code digest>", "size": 1234, "filename": "summary.jar"}
}
```

`artifact` is omitted for complete containers. Digests are 64 lowercase hexadecimal characters; sizes are positive byte counts. Metadata is published atomically and versions cannot be overwritten. The development project uses this schema directly; there are no old-manifest readers or migration fallbacks.

Repository layout:

```text
<root>/<name>/<version>/manifest.json
<root>/blobs/sha256/<digest>
<root>/images/<image-ID-without-prefix>.json
```

The image index avoids repeated `docker save` exports of the same image. Both code and runtime archives are stored once per digest. The public `/v1/blobs/sha256/<digest>` endpoint serves the bytes; operator pages expose separate runtime and code downloads, sizes, and checksums.

Execution node layout:

```text
<operator_cache>/manifests/<repository-URL-hash>/<name>/<version>.json
<operator_cache>/blobs/sha256/<digest>
```

Metadata stays scoped to the repository URL; verified blobs are shared across names, versions, and repositories. Locks protect concurrent publishers/downloaders and atomic moves prevent partial cache entries. Every remote/cached artifact is checked before execution; a corrupt blob fails explicitly. Remove that blob to download it again. `operator_max_bytes` applies to each artifact, not the sum or extracted container size. Caches currently have no automatic garbage collection.

Local definitions and installed images take precedence. Completed cached manifests are tried before network requests, then configured remote URLs in order. Only a manifest 404 tries the next repository; checksum, HTTP, and validation errors fail the task. Cached manifests and blobs allow offline execution. The runtime image is loaded only when its pinned ID is absent from Docker.

Docker layers are shared locally, and the JVM builder separates the runtime layer from the optional complete-image code layer. Repository transport currently transfers whole content-addressed image archives; OCI registry layer-by-layer delivery and cross-runtime layer transfer deduplication remain future work.

## Executor plugins

Implement `dacp.OperatorExecutor` from `dacp-plugin-api` and register the fully qualified class in `META-INF/services/dacp.OperatorExecutor`. Compile the API as `Provided`, install the plugin JAR and private dependencies in `plugin_directory`, and restart the node. No core registry or client changes are needed. Duplicate executor IDs fail explicitly. The built-in `container`, `jvm`, `python`, and `command` implementations live in `plugins/dacp-plugin-operator-executors` and use that same ServiceLoader mechanism.

```scala
final class CustomExecutor extends dacp.OperatorExecutor {
  val id = "custom"
  def command(p: dacp.OperatorProgram, artifactPath: Option[String]): dacp.OperatorCommand = {
    require(p.entrypoint.nonEmpty, "entrypoint required")
    dacp.OperatorCommand(entrypoint = Some(Vector("/opt/custom/bin/run", p.entrypoint)))
  }
}
```

The plugin validates its entrypoint/artifact and `OperatorProgram.configuration` and supplies a container command and environment. `OperatorProgram.artifact` is the verified host path; `artifactPath` is its read-only path inside the container. Plugins are trusted launch code; operator artifacts are separate executable payloads. The server owns image loading, read-only mounts/root filesystem, disabled networking, temporary storage, memory/PID limits, timeout, cleanup, Flight transport, and stream delivery. Plugins do not replace the isolation/resource lifecycle. No host-process executor is provided.

## Validation

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-repository && sbt test)
(cd dacp-server && sbt 'testOnly dacp.OperatorLibrarySpec dacp.CoreSpec dacp.CollaborationSpec')
scripts/test-operator-repository.sh
```

Contract tests cover service registration, launch validation, shared exports/bundles/downloads, immutable publication, public downloads, checksum failures, cleanup, library ordering, and offline caches. The repository integration exercises real JAR execution through the distributed Arrow/FIFO pipeline. A complete [Scala example](../examples/operators/scala/README_en.md) compiles independently against runtime-provided libraries. See the [Python example](../examples/operators/python/README_en.md) for source and wheel execution.

For a step-by-step workflow with full source examples and remote upload instructions, see [Write and publish user-defined operators](operator-authoring_en.md).

## Execution contract

The implemented [item-based, stream-only contract](operator-streams_en.md) binds named item inputs and returns named item streams. The `ports.yaml` passed to publication contains the same `inputs` and `outputs` maps as the local definition. Dataset mapping and general pipeline scheduling remain future work.

## Optional review evidence

API v2 supports optional `provenance` metadata and SHA-256-addressed source, license and SBOM downloads. These files are separate from execution and never fetched automatically by execution nodes. See [source and review information](operator-provenance_en.md) for YAML fields, CLI flags, manifest descriptors and validation limits.

Operators must also provide `category` and `icon`; see [categories and icons](operator-presentation_en.md) for the contract and publication examples.
