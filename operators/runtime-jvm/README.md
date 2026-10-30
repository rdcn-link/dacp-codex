# Shared JVM dependency environment

[中文](README_zh.md)

This project stages JVM dependencies only. It contains no operator implementation, entrypoint or server. Java/Scala operators are independently authored, submitted as source, built and approved through the repository.

From the repository root, build the shared environment on Linux with JDK 17, sbt and Docker:

```bash
scripts/build-jvm-runtime.sh
# Optional explicit image tag:
scripts/build-jvm-runtime.sh dacp/operator-jvm:custom
```

The script runs `sbt stageRuntime` here, copies dependency JARs from `target/runtime/lib`, creates a stripped JVM and native compression libraries, and builds the image using [Dockerfile](Dockerfile). Operator code is packaged separately and is never included in this image. The build requires access to Maven dependencies and the local Docker engine. Register the resulting immutable runtime image and archive in the repository before developers select it for source builds.

For SDK changes, rebuild the environment before registering a new runtime version. See [operator submission](../../docs/operator-submission.md) and [packaging](../../docs/operator-packaging.md).
