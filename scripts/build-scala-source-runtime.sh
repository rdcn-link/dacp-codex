#!/usr/bin/env bash
# Extend a local JVM runtime with a Scala compiler; no base image pull.
set -euo pipefail
cd "$(dirname "$0")/.."
base_image="${1:-dacp/operator-jvm:1.1}"
target_image="${2:-dacp/operator-scala-debug:1.0}"
(cd dacp-operator-executors && sbt -batch compile)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/dacp-scala-source-XXXXXX")
trap 'rm -rf "$build_dir"' EXIT
mkdir -p "$build_dir/lib"
for artifact in scala-compiler scala-reflect; do
  jar_path=$(find "${COURSIER_CACHE:-$HOME/.cache/coursier}" "$HOME/.sbt" -name "$artifact-2.13.16.jar" -type f -print -quit)
  if [[ -z "$jar_path" ]]; then echo "Missing $artifact 2.13.16 in the sbt/Coursier cache" >&2; exit 1; fi
  cp "$jar_path" "$build_dir/lib/"
done
jlink --add-modules java.base,java.logging,java.management,java.naming,java.security.jgss,java.sql,java.xml,jdk.unsupported,jdk.crypto.ec,jdk.security.auth,java.compiler,jdk.compiler,jdk.zipfs \
  --strip-debug --no-header-files --no-man-pages --compress=2 --output "$build_dir/java"
cat > "$build_dir/Dockerfile" <<'DOCKER'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY java/ /opt/java/
COPY lib/ /opt/lib/
DOCKER
docker build --build-arg "BASE_IMAGE=$base_image" -t "$target_image" "$build_dir"
