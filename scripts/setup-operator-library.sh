#!/usr/bin/env bash
# Build and publish the starter operators, then start the repository.
set -euo pipefail
cd "$(dirname "$0")/.."
project_root="$PWD"
# All remaining arguments are passed to `serve`, including TLS options.
# Supply --root once; seed and serve use the same directory.
catalog="$project_root/.operator-catalog"
serve_args=()
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--root" ]]; then
    if [[ $# -lt 2 ]]; then echo '--root requires a directory' >&2; exit 2; fi
    catalog="$2"
    shift 2
  else
    serve_args+=("$1")
    shift
  fi
done
docker build -f operators/format_convert/1.0/Dockerfile -t dacp/format-convert:1.0 .
(cd dacp-operator-library && sbt -batch stage)
library_bin="$project_root/dacp-operator-library/target/universal/stage/bin/dacp-operator-library"
"$library_bin" seed --operators "$project_root/operators" --root "$catalog"
exec "$library_bin" serve --root "$catalog" "${serve_args[@]}"
