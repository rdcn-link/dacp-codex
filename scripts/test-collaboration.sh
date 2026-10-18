#!/usr/bin/env bash
# Requires the built operator image, sbt, Python venv and access to Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
project_root="$PWD"
python_bin="${DACP_PYTHON:-$project_root/.venv/bin/python}"
bash scripts/certs.sh
port_a="${DACP_TEST_PORT_A:-18815}"
port_b="${DACP_TEST_PORT_B:-18816}"
repository_port="${DACP_TEST_REPOSITORY_PORT:-18820}"
export DACP_NODE_A="dacp://localhost:$port_a" DACP_NODE_B="dacp://localhost:$port_b"
(cd dacp-server && sbt -batch stage)
logs=$(mktemp -d "${TMPDIR:-/tmp}/dacp-collaboration-XXXXXX")
a_pid=""
b_pid=""
repository_pid=""
cleanup() {
  if [[ -n "$repository_pid" ]]; then kill "$repository_pid" 2>/dev/null || true; wait "$repository_pid" 2>/dev/null || true; fi
  if [[ -n "$a_pid" ]]; then kill "$a_pid" 2>/dev/null || true; wait "$a_pid" 2>/dev/null || true; fi
  if [[ -n "$b_pid" ]]; then kill "$b_pid" 2>/dev/null || true; wait "$b_pid" 2>/dev/null || true; fi
  echo "Node logs: $logs"
}
trap cleanup EXIT
for node in a b; do
  sed -e "s/8815/$port_a/g" -e "s/8816/$port_b/g" "$project_root/examples/multi-node/node-$node.yaml" > "$logs/node-$node.yaml"
done
node_a="$logs/node-a.yaml"
node_b="$logs/node-b.yaml"
if [[ "${DACP_TEST_REMOTE_REPOSITORY:-0}" == "1" ]]; then
  (cd "$project_root/dacp-operator-repository" && sbt -batch stage)
  repository_bin="$project_root/dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository"
  "$repository_bin" seed --operators "$project_root/operators" --root "$logs/catalog"
  "$repository_bin" serve --root "$logs/catalog" --port "$repository_port" > "$logs/repository.log" 2>&1 &
  repository_pid=$!
  for node in a b; do
    sed -i '/^local_operator_library:/d' "$logs/node-$node.yaml"
    cat >> "$logs/node-$node.yaml" <<EOF
remote_operator_library_urls: [http://127.0.0.1:$repository_port]
local_operator_library: /nonexistent/dacp-operators
operator_cache: $logs/cache-$node
EOF
  done
  node_a="$logs/node-a.yaml"
  node_b="$logs/node-b.yaml"
  export DACP_TEST_REPOSITORY_ORIGIN="http://127.0.0.1:$repository_port"
  "$python_bin" - <<'WAIT_REPOSITORY'
import os, time, urllib.request
for _ in range(100):
    try:
        urllib.request.urlopen(os.environ["DACP_TEST_REPOSITORY_ORIGIN"]+"/health").close()
        break
    except OSError:
        time.sleep(.1)
else:
    raise RuntimeError("operator repository did not start")
WAIT_REPOSITORY
fi
cd dacp-server
target/universal/stage/bin/dacp-server "$node_a" > "$logs/node-a.log" 2>&1 &
a_pid=$!
target/universal/stage/bin/dacp-server "$node_b" > "$logs/node-b.log" 2>&1 &
b_pid=$!
cd "$project_root"
export DACP_CA="$project_root/examples/certs/ca.crt"
export DACP_CERT="$project_root/examples/certs/client.crt"
export DACP_KEY="$project_root/examples/certs/client.key"
"$python_bin" - <<'PY'
import time
import os
from dacp import Source
for _ in range(300):
    try:
        with Source.open(os.environ["DACP_NODE_A"]+"/health") as ds:
            ds._client.wait_for_available(timeout=1)
        with Source.open(os.environ["DACP_NODE_B"]+"/csv_data") as ds:
            ds.list()
        break
    except Exception:
        time.sleep(.1)
else:
    raise RuntimeError("nodes did not become ready; inspect node logs")
PY
kill -0 "$a_pid"
kill -0 "$b_pid"
"$python_bin" examples/multi-node/check.py

if [[ "${DACP_TEST_REMOTE_REPOSITORY:-0}" == "1" ]]; then
  test "$(find "$logs/cache-a" "$logs/cache-b" -path "*/manifests/*" -name "*.json" | wc -l)" -ge 2
  echo "Remote operator download, verification, cache and local container execution passed"
fi
