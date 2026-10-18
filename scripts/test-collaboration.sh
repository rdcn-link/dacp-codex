#!/usr/bin/env bash
# Requires the built operator image, sbt, Python venv and access to Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
project_root="$PWD"
python_bin="${DACP_PYTHON:-$project_root/.venv/bin/python}"
bash scripts/certs.sh
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
node_a="$project_root/examples/multi-node/node-a.yaml"
node_b="$project_root/examples/multi-node/node-b.yaml"
if [[ "${DACP_TEST_REMOTE_REPOSITORY:-0}" == "1" ]]; then
  (cd "$project_root/dacp-operator-repository" && sbt -batch stage)
  repository_bin="$project_root/dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository"
  "$repository_bin" seed --operators "$project_root/operators" --root "$logs/catalog"
  "$repository_bin" serve --root "$logs/catalog" --port 8820 > "$logs/repository.log" 2>&1 &
  repository_pid=$!
  for node in a b; do
    cp "$project_root/examples/multi-node/node-$node.yaml" "$logs/node-$node.yaml"
    sed -i '/^local_operator_library:/d' "$logs/node-$node.yaml"
    cat >> "$logs/node-$node.yaml" <<EOF
remote_operator_library_urls: [http://127.0.0.1:8820]
local_operator_library: /nonexistent/dacp-operators
operator_cache: $logs/cache-$node
EOF
  done
  node_a="$logs/node-a.yaml"
  node_b="$logs/node-b.yaml"
  "$python_bin" - <<'WAIT_REPOSITORY'
import time, urllib.request
for _ in range(100):
    try:
        urllib.request.urlopen("http://127.0.0.1:8820/health").close()
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
from dacp import Source
for _ in range(300):
    try:
        with Source.open("dacp://localhost:8815/health") as ds:
            list(ds._client.list_actions())
        with Source.open("dacp://localhost:8816/csv_data") as ds:
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
