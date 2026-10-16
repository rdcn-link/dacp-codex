#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
project_root="$PWD"
python_bin="${DACP_PYTHON:-$project_root/.venv/bin/python}"
bash scripts/certs.sh
(cd dacp-server && sbt -batch stage)
logs=$(mktemp -d "${TMPDIR:-/tmp}/dacp-sql-XXXXXX")
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  echo "Server log: $logs/server.log"
}
trap cleanup EXIT
cd dacp-server
target/universal/stage/bin/dacp-server ../examples/sql/server.yaml > "$logs/server.log" 2>&1 &
server_pid=$!
cd "$project_root"
export DACP_CA="$project_root/examples/certs/ca.crt"
export DACP_CERT="$project_root/examples/certs/client.crt"
export DACP_KEY="$project_root/examples/certs/client.key"
"$python_bin" - <<'PY'
import time
from decimal import Decimal
import pyarrow as pa
from dacp import Source
for _ in range(300):
    try:
        with Source.open("dacp://localhost:8815/sales") as ds:
            items = ds.list()
        break
    except Exception:
        time.sleep(.1)
else:
    raise RuntimeError("server did not become ready; inspect log")
assert {i.path for i in items} == {"ORDERS", "CUSTOMERS"}
assert all(i.format == "arrow" and i.schema is not None for i in items)
with Source.open("dacp://localhost:8815/sales/ORDERS") as stream:
    records = pa.Table.from_batches(list(stream.as_records()))
assert records.num_rows == 2
assert sorted(records.column("AMOUNT").to_pylist()) == [Decimal("12.50"), Decimal("30.00")]
with Source.open("dacp://localhost:8815/sales") as ds:
    with ds.open("ORDERS").as_bytes() as stream:
        byte_table = pa.ipc.open_stream(stream).read_all()
    assert byte_table.equals(records)
    try:
        ds.open("ORDERS; DROP TABLE ORDERS")
    except Exception:
        pass
    else:
        raise AssertionError("invalid table accepted")
    assert len(ds.list()) == 2
with Source.open("dacp://localhost:8815/files") as ds:
    assert "nested/orders.csv" in {i.path for i in ds.list()}
    assert sum(b.num_rows for b in ds.open("nested/orders.csv").as_records()) == 2
    with ds.open("pixel.png").as_bytes() as stream:
        assert stream.read(8) == b"\x89PNG\r\n\x1a\n"
print("PASS: one mTLS DACP node serves directory files and typed SQL tables through records and bytes")
PY
kill -0 "$server_pid"
"$python_bin" examples/sql/client.py
