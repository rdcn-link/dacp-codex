"""DACP byte-preserving operator. Dependencies live in the shared runtime image."""
import json
from pathlib import Path
import re
import sys

import pyarrow as pa


def main():
    if len(sys.argv) != 5 or sys.argv[1] != "operator":
        raise ValueError("expected operator INPUT OUTPUT PARAMS")
    source, target = map(Path, sys.argv[2:4])
    params = json.loads(Path(sys.argv[4]).read_text())
    if not isinstance(params, dict) or params:
        raise ValueError("identity_python accepts no parameters")
    result = []
    schema = pa.schema([("data", pa.binary())])
    for index, item in enumerate(json.loads((source / "manifest.json").read_text())):
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", item["format"]):
            raise ValueError("invalid format")
        name = f"_stream-{index}.arrow"
        with (source / item["file"]).open("rb") as raw:
            with pa.ipc.open_stream(raw) as reader:
                if reader.schema != schema:
                    raise ValueError("expected Arrow bytes schema")
                with (target / name).open("xb") as sink:
                    with pa.ipc.new_stream(sink, schema) as writer:
                        for batch in reader:
                            writer.write_batch(batch)
        # IPC EOS terminates FIFO reads; do not wait for operating-system EOF.
        extension = {"excel": "xlsx", "sql": "bin"}.get(item["format"], item["format"])
        result.append({"file": name, "path": f"part-{index}.{extension}", "format": item["format"]})
    (target / "_output.json").write_text(json.dumps(result))


if __name__ == "__main__":
    main()
