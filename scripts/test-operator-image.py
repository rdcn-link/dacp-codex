#!/usr/bin/env python3
"""Exercise the actual operator container and Arrow IPC boundary (requires Docker/PyArrow)."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def check(image):
    with tempfile.TemporaryDirectory(prefix="dacp-operator-contract-") as temporary:
        base = Path(temporary)
        cases = [("csv", b"id,value\n1,2.5\n2,3.5\n"),
                 ("ndjson", b'{"id":1,"value":2.5}\n{"id":2,"value":3.5}\n')]
        for index, (format_name, payload) in enumerate(cases):
            source = base / f"input-{index}"
            source.mkdir()
            schema = pa.schema([("data", pa.binary())])
            with pa.OSFile(str(source / "input.arrow"), "wb") as raw:
                with pa.ipc.new_stream(raw, schema) as writer:
                    for chunk in (payload[:7], payload[7:]):
                        writer.write_batch(pa.record_batch([pa.array([chunk], type=pa.binary())], schema=schema))
            (source / "manifest.json").write_text(json.dumps([{"file": "input.arrow", "format": format_name}]))
            for compression in ("none", "snappy", "zstd"):
                output = base / f"output-{index}-{compression}"
                output.mkdir()
                (source / "params.json").write_text(json.dumps({"to": "parquet", "compression": compression,
                                                               "row_group_size": "1MB"}))
                run(image, "format_convert@1.1", source, output)
                manifest = json.loads((output / "_output.json").read_text())
                assert len(manifest) == 1 and manifest[0]["format"] == "parquet"
                table = pq.read_table(pa.BufferReader(decode(output / manifest[0]["file"])))
                assert table.to_pydict() == {"id": [1, 2], "value": [2.5, 3.5]}
                assert b"dacp.arrow.schema" in table.schema.metadata
                print(f"conversion {format_name}/{compression}: passed")
            output = base / f"identity-{index}"
            output.mkdir()
            run(image, "identity@1.1", source, output)
            manifest = json.loads((output / "_output.json").read_text())
            assert manifest[0]["format"] == format_name
            assert decode(output / manifest[0]["file"]) == payload
            print(f"identity {format_name}: passed")


def run(image, reference, source, output):
    subprocess.run(["docker", "run", "--rm", "--network", "none", "--read-only", "--tmpfs", "/tmp",
                    "-e", f"DACP_OPERATOR={reference}", "-v", f"{source}:/input:ro", "-v", f"{output}:/output",
                    image, "operator", "/input", "/output", "/input/params.json"], check=True)


def decode(path):
    with pa.memory_map(str(path), "r") as source:
        with pa.ipc.open_stream(source) as stream:
            return b"".join(value.as_py() for batch in stream for value in batch.column(0))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="dacp/format-convert:1.1")
    check(parser.parse_args().image)
