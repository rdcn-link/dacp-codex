#!/usr/bin/env python3
"""Export the canonical DACP metadata specification for the XStream connector."""
from pathlib import Path
import argparse
import hashlib
p = argparse.ArgumentParser(description=__doc__)
p.add_argument("output", type=Path, help="Connector proto snapshot path")
p.add_argument("--check", action="store_true", help="Fail if the existing snapshot differs")
a = p.parse_args()
spec = (Path(__file__).resolve().parents[1] / "proto/dacp.proto").read_text()
export = spec.replace("package dacp.protocol;", "package xstream.connector.dacp.protocol;", 1)
if a.check:
    if not a.output.is_file() or a.output.read_text() != export:
        raise SystemExit("Connector metadata specification is out of sync")
else:
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(export)
print("DACP metadata specification: " + hashlib.sha256(export.encode()).hexdigest())
