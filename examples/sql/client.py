"""Run from the repository root with DACP_CA, DACP_CERT and DACP_KEY set."""
import pyarrow as pa
from dacp import Source

with Source.open("dacp://localhost:8815/files") as ds:
    print("Files:", [item.path for item in ds.list()])
    for batch in ds.open("nested/orders.csv").as_records():
        print("CSV:", batch.to_pydict())

with Source.open("dacp://localhost:8815/sales") as ds:
    for item in ds.list():
        print("Table:", item.path, item.schema)
    for batch in ds.open("ORDERS").as_records():
        print("SQL:", batch.to_pydict())
    # SQL byte view is a streaming Arrow IPC serialization, not a database dump.
    with ds.open("ORDERS").as_bytes() as stream:
        for batch in pa.ipc.open_stream(stream):
            print("SQL through byte view:", batch.to_pydict())

with Source.open("dacp://localhost:8815/sales/CUSTOMERS") as stream:
    for batch in stream.as_records():
        print("Direct table URI:", batch.to_pydict())
