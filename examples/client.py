import os
from dacp import Source

uri = os.environ.get("DACP_URI", "dacp://localhost:8815/data1")
with Source.open(uri) as ds:
    for item in ds.list():
        print(item.path, item.format, item.schema)
    for batch in ds.open("nested/orders.csv").as_records():
        print(batch.to_pydict())
    with ds.open("pixel.png").as_bytes() as stream:
        assert stream.read(8) == b"\x89PNG\r\n\x1a\n"
        print("PNG remaining bytes:", len(stream.read()))
