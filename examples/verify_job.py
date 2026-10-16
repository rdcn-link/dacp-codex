import pyarrow.parquet as pq
from dacp import Source
for name in ("s1", "s2"):
    with Source.open(f"dacp://localhost:8815/{name}") as ds:
        items = ds.list()
        assert items and all(i.format == "parquet" and i.schema is not None for i in items)
        for item in items:
            stream = ds.open(item.path)
            batches = list(stream.as_records())
            assert sum(b.num_rows for b in batches) == 2
            with stream.as_bytes() as raw:
                assert raw.read(4) == b"PAR1"
            print(name, item.path, item.schema, "rows=2")
