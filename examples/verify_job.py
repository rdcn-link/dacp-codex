"""Submit the example job and verify its streamed CSV output; use DACP_CA/CERT/KEY."""
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as csv
from dacp import Job

with Job.run('dacp://localhost:8815', Path(__file__).with_name('convert.yaml').read_text()) as events:
    payload = bytearray()
    completed = False
    for event in events:
        if event['type'] == 'data':
            payload.extend(event['data'])
        if event['type'] == 'complete':
            completed = True
    assert completed
    table = csv.read_csv(pa.BufferReader(payload))
    assert table.num_rows == 2
    print('Streamed result:', table.schema, 'rows=2')
