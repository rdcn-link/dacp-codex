#!/usr/bin/env python3
"""Run the guide's records operator locally and verify its binary output contract."""
import base64
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

import pyarrow as pa

OPERATOR = Path(__file__).resolve().parents[1] / 'examples/operators/python/filter_amount/1.0/filter_amount.py'


def run(directory, parameters):
    (directory / 'params.json').write_text(json.dumps(parameters))
    return subprocess.run([sys.executable, str(OPERATOR), 'operator', str(directory),
                           str(directory / 'params.json')], capture_output=True, check=False)


def frames(data):
    while data:
        header_size, payload_size = struct.unpack('>II', data[:8])
        assert header_size <= 65536 and payload_size <= 8 * 1024 * 1024
        end = 8 + header_size + payload_size
        assert end <= len(data)
        yield json.loads(data[8:8 + header_size]), data[8 + header_size:end]
        data = data[end:]


with tempfile.TemporaryDirectory(prefix='dacp-authoring-') as temporary:
    directory = Path(temporary)
    schema = pa.schema([('id', pa.int64()), ('amount', pa.float64())])
    with (directory / 'source.arrow').open('wb') as output:
        with pa.ipc.new_stream(output, schema) as writer:
            writer.write_batch(pa.record_batch([[1, 2], [12.5, 30.0]], schema=schema))
            writer.write_batch(pa.record_batch([[3, 4], [60.0, None]], schema=schema))
    (directory / 'manifest.json').write_text(json.dumps([
        dict(port='source', file='source.arrow', representation='records', format='csv')]))
    result = run(directory, dict(min_amount=20, factor=2))
    assert result.returncode == 0, result.stderr.decode()
    events = list(frames(result.stdout))
    assert [h['type'] for h, _ in events] == ['start', 'data', 'data', 'end']
    assert all(h['port'] == 'result' for h, _ in events)
    declared = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(events[0][0]['schema'])))
    batches = []
    for header, payload in events[1:-1]:
        with pa.ipc.open_stream(payload) as reader:
            assert reader.schema == declared
            batch = list(reader)
            assert len(batch) == 1
            batches.extend(batch)
    assert pa.Table.from_batches(batches).to_pydict() == dict(
        id=[2, 3], amount=[30.0, 60.0], scaled_amount=[60.0, 120.0])
    result = run(directory, dict(min_amount=1000))
    assert result.returncode == 0
    assert [h['type'] for h, _ in frames(result.stdout)] == ['start', 'end']
    assert run(directory, dict(factor='bad')).returncode != 0
    assert run(directory, dict(unexpected=1)).returncode != 0
    assert run(directory, dict(factor=True)).returncode != 0
    assert run(directory, {}).returncode == 0
print('Authoring example passed: parameters, batches, null filtering, output schema, empty output and errors')
