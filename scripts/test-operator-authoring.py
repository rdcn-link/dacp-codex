#!/usr/bin/env python3
"""Run the guide's records operator locally and verify its binary output contract."""
import os
import base64
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

import pyarrow as pa

os.environ['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'dacp-operator-sdk/python') + os.pathsep + os.environ.get('PYTHONPATH', '')

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

MULTIPORT = OPERATOR.parents[2] / 'split_orders/1.0/split_orders.py'
with tempfile.TemporaryDirectory(prefix='dacp-multiport-') as temporary:
    directory = Path(temporary)
    for name, ids, amounts in [('left', [1, 2], [12.5, 30.0]), ('right', [3, 4, 5], [60.0, 8.0, None])]:
        with (directory / (name + '.arrow')).open('wb') as output:
            with pa.ipc.new_stream(output, schema) as writer:
                writer.write_batch(pa.record_batch([ids, amounts], schema=schema))
    # Reverse manifest order intentionally: operator must dispatch by name and sort input ports.
    manifest = [dict(port=p, file=p + '.arrow', representation='records', format='csv') for p in ['right', 'left']]
    (directory / 'manifest.json').write_text(json.dumps(manifest))
    (directory / 'params.json').write_text(json.dumps(dict(min_amount=20)))
    def execute():
        return subprocess.run([sys.executable, str(MULTIPORT), 'operator', str(directory),
                               str(directory / 'params.json')], capture_output=True, check=False)
    result = execute()
    assert result.returncode == 0, result.stderr.decode()
    events = list(frames(result.stdout))
    assert [(h['port'], h['type']) for h, _ in events[:2]] == [('accepted', 'start'), ('rejected', 'start')]
    assert [(h['port'], h['type']) for h, _ in events[-2:]] == [('accepted', 'end'), ('rejected', 'end')]
    output = dict(accepted=[], rejected=[])
    for header, payload in events[2:-2]:
        assert header['type'] == 'data'
        with pa.ipc.open_stream(payload) as reader:
            assert reader.schema == schema
            batches = list(reader)
            assert len(batches) == 1
            output[header['port']].extend(batches[0].column('id').to_pylist())
    assert output == dict(accepted=[2, 3], rejected=[1, 4, 5])
    (directory / 'params.json').write_text(json.dumps(dict(min_amount=1000)))
    empty = execute()
    assert empty.returncode == 0
    assert not any(h['type'] == 'data' and h['port'] == 'accepted' for h, _ in frames(empty.stdout))
    (directory / 'manifest.json').write_text(json.dumps(manifest[:1]))
    assert execute().returncode != 0
print('Multiport example passed: named inputs, two output schemas, frame order, nulls and empty port')
