import io
import json
import struct
import pytest
import pyarrow as pa
from types import SimpleNamespace
from dacp.union import Union, UnionExecution


def frame(kind, data=b'', **metadata):
    header = json.dumps(dict(type=kind, **metadata)).encode()
    return struct.pack('>II', len(header), len(data)) + header + data


class Connection:
    def __init__(self):
        self.closed = False

    def cancel(self):
        self.closed = True


class FakeUnion:
    def __init__(self, raw):
        self.raw = raw
        self.connections = []
        self.calls = []

    def _stream(self, kind, body):
        connection = Connection()
        raw = self.raw
        frames = []
        if kind == 'events':
            frames = [raw]
        else:
            while raw:
                if len(raw) < 8:
                    frames.append(raw)
                    break
                h, n = struct.unpack('>II', raw[:8])
                size = 8+h+n
                frames.append(raw[:size])
                raw = raw[size:]
        iterator = iter(frames)
        def read_chunk():
            value = next(iterator)
            return SimpleNamespace(data=pa.record_batch([pa.array([value], type=pa.binary())], names=['event' if kind == 'events' else 'data']))
        connection.read_chunk = read_chunk
        self.connections.append(connection)
        return connection

    def _action(self, name, body):
        self.calls.append((name, body))
        return {'state': 'cancelled'}


def test_incremental_results_preserve_payload_and_complete():
    union = FakeUnion(frame('start', port='r', representation='bytes', format='binary') + frame('data', b'abc', port='r') + frame('end', port='r') + frame('complete'))
    with UnionExecution(union, 'job1') as execution:
        output = list(execution.results())
    assert output[1]['data'] == b'abc'
    assert output[-1]['type'] == 'complete'
    assert union.connections[0].closed
    assert not union.calls


@pytest.mark.parametrize('raw', [b'', frame('data', b'bad', port='r'), frame('complete'), frame('error', message='node failed'), struct.pack('>II', 65537, 0)])
def test_invalid_or_truncated_results_cancel_the_job(raw):
    union = FakeUnion(raw)
    with pytest.raises((ValueError, RuntimeError)):
        list(UnionExecution(union, 'job1').results())
    assert union.calls[-1] == ('cancel_job', {'job_id': 'job1'})
    assert union.connections[0].closed


def test_early_iterator_close_cancels_results_but_events_do_not_cancel():
    union = FakeUnion(frame('start', port='r', representation='bytes', format='binary'))
    stream = UnionExecution(union, 'job1').results()
    next(stream)
    stream.close()
    assert union.calls[-1] == ('cancel_job', {'job_id': 'job1'})
    union = FakeUnion(b'{"type":"gap","sequence":20,"snapshot":{}}\n')
    stream = UnionExecution(union, 'job1').events(1)
    assert next(stream)['type'] == 'gap'
    stream.close()
    assert not union.calls
    assert union.connections[0].closed


def test_context_exit_closes_active_stream_and_cancels():
    union = FakeUnion(frame('start', port='r', representation='bytes', format='binary'))
    with UnionExecution(union, 'job1') as execution:
        stream = execution.results()
        next(stream)
    assert union.connections[0].closed
    assert union.calls[-1] == ('cancel_job', {'job_id': 'job1'})
    stream.close()


@pytest.mark.parametrize('url', ['http://localhost', 'https://localhost/path', 'https://user@localhost', 'https://localhost?query', 'dacp://@localhost', 'dacp://localhost:0', 'dacp://localhost:65536', 'dacp://localhost/path'])
def test_union_requires_dacp_origin(url):
    with pytest.raises(ValueError):
        Union(url, cert='unused', key='unused', ca='unused')


def test_complete_requires_successful_flight_trailer():
    union = FakeUnion(frame('start', port='r', representation='bytes', format='binary') + frame('end', port='r') + frame('complete'))
    original = union._stream
    def failing_stream(*args):
        reader = original(*args)
        read_chunk = reader.read_chunk
        def next_chunk():
            try:
                return read_chunk()
            except StopIteration:
                raise RuntimeError('Flight transport failed after complete')
        reader.read_chunk = next_chunk
        return reader
    union._stream = failing_stream
    execution = UnionExecution(union, 'job1')
    with pytest.raises(RuntimeError, match='transport failed'):
        list(execution.results())
    assert not execution.complete
    assert union.calls[-1][0] == 'cancel_job'


def test_record_results_validate_schema_and_close_flight_reader():
    import base64
    batch = pa.record_batch([pa.array([3, 7], type=pa.int64())], names=['count'])
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, batch.schema) as writer:
        writer.write_batch(batch)
    schema = base64.b64encode(batch.schema.serialize().to_pybytes()).decode()
    union = FakeUnion(frame('start', port='r', representation='records', format='arrow', schema=schema) + frame('data', output.getvalue().to_pybytes(), port='r') + frame('end', port='r') + frame('complete'))
    execution = UnionExecution(union, 'job1')
    results = list(execution.results())
    assert results[1]['batch'].equals(batch)
    assert execution.complete
    assert union.connections[0].closed
    assert not union.calls
