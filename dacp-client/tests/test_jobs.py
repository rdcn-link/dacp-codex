import json
import struct
from types import SimpleNamespace
import pytest
import pyarrow as pa
from dacp import Job
from dacp import jobs
from dacp import dacp_pb2 as pb


def chunk(event, data=b''):
    header = json.dumps(event).encode()
    frame = struct.pack('>II', len(header), len(data)) + header + data
    return SimpleNamespace(data=pa.record_batch([pa.array([frame])], names=['data']))


class Reader:
    def __init__(self, events):
        self.events = iter(events)
        self.cancelled = False
    def read_chunk(self):
        value = next(self.events)
        if isinstance(value, Exception):
            raise value
        return value
    def cancel(self): self.cancelled = True


class Client:
    def __init__(self, events):
        self.reader = Reader(events)
        self.closed = False
        self.calls = 0
        self.writer = SimpleNamespace(begin=lambda schema: None, done_writing=lambda: None, close=lambda: None)
    def do_exchange(self, descriptor, options):
        self.calls += 1
        assert pb.OperatorRequest.FromString(descriptor.command).job_yaml == 'job: demo'
        return self.writer, self.reader
    def close(self): self.closed = True


def setup(monkeypatch, events):
    client = Client(events)
    monkeypatch.setattr(jobs, '_flight_client', lambda *args: client)
    return client


def test_job_stream_incremental_cleanup(monkeypatch):
    client = setup(monkeypatch, [chunk(dict(type='start',port='result',representation='bytes',format='text')),
                                chunk(dict(type='data',port='result'), b'hello'),
                                chunk(dict(type='end',port='result')), chunk(dict(type='complete'))])
    with Job.run('dacp://node-a', 'job: demo') as stream:
        assert next(stream)['type'] == 'start'
        assert not client.closed
        assert next(stream)['data'] == b'hello'
        assert [e['type'] for e in stream] == ['end', 'complete']
    assert client.closed and client.reader.cancelled and client.calls == 1


def test_early_close_cancels(monkeypatch):
    client = setup(monkeypatch, [])
    with Job.run('dacp://node-a', 'job: demo'):
        pass
    assert client.reader.cancelled and client.closed


@pytest.mark.parametrize('events', [[], [RuntimeError('worker failed')], [chunk(dict(type='data',port='unknown'),b'x')]])
def test_bad_stream_closes_without_retry(monkeypatch, events):
    client = setup(monkeypatch, events)
    with pytest.raises((RuntimeError, ValueError)):
        with Job.run('dacp://node-a', 'job: demo') as stream:
            list(stream)
    assert client.closed and client.calls == 1


@pytest.mark.parametrize('node', ['https://node','dacp://node/data','dacp://user:secret@node','dacp://node?token=x'])
def test_job_requires_origin(node):
    with pytest.raises(ValueError): Job.run(node, 'job: demo')
