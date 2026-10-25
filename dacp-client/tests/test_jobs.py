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
        self.request = pb.OperatorRequest.FromString(descriptor.command)
        assert self.request.job_yaml == getattr(self, 'expected_yaml', 'job: demo')
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


def test_retry_callback_resets_partial_output_and_exposes_execution(monkeypatch):
    start = dict(type='start', port='result', representation='bytes', format='text', execution_id='job1')
    client = setup(monkeypatch, [chunk(start), chunk(dict(type='data', port='result'), b'partial'),
        chunk(dict(type='attempt_failed', port='', retry_count=1)), chunk(start),
        chunk(dict(type='data', port='result'), b'final'), chunk(dict(type='end', port='result')), chunk(dict(type='complete'))])
    output = bytearray()
    seen = []
    def retry(event):
        seen.append(event['retry_count'])
        output.clear()
    with Job.run('dacp://node-a', 'job: demo', execution_policy={'max_retries': 1}, on_retry=retry) as stream:
        for event in stream:
            if event['type'] == 'data': output.extend(event['data'])
        assert stream.execution_id == 'job1'
    assert output == b'final' and seen == [1] and client.closed


def test_retry_callback_error_cancels_stream(monkeypatch):
    client = setup(monkeypatch, [chunk(dict(type='attempt_failed', port='', retry_count=1))])
    def retry(event): raise RuntimeError('abort')
    with pytest.raises(RuntimeError, match='abort'):
        list(Job.run('dacp://node-a', 'job: demo', on_retry=retry))
    assert client.reader.cancelled and client.closed


def test_policy_and_replay_are_separate_flight_fields(monkeypatch):
    client = setup(monkeypatch, [chunk(dict(type='execution', port='', execution_id='job1')), chunk(dict(type='complete'))])
    with Job.run('dacp://node-a', 'job: demo', execution_policy={'max_retries': 2, 'timeout_seconds': 120}, on_retry=lambda e: None) as stream:
        list(stream)
        assert stream.execution_id == 'job1'
    assert json.loads(client.request.execution_policy_json) == {'max_retries': 2, 'timeout_seconds': 120}
    assert client.request.retry_aware
    client = setup(monkeypatch, [chunk(dict(type='complete'))])
    client.expected_yaml = ''
    with Job.replay('dacp://node-a', 'job1', on_retry=lambda e: None) as stream:
        list(stream)
    assert client.request.replay_from == 'job1' and not client.request.execution_policy_json


def test_job_resolves_variables_before_flight_and_replay_cannot_override(monkeypatch):
    client = setup(monkeypatch, [])
    client.expected_yaml = json.dumps({'job': 'demo', 'tasks': [{'params': {'limit': 123}}]})
    with Job.run('dacp://node-a', 'job: demo\ntasks: [{params: {limit: $LIMIT}}]', variables={'LIMIT': 123}):
        pass
    assert json.loads(client.request.job_yaml)['tasks'][0]['params']['limit'] == 123
    with pytest.raises(ValueError, match='original resolved variables'):
        Job.replay('dacp://node-a', 'previous', variables={'LIMIT': 456})


def test_missing_variables_do_not_open_connection(monkeypatch):
    monkeypatch.setattr(jobs, '_flight_client', lambda *args: pytest.fail('must not connect'))
    with pytest.raises(ValueError, match='missing workflow variable'):
        Job.run('dacp://node-a', 'params: {limit: $LIMIT}', variables={})
