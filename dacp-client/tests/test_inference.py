import yaml
import json
from types import SimpleNamespace
import pyarrow as pa
import pytest
from dacp import Job, Union
from dacp import inference


def test_job_inference_sends_resolved_workflow_without_stream_exchange(monkeypatch):
    calls = []
    class Client:
        closed = False
        def do_action(self, action, options):
            calls.append((action.type, json.loads(action.body.to_pybytes())))
            return iter([SimpleNamespace(body=pa.py_buffer(b'{"tasks":{},"outputs":{}}'))])
        def close(self): self.closed = True
    client = Client()
    monkeypatch.setattr(inference, '_flight_client', lambda *args: client)
    assert Job.infer('dacp://localhost', 'params: {limit: $LIMIT}', variables={'LIMIT': 123}) == {'tasks': {}, 'outputs': {}}
    assert calls[0][0] == 'dacp.infer_job'
    assert yaml.safe_load(calls[0][1]['job_yaml'])['params']['limit'] == 123
    assert client.closed


def test_union_uses_same_public_inference_action():
    client = Union.__new__(Union)
    client.timeout = 30
    calls = []
    def action(request, options):
        calls.append(request.type)
        return [SimpleNamespace(body=pa.py_buffer(b'{"outputs":{}}'))]
    client.client = SimpleNamespace(do_action=action)
    assert client.infer('name: test') == {'outputs': {}}
    assert calls == ['dacp.infer_job']


def test_malformed_or_duplicate_inference_response_rejected():
    client = SimpleNamespace(do_action=lambda *args, **kwargs: [])
    with pytest.raises(ValueError, match='invalid inference response'):
        inference.infer_client(client, b'{}')
    client.do_action = lambda *args, **kwargs: [SimpleNamespace(body=pa.py_buffer(b'{}'))]*2
    with pytest.raises(ValueError, match='invalid inference response'):
        inference.infer_client(client, b'{}')
