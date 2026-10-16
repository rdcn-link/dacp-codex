import pytest
from dacp import Job
from dacp import dacp_pb2 as pb
from dacp import jobs
import pyarrow as pa

class Client:
    def __init__(self, error=None):
        self.calls = 0
        self.closed = False
        self.error = error
    def do_action(self, action, options):
        self.calls += 1
        assert action.type == "dacp.run_job"
        assert pb.RunJobRequest.FromString(action.body.to_pybytes()).yaml == "job: demo"
        if self.error:
            raise self.error
        class Result:
            body = pa.py_buffer(pb.RunJobResult(outputs={"s1": "dacp://node-b:8816/s1"}).SerializeToString())
        return iter([Result()])
    def close(self): self.closed = True

def test_job_result_and_connection_cleanup(monkeypatch):
    client = Client()
    monkeypatch.setattr(jobs, "_flight_client", lambda *args: client)
    assert Job.run("dacp://node-a", "job: demo") == {"s1": "dacp://node-b:8816/s1"}
    assert client.calls == 1 and client.closed

def test_job_failure_is_not_retried(monkeypatch):
    client = Client(RuntimeError("worker failed"))
    monkeypatch.setattr(jobs, "_flight_client", lambda *args: client)
    with pytest.raises(RuntimeError, match="worker failed"):
        Job.run("dacp://node-a", "job: demo")
    assert client.calls == 1 and client.closed

@pytest.mark.parametrize("node", ["https://node", "dacp://node/dataset", "dacp://user:secret@node", "dacp://node?token=x"])
def test_job_requires_node_origin(node):
    with pytest.raises(ValueError): Job.run(node, "job: demo")
