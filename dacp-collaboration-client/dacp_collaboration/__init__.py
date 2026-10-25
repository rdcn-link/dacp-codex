"""Union-to-member service SDK, independently packaged from ordinary clients."""
from pathlib import Path
from urllib.parse import urlsplit
from dacp.source import _flight_client
from dacp.flight_control import FlightControl

class CollaborationClient(FlightControl):
    def __init__(self, url, *, ca, cert, key, credential_file=None, timeout=1800):
        origin = urlsplit(url)
        if origin.scheme != 'dacp' or not origin.hostname or origin.path not in ('', '/') or origin.query or origin.fragment or origin.username:
            raise ValueError('expected DACP origin')
        if timeout <= 0:
            raise ValueError('timeout must be positive')
        self.timeout = timeout
        self.client = _flight_client(origin, Path(ca).read_bytes(), Path(cert).read_bytes(), Path(key).read_bytes(),
                                     token=Path(credential_file).read_text().strip() if credential_file else None, use_env_token=False)
    def membership(self, union_id): return self._action('membership', {'union_id': union_id})
    def node_status(self, union_id): return self._action('node_status', {'union_id': union_id})
    def prepare(self, plan): return self._action('prepare', plan)
    def start(self, execution_id): return self._action('start', {'execution_id': execution_id})
    def status(self, execution_id): return self._action('execution_status', {'execution_id': execution_id})
    def cancel(self, execution_id): return self._action('cancel_execution', {'execution_id': execution_id})
    def output(self, execution_id, task, port):
        return self._stream('output', {'execution_id': execution_id, 'task': task, 'port': port})
    def materialized_status(self, union_id, execution_id, task):
        return self._action('materialized_status', {'union_id': union_id, 'execution_id': execution_id, 'task': task})
    def close(self): self.client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
