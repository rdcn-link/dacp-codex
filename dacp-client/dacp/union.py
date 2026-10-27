"""Ordinary Flight client for one DACP Union: TLS and a user bearer token."""
from .variables import resolve_variables
from . import yaml_document
import base64
from contextlib import suppress
import json
import struct
from urllib.parse import urlsplit
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight
from .source import _flight_client
from .flight_control import FlightControl


class Union(FlightControl):
    def __init__(self, url, *, token=None, ca=None, cert=None, key=None, timeout=1800):
        origin = urlsplit(url)
        if origin.scheme != 'dacp' or not origin.hostname or origin.path not in ('', '/') or origin.query or origin.fragment or origin.username is not None or origin.port == 0:
            raise ValueError('expected DACP origin')
        if timeout <= 0:
            raise ValueError('timeout must be positive')
        self.timeout = timeout
        if (cert is None) != (key is None):
            raise ValueError('client certificate and key must be provided together')
        self.client = _flight_client(origin, Path(ca).read_bytes() if ca else None,
                                     Path(cert).read_bytes() if cert else None, Path(key).read_bytes() if key else None,
                                     **({'token': token} if token is not None else {}))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.client.close()

    def infer(self, job_yaml, *, variables=None):
        from .inference import infer_client
        raw = json.dumps({'job_yaml': resolve_variables(job_yaml, variables)}).encode()
        return infer_client(self.client, raw, workflow=True, timeout=self.timeout)

    def submit(self, job_yaml, *, execution_policy=None, variables=None):
        body = {'job_yaml': resolve_variables(job_yaml, variables)}
        if execution_policy is not None:
            if not isinstance(execution_policy, dict):
                raise ValueError('execution_policy must be a mapping')
            body['execution_policy_yaml'] = yaml_document.dump(execution_policy)
        result = self._action('submit', body)
        return UnionExecution(self, result['job_id'])

    def replay(self, job_id, *, tasks=None):
        body = {'job_id': job_id}
        if tasks is not None:
            body['tasks'] = list(tasks)
        result = self._action('replay_job', body)
        return UnionExecution(self, result['job_id'])


def _values(reader, name):
    expected = pa.schema([(name, pa.binary())])
    while True:
        try:
            batch = reader.read_chunk().data
        except StopIteration:
            return
        if batch is None or batch.schema != expected or batch.num_rows != 1:
            raise ValueError('invalid Union Flight envelope')
        value = batch.column(0)[0].as_py()
        if value is None or len(value) > 9*1024*1024:
            raise ValueError('invalid Union payload')
        yield value


class UnionExecution:
    def __init__(self, union, job_id):
        if not isinstance(job_id, str) or not job_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in job_id):
            raise ValueError('invalid job id')
        self.union, self.job_id = union, job_id
        self.complete = False
        self.closed = False
        self._streams = set()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def replay(self, *, tasks=None):
        return self.union.replay(self.job_id, tasks=tasks)

    def status(self):
        return self.union._action('job_status', {'job_id': self.job_id})

    def cancel(self):
        return self.union._action('cancel_job', {'job_id': self.job_id})

    def close(self):
        if not self.closed:
            self.closed = True
            for connection in tuple(self._streams):
                connection.cancel()
            if not self.complete:
                with suppress(Exception):
                    self.cancel()

    def events(self, after=0):
        """Closing the event subscription alone does not cancel execution."""
        if self.closed or not isinstance(after, int) or after < 0:
            raise ValueError('invalid event cursor or closed execution')
        reader = self.union._stream('events', {'job_id': self.job_id, 'after': after})
        self._streams.add(reader)
        try:
            for raw in _values(reader, 'event'):
                if len(raw) > 1024*1024:
                    raise ValueError('invalid event size')
                yield json.loads(raw)
        finally:
            self._streams.discard(reader)
            reader.cancel()

    def results(self, *, on_retry=None):
        """Yield start/data/end/complete; records data also includes a PyArrow batch.

        With automatic retries, on_retry is required and must discard partial results.

        Close this iterator on early exit, or use the execution as a context manager.
        """
        if self.closed:
            raise ValueError('execution is closed')
        if on_retry is not None and not callable(on_retry):
            raise ValueError('on_retry must be callable')
        body = {'job_id': self.job_id}
        if on_retry is not None:
            body['retry_aware'] = True
        reader = self.union._stream('results', body)
        self._streams.add(reader)
        ports = {}
        values = _values(reader, 'data')
        try:
            for raw in values:
                if len(raw) < 8:
                    raise ValueError('invalid operator frame size')
                header_size, payload_size = struct.unpack('>II', raw[:8])
                if not 0 < header_size <= 65536 or payload_size > 8*1024*1024 or len(raw) != 8+header_size+payload_size:
                    raise ValueError('invalid operator frame size')
                event = json.loads(raw[8:8+header_size])
                event['data'] = raw[8+header_size:]
                kind, port = event.get('type'), event.get('port')
                if kind == 'attempt_failed':
                    if on_retry is None:
                        raise RuntimeError('retry requires an on_retry callback')
                    on_retry(event)
                    ports.clear()
                    continue
                if kind == 'error':
                    raise RuntimeError(event.get('message', 'Union execution failed'))
                if kind == 'complete':
                    if any(not p['ended'] for p in ports.values()):
                        raise ValueError('incomplete output')
                    if next(values, None) is not None:
                        raise ValueError("events after completion")
                    self.complete = True
                    yield event
                    return
                if kind == 'start':
                    if not port or port in ports or event.get('representation') not in ('bytes', 'records'):
                        raise ValueError('invalid output start')
                    schema = None
                    if event['representation'] == 'records':
                        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(event['schema'], validate=True)))
                    ports[port] = {'ended': False, 'schema': schema}
                elif kind in ('data', 'end'):
                    if port not in ports or ports[port]['ended']:
                        raise ValueError('invalid output event')
                    if kind == 'end':
                        ports[port]['ended'] = True
                    elif ports[port]['schema'] is not None:
                        with pa.ipc.open_stream(event['data']) as ipc_reader:
                            if ipc_reader.schema != ports[port]['schema']:
                                raise ValueError('output schema changed')
                            batches = list(ipc_reader)
                            if len(batches) != 1:
                                raise ValueError('expected one record batch')
                            event['batch'] = batches[0]
                else:
                    raise ValueError('unknown output event')
                yield event
            raise RuntimeError("Union result stream truncated")
        finally:
            self._streams.discard(reader)
            reader.cancel()
            if not self.complete:
                with suppress(Exception):
                    self.cancel()
