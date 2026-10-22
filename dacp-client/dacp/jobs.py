"""Closable, incremental operator/job result streams over Flight."""
from contextlib import suppress
import base64
import json
import struct
from urllib.parse import urlsplit
import pyarrow as pa
import pyarrow.flight as flight
from . import dacp_pb2 as pb
from .source import _flight_client


class JobStream:
    """Iterator of metadata dictionaries with a bytes ``data`` field. Use with a context manager."""
    def __init__(self, client, writer, reader, on_retry=None):
        self.client, self.writer, self.reader = client, writer, reader
        self.ports = {}
        self.on_retry = on_retry
        self.execution_id = None
        self.complete = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed:
            raise StopIteration
        try:
            while True:
                try:
                    chunk = self.reader.read_chunk()
                except StopIteration:
                    if not self.complete:
                        raise RuntimeError('job result stream truncated')
                    self.close()
                    raise
                batch = chunk.data
                if batch is None:
                    continue
                if batch.schema != pa.schema([('data', pa.binary())]) or batch.num_rows != 1:
                    raise ValueError('invalid job stream envelope')
                raw = batch.column(0)[0].as_py()
                if raw is None or len(raw) < 8:
                    raise ValueError('invalid operator frame')
                header_size, size = struct.unpack('>II', raw[:8])
                if not 0 < header_size <= 65536 or size > 8*1024*1024 or len(raw) != 8+header_size+size:
                    raise ValueError('invalid operator frame lengths')
                event = json.loads(raw[8:8+header_size])
                event['data'] = raw[8+header_size:]
                kind, port = event.get('type'), event.get('port')
                self.execution_id = event.get('execution_id', self.execution_id)
                if self.complete:
                    raise ValueError('event after completion')
                if kind == 'attempt_failed':
                    if self.on_retry is None:
                        raise ValueError('retry notification requires on_retry callback')
                    self.on_retry(event)
                    self.ports.clear()
                    continue
                if kind == 'execution':
                    if not event.get('execution_id') or self.ports:
                        raise ValueError('invalid execution metadata')
                elif kind == 'complete':
                    if any(state['ended'] is False for state in self.ports.values()):
                        raise ValueError('incomplete output port')
                    self.complete = True
                elif kind == 'start':
                    if not port or port in self.ports or event.get('representation') not in ('bytes', 'records'):
                        raise ValueError('invalid output start')
                    schema = None
                    if event['representation'] == 'records':
                        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(event['schema'], validate=True)))
                    self.ports[port] = dict(ended=False, schema=schema, metadata=event.copy())
                elif kind in ('data', 'end'):
                    if port not in self.ports or self.ports[port]['ended']:
                        raise ValueError('unexpected output port event')
                    if kind == 'end':
                        self.ports[port]['ended'] = True
                    elif self.ports[port]['schema'] is not None:
                        with pa.ipc.open_stream(event['data']) as records:
                            if records.schema != self.ports[port]['schema']:
                                raise ValueError('output schema changed')
                            batches = list(records)
                            if len(batches) != 1:
                                raise ValueError('expected one record batch per frame')
                            event['batch'] = batches[0]
                else:
                    raise ValueError('unknown output event')
                return event
        except BaseException:
            self.close()
            raise

    def close(self):
        if not self.closed:
            self.closed = True
            # Cancellation/transport errors during cleanup must not replace the original failure.
            with suppress(Exception):
                self.reader.cancel()
            with suppress(Exception):
                self.writer.close()
            self.client.close()


class Job:
    @staticmethod
    def run(node, yaml, *, tls_root_certs=None, cert_chain=None, private_key=None, timeout=3600,
            execution_policy=None, on_retry=None, replay_from=None):
        u = urlsplit(node)
        if (u.scheme != 'dacp' or not u.hostname or u.username or u.password
                or u.path not in ('', '/') or u.query or u.fragment):
            raise ValueError('expected node origin dacp://host[:port]')
        if execution_policy is not None and not isinstance(execution_policy, dict):
            raise TypeError('execution_policy must be a dict')
        if on_retry is not None and not callable(on_retry):
            raise TypeError('on_retry must be callable')
        body = pb.OperatorRequest(job_yaml=yaml, retry_aware=on_retry is not None,
                                  replay_from=replay_from or '',
                                  execution_policy_json=(json.dumps(execution_policy).encode()
                                                         if execution_policy is not None else b'')).SerializeToString()
        if len(body) > 1024*1024:
            raise ValueError('control request exceeds 1 MiB')
        client = _flight_client(u, tls_root_certs, cert_chain, private_key)
        try:
            writer, reader = client.do_exchange(flight.FlightDescriptor.for_command(body),
                                               options=flight.FlightCallOptions(timeout=timeout))
            writer.begin(pa.schema([('data', pa.binary())]))
            writer.done_writing()
            return JobStream(client, writer, reader, on_retry)
        except BaseException:
            client.close()
            raise

    @staticmethod
    def replay(node, execution_id, **options):
        """Replay the original job using its committed materialized boundaries."""
        return Job.run(node, '', replay_from=execution_id, **options)
