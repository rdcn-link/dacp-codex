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
    def __init__(self, client, writer, reader):
        self.client, self.writer, self.reader = client, writer, reader
        self.ports = {}
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
                if self.complete:
                    raise ValueError('event after completion')
                if kind == 'complete':
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
    def run(node, yaml, *, tls_root_certs=None, cert_chain=None, private_key=None, timeout=3600):
        u = urlsplit(node)
        if (u.scheme != 'dacp' or not u.hostname or u.username or u.password
                or u.path not in ('', '/') or u.query or u.fragment):
            raise ValueError('expected node origin dacp://host[:port]')
        body = pb.OperatorRequest(job_yaml=yaml).SerializeToString()
        if len(body) > 1024*1024:
            raise ValueError('control request exceeds 1 MiB')
        client = _flight_client(u, tls_root_certs, cert_chain, private_key)
        try:
            writer, reader = client.do_exchange(flight.FlightDescriptor.for_command(body),
                                               options=flight.FlightCallOptions(timeout=timeout))
            writer.begin(pa.schema([('data', pa.binary())]))
            writer.done_writing()
            return JobStream(client, writer, reader)
        except BaseException:
            client.close()
            raise
