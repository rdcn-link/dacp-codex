"""Pull-only Flight SDK. A failed reader is terminal; reopen the source to retry."""
import io
import os
from dataclasses import dataclass
from urllib.parse import urlsplit, unquote
import pyarrow as pa
import pyarrow.flight as flight
from . import dacp_pb2 as pb

def _flight_client(u, tls_root_certs=None, cert_chain=None, private_key=None):
    def credential(value, env):
        if value is not None: return value
        path = os.environ.get(env)
        if path:
            with open(path, "rb") as f: return f.read()
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    return flight.FlightClient(
        f"grpc+tls://{host}:{u.port or 8815}",
        tls_root_certs=credential(tls_root_certs, "DACP_CA"),
        cert_chain=credential(cert_chain, "DACP_CERT"),
        private_key=credential(private_key, "DACP_KEY"))

@dataclass(frozen=True)
class Item:
    path: str
    format: str
    schema: object = None

class Source:
    @staticmethod
    def open(uri, *, tls_root_certs=None, cert_chain=None, private_key=None):
        u = urlsplit(uri)
        if u.scheme != "dacp" or not u.hostname or u.username or u.password or u.query or u.fragment:
            raise ValueError("expected dacp://host[:port]/dataset[/item]")
        parts = unquote(u.path).strip("/").split("/", 1)
        if not parts[0] or any(x in (".", "..") for x in unquote(u.path).split("/")):
            raise ValueError("invalid dataset path")
        client = _flight_client(u, tls_root_certs, cert_chain, private_key)
        ds = DataSet(client, parts[0], uri)
        if len(parts) == 2:
            try: stream = ds.open(parts[1])
            except Exception:
                client.close(); raise
            stream._owns_client = True
            return stream
        return ds

class DataSet:
    def __init__(self, client, name, uri):
        self._client, self.name, self.uri = client, name, uri
    def list(self):
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path("datasets", self.name))
        meta = pb.DataSet.FromString(info.app_metadata)
        return [Item(x.path, x.format, pa.ipc.read_schema(pa.BufferReader(x.arrow_schema)) if x.arrow_schema else None) for x in meta.items]
    def open(self, path):
        if not path or path.startswith("/") or any(x in ("", ".", "..") for x in path.split("/")):
            raise ValueError("expected relative item path")
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path("datasets", self.name, path))
        return Stream(self._client, info.endpoints[0].ticket)
    def close(self): self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

class Stream:
    def __init__(self, client, ticket):
        self._client, self._ticket, self._owns_client = client, ticket, False
    def close(self):
        if self._owns_client: self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    def _reader(self, view):
        t = pb.Ticket.FromString(self._ticket.ticket)
        t.view = view
        return self._client.do_get(flight.Ticket(t.SerializeToString()))
    def as_records(self):
        reader = self._reader("records")
        try:
            for chunk in reader:
                if chunk.data is not None: yield chunk.data
        finally: reader.cancel()
    def as_bytes(self): return io.BufferedReader(_Bytes(self._reader("bytes")))

class _Bytes(io.RawIOBase):
    def __init__(self, reader):
        super().__init__(); self.reader = reader; self.chunks = iter(reader); self.pending = memoryview(b""); self.failed = False
    def readable(self): return True
    def readinto(self, target):
        if not len(target): return 0
        if self.failed: raise OSError("stream failed; reopen source")
        try:
            while not self.pending:
                chunk = next(self.chunks, None)
                if chunk is None: return 0
                if chunk.data is None: continue
                self.pending = memoryview(b"".join(chunk.data.column(0).to_pylist()))
            n = min(len(target), len(self.pending)); target[:n] = self.pending[:n]; self.pending = self.pending[n:]; return n
        except Exception:
            self.failed = True; self.reader.cancel(); raise
    def close(self):
        if not self.closed: self.reader.cancel()
        super().close()
