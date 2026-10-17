"""Pull-only Flight SDK. A failed reader is terminal; reopen the source to retry."""
import io
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit, unquote
import pyarrow as pa
import pyarrow.flight as flight
from . import dacp_pb2 as pb
from .errors import public_errors, translate, TRANSPORT_ERRORS

@public_errors
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

def _logical_path(path):
    if (not isinstance(path, str) or not path.startswith("/") or "\\" in path
            or any(ord(c) < 32 or ord(c) == 127 for c in path)
            or (path != "/" and any(p in ("", ".", "..") for p in path[1:].split("/")))):
        raise ValueError("expected absolute logical path, for example /reports/orders")
    return path


@dataclass(frozen=True)
class Item:
    path: str
    format: str
    schema: object = None
    kind: str = "data"
    views: tuple = ()
    last_modified_time_ms: int | None = None
    total_size_bytes: int | None = None
    record_count: int | None = None

    @property
    def last_modified_time(self):
        return (datetime.fromtimestamp(self.last_modified_time_ms / 1000, timezone.utc)
                if self.last_modified_time_ms is not None else None)

class Source:
    @staticmethod
    def open(uri, *, tls_root_certs=None, cert_chain=None, private_key=None):
        u = urlsplit(uri)
        if u.scheme != "dacp" or not u.hostname or u.username or u.password or u.query or u.fragment:
            raise ValueError("expected dacp://host[:port]/dataset[/item]")
        path = unquote(u.path)
        parts = path[1:].split("/", 1) if path.startswith("/") else [""]
        if (not parts[0] or u.port is not None and not 0 < u.port <= 65535
                or any(x in ("", ".", "..") for x in path[1:].split("/"))
                or "\\" in path or any(ord(c) < 32 or ord(c) == 127 for c in path)):
            raise ValueError("invalid dataset path")
        client = _flight_client(u, tls_root_certs, cert_chain, private_key)
        ds = DataSet(client, parts[0], uri)
        if len(parts) == 2:
            try: stream = ds.open("/" + parts[1])
            except Exception:
                client.close(); raise
            stream._owns_client = True
            return stream
        return ds

class DataSet:
    def __init__(self, client, name, uri):
        self._client, self.name, self.uri = client, name, uri
    @public_errors
    def list(self, path=None):
        descriptor = ("datasets", self.name) if path is None else ("datasets", self.name, "list", _logical_path(path))
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path(*descriptor))
        meta = pb.DataSet.FromString(info.app_metadata)
        return [Item(x.path, x.format, pa.ipc.read_schema(pa.BufferReader(x.arrow_schema)) if x.arrow_schema else None,
                     x.kind, tuple(x.views),
                     x.last_modified_time_ms if x.HasField("last_modified_time_ms") else None,
                     x.total_size_bytes if x.HasField("total_size_bytes") else None,
                     x.record_count if x.HasField("record_count") else None) for x in meta.items]
    @public_errors
    def count(self, path):
        """Request an exact record count explicitly; None for non-record data."""
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path(
            "datasets", self.name, "count", _logical_path(path)))
        metadata = pb.Item.FromString(info.app_metadata)
        return metadata.record_count if metadata.HasField("record_count") else None
    @public_errors
    def open(self, path):
        _logical_path(path)
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path("datasets", self.name, path))
        metadata = pb.Item.FromString(info.app_metadata)
        if metadata.kind != "data" or not info.endpoints:
            raise ValueError("only data items can be opened; use list(path) for folders")
        return Stream(self._client, info.endpoints[0].ticket, tuple(metadata.views))
    @public_errors
    def close(self): self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

class Stream:
    def __init__(self, client, ticket, views=("records", "bytes")):
        self._client, self._ticket, self._owns_client = client, ticket, False
        self.views = views
    @public_errors
    def close(self):
        if self._owns_client: self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    @public_errors
    def _reader(self, view):
        if view not in self.views:
            raise ValueError(f"item does not support {view} view")
        t = pb.Ticket.FromString(self._ticket.ticket)
        t.view = view
        return self._client.do_get(flight.Ticket(t.SerializeToString()))
    def as_records(self):
        reader = self._reader("records")
        try:
            for chunk in reader:
                if chunk.data is not None: yield chunk.data
        except TRANSPORT_ERRORS as error:
            raise translate(error) from None
        finally:
            try: reader.cancel()
            except TRANSPORT_ERRORS: pass
    @public_errors
    def as_bytes(self): return io.BufferedReader(_Bytes(self._reader("bytes")))

class _Bytes(io.RawIOBase):
    def __init__(self, reader):
        super().__init__(); self.reader = reader; self.chunks = iter(reader); self.pending = memoryview(b""); self.failed = False
    def readable(self): return True
    @public_errors
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
            self.failed = True
            try: self.reader.cancel()
            except TRANSPORT_ERRORS: pass
            raise
    @public_errors
    def close(self):
        if not self.closed: self.reader.cancel()
        super().close()
