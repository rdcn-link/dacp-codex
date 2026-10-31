"""Pull-only Flight SDK. A failed reader is terminal; reopen the source to retry."""
import io
import json
from pathlib import Path
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit, unquote
import pyarrow as pa
import pyarrow.flight as flight
from . import dacp_pb2 as pb
from .expressions import scan_options
from .errors import public_errors, translate, TRANSPORT_ERRORS

@public_errors
def _flight_client(u, tls_root_certs=None, cert_chain=None, private_key=None, token=None, use_env_token=True):
    def credential(value, env):
        if value is not None: return value
        path = os.environ.get(env)
        if path:
            with open(path, "rb") as f: return f.read()
    if token is None and use_env_token:
        token_path = os.environ.get('DACP_TOKEN_FILE')
        token = Path(token_path).read_text().strip() if token_path else None
    if token is not None and (not isinstance(token, str) or not token or any(c.isspace() for c in token)):
        raise ValueError('invalid bearer token')
    class AuthMiddleware(flight.ClientMiddleware):
        def sending_headers(self):
            return {'authorization': 'Bearer ' + token} if token else {}
    class AuthFactory(flight.ClientMiddlewareFactory):
        def start_call(self, info): return AuthMiddleware()
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    ca = credential(tls_root_certs, "DACP_CA")
    certificate = credential(cert_chain, "DACP_CERT")
    key = credential(private_key, "DACP_KEY")
    anonymous = token is None and certificate is None and key is None
    return flight.FlightClient(
        f"grpc+tls://{host}:{u.port or 8815}",
        tls_root_certs=ca,
        disable_server_verification=anonymous and ca is None,
        cert_chain=certificate,
        private_key=key, middleware=[AuthFactory()])

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

@dataclass(frozen=True)
class DatasetInfo:
    name: str
    uri: str
    description: str = "{}"
    node_identity: str = ""
    observed_at_ms: int = 0


class DatasetDirectory(list):
    def __init__(self, entries=(), *, complete=True, unavailable_sources=0):
        super().__init__(entries)
        self.complete = complete
        self.unavailable_sources = unavailable_sources


def _dataset_infos(client, timeout=30):
    results = list(client.do_action(flight.Action('dacp.catalog', b''), options=flight.FlightCallOptions(timeout=timeout)))
    if len(results) != 1:
        raise ValueError('invalid dataset directory response')
    directory = pb.Directory.FromString(results[0].body.to_pybytes())
    entries = [DatasetInfo(m.name, m.uri, m.description, m.node_identity,
                           m.observed_at_ms) for m in directory.datasets]
    return DatasetDirectory(entries, complete=directory.complete,
                            unavailable_sources=directory.unavailable_sources)


class Source:
    @staticmethod
    @public_errors
    def datasets(url, *, tls_root_certs=None, cert_chain=None, private_key=None, token=None, timeout=30):
        """Read public metadata from dacp-server."""
        u = urlsplit(url)
        if (u.scheme != "dacp" or not u.hostname or u.username is not None or u.query or u.fragment
                or u.path not in ("", "/") or u.port is not None and not 0 < u.port <= 65535 or timeout <= 0):
            raise ValueError("expected dacp://host[:port] origin and positive timeout")
        client = _flight_client(u, tls_root_certs, cert_chain, private_key, **({"token": token} if token is not None else {}))
        try:
            return _dataset_infos(client, timeout)
        finally:
            client.close()

    @staticmethod
    def open(uri, *, tls_root_certs=None, cert_chain=None, private_key=None, token=None):
        u = urlsplit(uri)
        if u.scheme != "dacp" or not u.hostname or u.username or u.password or u.query or u.fragment:
            raise ValueError("expected dacp://host[:port]/dataset[/item]")
        path = unquote(u.path)
        parts = path[1:].split("/", 1) if path.startswith("/") else [""]
        if (not parts[0] or u.port is not None and not 0 < u.port <= 65535
                or any(x in ("", ".", "..") for x in path[1:].split("/"))
                or "\\" in path or any(ord(c) < 32 or ord(c) == 127 for c in path)):
            raise ValueError("invalid dataset path")
        client = _flight_client(u, tls_root_certs, cert_chain, private_key, **({"token": token} if token is not None else {}))
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
    def open(self, path, *, predicate=None, columns=None, limit=None):
        scan = scan_options(predicate, columns, limit)
        _logical_path(path)
        info = self._client.get_flight_info(flight.FlightDescriptor.for_path("datasets", self.name, path))
        metadata = pb.Item.FromString(info.app_metadata)
        if not metadata.views or not info.endpoints:
            raise ValueError("item has no readable views; use list(path) for folders")
        ticket = info.endpoints[0].ticket
        if scan is not None:
            if "records" not in metadata.views: raise ValueError("scan options require records view")
            message = pb.Ticket.FromString(ticket.ticket)
            message.scan.CopyFrom(scan)
            ticket = flight.Ticket(message.SerializeToString())
        return Stream(self._client, ticket, tuple(metadata.views),
                      item_schema=info.schema if metadata.views[0] == "records" else None)
    @public_errors
    def close(self): self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

class Stream:
    def __init__(self, client, ticket, views=("records", "bytes"), item_schema=None):
        self._client, self._ticket, self._owns_client = client, ticket, False
        self.views, self.item_schema = views, item_schema
        self._scientific_readers = set()
    @public_errors
    def close(self):
        for reader in tuple(self._scientific_readers): reader.close()
        if self._owns_client: self._client.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    @public_errors
    def _reader(self, view, selection=None):
        if view not in self.views:
            raise ValueError(f"item does not support {view} view")
        t = pb.Ticket.FromString(self._ticket.ticket)
        if t.HasField("scan") and view != "records":
            raise ValueError("scan options require records view")
        t.view = view
        if selection is not None:
            if not isinstance(selection, dict): raise ValueError("selection must be an object")
            t.selection_json = json.dumps(selection, ensure_ascii=False, allow_nan=False).encode("utf-8")
            if len(t.selection_json) > 1024 * 1024: raise ValueError("selection too large")
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

    def as_view(self, view, selection=None):
        """Read a registered scientific view as a descriptor and bounded metadata/payload blocks."""
        reader = ScientificReader(self._reader(view, {} if selection is None else selection), view,
                                  lambda r: self._scientific_readers.discard(r))
        self._scientific_readers.add(reader)
        return reader
    def as_array(self, selection=None): return self.as_view("array", selection)
    def as_image(self, selection=None): return self.as_view("image", selection)
    def as_sequences(self, selection=None): return self.as_view("sequence", selection)

@dataclass(frozen=True)
class ScientificChunk:
    metadata: dict
    data: bytes
    @property
    def text(self): return self.data.decode("utf-8")

class ScientificReader:
    """Single-pass scientific blocks. The descriptor is available even for an empty stream."""
    @public_errors
    def __init__(self, reader, view, released=lambda r: None):
        self._reader, self._released, self._closed = reader, released, False
        try:
            schema = reader.schema
            if (schema.names != ["metadata", "data"] or schema.types != [pa.string(), pa.binary()]
                    or (schema.metadata or {}).get(b"xstream.view") != view.encode("utf-8")):
                raise ValueError("invalid scientific envelope")
            self.descriptor = self._json((schema.metadata or {}).get(b"xstream.descriptor", b""))
            self.view = view
            self._batches = iter(reader)
            self._pending = iter(())
        except Exception:
            reader.cancel()
            raise
    @staticmethod
    def _json(data):
        if len(data) > 1024 * 1024: raise ValueError("scientific metadata too large")
        value = json.loads(data)
        if not isinstance(value, dict): raise ValueError("scientific metadata must be an object")
        return value
    def __iter__(self): return self
    @public_errors
    def __next__(self):
        if self._closed: raise StopIteration
        try:
            while True:
                pair = next(self._pending, None)
                if pair is not None:
                    metadata, data = pair
                    if metadata is None or data is None or len(data) > 16 * 1024 * 1024:
                        raise ValueError("invalid scientific block")
                    return ScientificChunk(self._json(metadata.encode("utf-8")), data)
                chunk = next(self._batches)
                if chunk.data is None: continue
                batch = chunk.data
                if batch.num_rows > 1024: raise ValueError("too many scientific blocks")
                self._pending = iter(zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()))
        except StopIteration:
            self.close()
            raise
        except Exception:
            self.close()
            raise
    @public_errors
    def close(self):
        if not self._closed:
            self._closed = True
            try: self._reader.cancel()
            finally: self._released(self)
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

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
